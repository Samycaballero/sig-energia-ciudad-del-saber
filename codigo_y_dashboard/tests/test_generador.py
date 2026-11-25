"""Generador de datos: parametros con rango, valores generados dentro de [min, max], reproducibilidad por semilla."""
import copy
import csv
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

import config
import generar_datos_fuente as gen

ROOT = Path(__file__).resolve().parent.parent
P = config.cargar_parametros()
PR = P["parametros"]
TIPO = {e["building_id"]: e["tipo"] for e in P["edificios"]}


@pytest.fixture(scope="module")
def gen_dir(tmp_path_factory):
    d = tmp_path_factory.mktemp("raw")
    meta = gen.generar_datos(raw_dir=d, verbose=False)
    return d, meta


def _en(nombre, x, tol=0.0):
    return PR[nombre]["min"] - tol <= x <= PR[nombre]["max"] + tol


def test_cada_parametro_tiene_rango_unidad_ancla_y_justificacion():
    ids = {f["id"] for f in config.cargar_fuentes()["fuentes"]} | {"supuesto del equipo"}
    assert len(PR) >= 60 and isinstance(P["semilla"], int)
    for n, p in PR.items():
        assert {"min", "max", "unidad", "ancla", "justificacion"} <= set(p), n
        assert p["min"] <= p["max"] and p["ancla"] in ids and len(p["justificacion"]) > 10, n
    # anclas oficiales: tarifas, regla de FP, factor de emision, clima y estacionalidad EDEMET (+ areas por valor); el resto es supuesto del equipo
    oficiales = {n for n, p in PR.items() if p["ancla"] != "supuesto del equipo"}
    assert oficiales == {"tarifa_energia_usd_kwh", "tarifa_demanda_usd_kw_mes", "cargo_fijo_usd_cliente_mes", "fp_umbral",
                         "fp_recargo_fraccion_por_0_01", "factor_emision_kg_kwh", "temp_media_mensual_c", "humedad_media_mensual_pct",
                         "indice_mensual_edemet_2025"}   # + areas reales de 3 edificios (ancla_por_valor)


def test_todos_los_valores_de_los_parametros_estan_dentro_de_su_rango():
    sup = config.cargar_supuestos()["tarifas_y_emisiones"]
    n_valores = 0
    for n, p in PR.items():
        vals = ([p["valor"]] if "valor" in p else list(p.get("valores", {}).values()) +
                ([sup[n]["valor"]] if p["rol"] == "tarifa" else []))
        for v in vals:
            assert p["min"] <= v <= p["max"], f"{n}={v} fuera de [{p['min']}, {p['max']}]"
            n_valores += 1
    assert n_valores > 150
    assert gen.validar_parametros(P) == []


def test_eui_por_tipo_dentro_de_la_orientacion_de_literatura_no_confirmada():
    # solo sanidad: 47-628 kWh/m2/anio es orientacion NO confirmada (Energies 2021), no un respaldo
    for n, p in PR.items():
        if n.startswith("eui_"):
            assert 47 <= p["min"] <= p["max"] <= 628 and p["ancla"] == "supuesto del equipo"


def test_parametros_invalidos_se_rechazan(tmp_path):
    mal = copy.deepcopy(P)
    mal["parametros"]["fp_base"]["valores"]["CDS-208"] = 0.99  # fuera de [0.85, 0.97]
    mal["parametros"]["area_m2"]["ancla"] = "fuente_inexistente"
    err = gen.validar_parametros(mal)
    assert any("fp_base" in e for e in err) and any("fuente_inexistente" in e for e in err)
    ruta = tmp_path / "mal.json"
    ruta.write_text(json.dumps(mal), encoding="utf-8")
    with pytest.raises(ValueError, match="parametros_generador.json invalido"):
        gen.generar_datos(raw_dir=tmp_path / "raw", params_path=ruta, verbose=False)


def test_valores_generados_dentro_de_min_max(gen_dir):
    d, meta = gen_dir
    filas = list(csv.DictReader(open(d / "telemetria_medidores_ami_2025.csv", encoding="utf-8")))
    assert len(filas) == meta["filas_ami_raw"]
    for r in filas:
        assert _en("demanda_max_kw", float(r["demanda_max_kw"]))
        assert _en("factor_potencia_diario", float(r["factor_potencia"]))
        assert _en("voltaje_promedio_v", float(r["voltaje_promedio_v"]))
        assert _en("ocupacion_pct", float(r["ocupacion_pct"]))
        if r["kwh_horario_laboral"] != "":
            lab, off = float(r["kwh_horario_laboral"]), float(r["kwh_fuera_horario"])
            assert _en("kwh_dia_edificio", lab + off) and _en("pct_fuera_horario_diario", off / (lab + off), tol=1e-3)
    iot = json.load(open(d / "sensores_iot_hvac_clima_2025.json", encoding="utf-8"))
    assert len(iot) == 12 * 365
    for x in iot:
        assert _en("temp_exterior_c", x["ambiente"]["temp_exterior_c"])
        assert _en("humedad_relativa_pct", x["ambiente"]["humedad_relativa_pct"])
        assert _en("cop_eficiencia", x["hvac"]["cop_eficiencia"]) and _en("setpoint_generado_c", x["hvac"]["setpoint_c"])
        assert PR["horas_operacion_finde"]["min"] <= x["hvac"]["horas_operacion_dia"] <= PR["horas_operacion_24h"]["max"]
    kwh, area = {}, {e["building_id"]: PR["area_m2"]["valores"][e["building_id"]] for e in P["edificios"]}
    for f in ET.parse(d / "facturacion_mtd_distribuidora_2025.xml").getroot().findall("FacturaMensual"):
        b = f.attrib["building_id"]
        kwh[b] = kwh.get(b, 0.0) + float(f.findtext("kwh_facturados"))
        assert _en("factor_potencia_diario", float(f.findtext("factor_potencia_mensual")))
        assert _en("demanda_max_kw", float(f.findtext("demanda_maxima_kw")))
    assert set(kwh) == set(area) and len(kwh) == 12
    for b, total in kwh.items():  # EUI realizado dentro del rango de su tipo (y = objetivo)
        eui = total / area[b]
        assert _en(f"eui_{TIPO[b]}", eui), f"{b}: EUI {eui:.1f} fuera del rango de {TIPO[b]}"
        assert eui == pytest.approx(PR[f"eui_{TIPO[b]}"]["valores"][b], rel=1e-3)
    for k, o in meta["rangos_observados_en_datos"].items():
        assert PR[k]["min"] <= o["min"] <= o["max"] <= PR[k]["max"], k


def test_defectos_inyectados_con_tasas_dentro_de_rango(gen_dir):
    d, meta = gen_dir
    N = 12 * 365
    for k, tasa in meta["tasas_defectos_realizadas"].items():
        assert _en(f"prob_{k}", tasa), k
    dfc = meta["defectos_inyectados"]
    assert dfc["nulos"] == round(PR["prob_nulo"]["valor"] * N) and meta["filas_ami_raw"] == N + dfc["duplicados"]
    filas = list(csv.DictReader(open(d / "telemetria_medidores_ami_2025.csv", encoding="utf-8")))
    assert sum("/" in r["fecha_lectura"] for r in filas) >= dfc["fechas_mixtas"]
    assert sum(float(r["kwh_total"]) < 0 for r in filas if r["kwh_total"] != "") >= dfc["outliers_negativos"]


def test_metodologia_escrita(gen_dir):
    d, meta = gen_dir
    m = json.loads((d / "_metodologia.json").read_text(encoding="utf-8"))
    assert m["semilla"] == P["semilla"] and m["modo"] == "sintetico_anclado" and m["version_generador"]
    assert re.match(r"\d{4}-\d{2}-\d{2}T", m["fecha_generacion"])
    assert set(m["rangos_efectivos"]) == set(PR) and sum(m["defectos_inyectados"].values()) > 0
    assert m["rangos_efectivos"]["tarifa_energia_usd_kwh"]["valor_usado"] == 0.1557
    assert "factor_emision_sin_2024" in m["anclas_oficiales_usadas"]


def _bytes(d):
    return {a: (d / a).read_bytes() for a in gen.ARCHIVOS}


def test_misma_semilla_mismos_datos_y_otra_semilla_otros(tmp_path, gen_dir):
    d0, _ = gen_dir
    gen.generar_datos(raw_dir=tmp_path / "a", semilla=P["semilla"], verbose=False)
    gen.generar_datos(raw_dir=tmp_path / "b", semilla=777, verbose=False)
    assert _bytes(tmp_path / "a") == _bytes(d0)
    otro = _bytes(tmp_path / "b")
    assert otro["telemetria_medidores_ami_2025.csv"] != _bytes(d0)["telemetria_medidores_ami_2025.csv"]
    assert json.loads((tmp_path / "b" / "_metodologia.json").read_text(encoding="utf-8"))["semilla"] == 777


def test_no_sobrescribe_data_raw_ajena(tmp_path):
    (tmp_path / "telemetria_medidores_ami_2025.csv").write_text("datos reales", encoding="utf-8")
    with pytest.raises(FileExistsError, match="no se sobrescriben"):
        gen.generar_datos(raw_dir=tmp_path, verbose=False)
    assert (tmp_path / "telemetria_medidores_ami_2025.csv").read_text(encoding="utf-8") == "datos reales"


def test_catalogo_nombres_y_certificaciones(gen_dir):
    d, _ = gen_dir
    cat = {e["building_id"]: e for e in json.loads((d / "catalogo_edificios_cds.json").read_text(encoding="utf-8"))}
    assert len(cat) == 12
    reales = {"CDS-128": "Ed. 128 Casa de las Naciones Unidas", "CDS-108": "Ed. 108 Casa Club", "CDS-202": "Ed. 202 Clayton Bowling",
              "CDS-205": "Ed. 205 Senacyt", "CDS-PLZ": "Plaza Ciudad del Saber", "CDS-DRM": "Complejo de Hospedaje (Dormitorios)"}
    for b, e in cat.items():
        if b in reales:
            assert e["nombre"] == reales[b] and e["nombre_estado"].startswith("nombre verificado")
        else:
            assert re.fullmatch(r"Edificio \w+ \(ilustrativo\)", e["nombre"]), e["nombre"]
    leed = {b for b, e in cat.items() if "LEED" in e["cert_leed"]}
    assert leed == {"CDS-128", "CDS-PLZ", "CDS-DRM"}
    assert cat["CDS-PLZ"]["cert_leed"] == "LEED Gold" and cat["CDS-DRM"]["cert_leed"] == "LEED Platinum"
    assert all(cat[b]["certificacion_estado"] == "reportado por una fuente, no verificado en USGBC" for b in leed)
    assert all(cat[b]["cert_leed"] == "Sin dato" for b in set(cat) - leed)


def test_sin_ppa_ni_afirmaciones_no_verificadas(gen_dir):
    d, _ = gen_dir
    prohibidos = re.compile(r"\bPPA\b|Enel|INDICASAT|FSU Panam|LEED Silver|Rehabilitaci[oó]n Eco|Hospedaje Sostenible|kwh_ppa|pct_ppa", re.I)
    archivos = [p for p in ROOT.iterdir() if p.suffix in (".py", ".md", ".html", ".json", ".yml", ".txt", ".example")]
    archivos += [p for p in d.iterdir() if p.is_file()]
    for p in archivos:
        if p.name in ("dashboard_interactivo.html",):
            continue  # salida regenerable: se revisa en test_dag_local
        m = prohibidos.search(p.read_text(encoding="utf-8", errors="ignore"))
        assert not m, f"{p.name}: texto no permitido {m.group(0)!r}"


# ---------------------------------------------------------------- ronda 2: escala, areas reales, clima, feriados, estacionalidad EDEMET
FU = config.cargar_fuentes()
ANC = lambda i: config.ancla(FU, i)  # noqa: E731


def test_areas_reales_y_escala_del_catalogo(gen_dir):
    d, meta = gen_dir
    cat = {e["building_id"]: e for e in json.loads((d / "catalogo_edificios_cds.json").read_text(encoding="utf-8"))}
    for num, bid in (("128", "CDS-128"), ("108", "CDS-108"), ("202", "CDS-202")):
        assert cat[bid]["area_m2"] == ANC("areas_cds_tesis_2018")["valor"][num]
        assert cat[bid]["area_fecha"] == "2018" and "tesis TalTech 2018" in cat[bid]["area_fuente"] and "SUPOSICION" in cat[bid]["area_fuente"]
    otros = [e for b, e in cat.items() if b not in ("CDS-128", "CDS-108", "CDS-202")]
    assert all(e["area_fuente"].startswith("supuesto del equipo") and e["area_fecha"] == "supuesto" for e in otros)
    assert not {13185, 9590, 2116} & {e["area_m2"] for e in cat.values()}   # areas de 104, 127 y 124 de la tesis: no se reutilizan
    assert 12000 <= meta["area_total_m2"] <= 16000 and len(cat) == 12
    assert all(PR["area_m2"]["min"] <= e["area_m2"] <= PR["area_m2"]["max"] for e in cat.values())
    anual = sum(float(f.findtext("kwh_facturados")) for f in ET.parse(d / "facturacion_mtd_distribuidora_2025.xml").getroot().findall("FacturaMensual"))
    assert 0.5 <= anual / ANC("tope_escala_fundacion_2021")["valor"] <= 1.0   # entre 0.5x y 1.0x del tope de 3,462 MWh


def test_clima_generado_alrededor_de_la_media_real_de_albrook(gen_dir):
    d, _ = gen_dir
    clima = ANC("clima_albrook_etesa_eia_2023")["valor"]
    assert [PR["temp_media_mensual_c"]["valores"][str(m)] for m in range(1, 13)] == clima["temperatura_media_mensual_c"]
    for m, h in clima["humedad_relativa_pct_meses_con_dato"].items():
        assert PR["humedad_media_mensual_pct"]["valores"][m] == h
    for m in ("2", "3", "4", "6", "7"):   # meses sin dato: valor supuesto dentro de 85-90 % y marcado como supuesto
        assert 85 <= PR["humedad_media_mensual_pct"]["valores"][m] <= 90 and PR["humedad_media_mensual_pct"]["ancla_por_valor"][m] == "supuesto del equipo"
    iot = json.load(open(d / "sensores_iot_hvac_clima_2025.json", encoding="utf-8"))
    por_mes = {}
    for x in iot:
        por_mes.setdefault(int(x["timestamp"][5:7]), []).append(x["ambiente"]["temp_exterior_c"])
    for m, vals in por_mes.items():
        assert sum(vals) / len(vals) == pytest.approx(clima["temperatura_media_mensual_c"][m - 1], abs=0.4), m
        assert max(vals) <= 34.0 and min(vals) >= 22.0
    assert PR["temp_media_mensual_c"]["min"] >= 25.0 and PR["temp_media_mensual_c"]["max"] <= 29.0   # ya no los 28-33 C supuestos


def test_calendario_de_feriados_aplicado(gen_dir):
    d, meta = gen_dir
    fer = [f["fecha"] for f in P["calendario_no_laborable"]["feriados_2025"]]
    assert fer == ANC("feriados_2025_panama")["valor"] and len(fer) == 16
    assert not {"2025-12-24", "2025-12-31", "2025-04-22", "2025-04-23", "2025-04-24"} & set(fer)
    n = meta["dias_no_laborables"]
    assert n == {"dias_total": 365, "fines_de_semana": 104, "feriados": 16, "feriados_en_dia_habil": 15, "total_no_laborables": 119, "dias_laborables": 246}
    assert meta["feriados_aplicados"] == sorted(fer)
    iot = {(x["building_id"], x["timestamp"][:10]): x for x in json.load(open(d / "sensores_iot_hvac_clima_2025.json", encoding="utf-8"))}
    finde = PR["horas_operacion_finde"]["valor"]
    for f in fer:   # oficinas (no laboratorio): horas de fin de semana en feriado, de entre semana en un dia habil normal
        assert iot[("CDS-104", f)]["hvac"]["horas_operacion_dia"] == finde, f
    assert iot[("CDS-104", "2025-01-02")]["hvac"]["horas_operacion_dia"] == PR["horas_operacion_semana"]["valor"]
    # consumo: un edificio de oficinas consume mucho menos en feriado (dia habil) que en un dia habil ordinario
    import statistics
    filas = [r for r in csv.DictReader(open(d / "telemetria_medidores_ami_2025.csv", encoding="utf-8")) if r["building_id"] == "CDS-104" and r["kwh_total"]]
    kwh = {}
    for r in filas:
        if "/" not in r["fecha_lectura"]:
            kwh[r["fecha_lectura"]] = float(r["kwh_total"])
    import datetime
    habiles = [v for k, v in kwh.items() if datetime.date.fromisoformat(k).weekday() < 5 and k not in fer and 0 < v < 1000]
    feriados_h = [v for k, v in kwh.items() if k in fer and datetime.date.fromisoformat(k).weekday() < 5 and 0 < v < 1000]
    assert len(feriados_h) >= 10 and statistics.median(feriados_h) < 0.6 * statistics.median(habiles)


def test_indice_mensual_edemet_y_estacionalidad_plana(gen_dir):
    d, _ = gen_dir
    gwh = ANC("edemet_ventas_mensuales_2025")["valor"]
    dias = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    base = [g / n for g, n in zip(gwh, dias)]
    idx = [PR["indice_mensual_edemet_2025"]["valores"][str(m)] for m in range(1, 13)]
    assert sum(idx) / 12 == pytest.approx(1.0, abs=1e-3)   # normalizado a promedio 1
    for i, b in zip(idx, base):
        assert i == pytest.approx(b / (sum(base) / 12), abs=1e-3)
    mes = {}
    for f in ET.parse(d / "facturacion_mtd_distribuidora_2025.xml").getroot().findall("FacturaMensual"):
        mes[int(f.attrib["mes"])] = mes.get(int(f.attrib["mes"]), 0.0) + float(f.findtext("kwh_facturados"))
    tot = [mes[m] for m in range(1, 13)]
    assert max(tot) / min(tot) < 1.30   # estacionalidad plana como la de EDEMET (max/min de ventas 1.07)


def test_fechas_en_parametros_y_metodologia(gen_dir):
    d, _ = gen_dir
    m = json.loads((d / "_metodologia.json").read_text(encoding="utf-8"))
    assert m["version_generador"] == "3.0" and m["dias_no_laborables"]["total_no_laborables"] == 119
    assert {"areas_cds_tesis_2018", "clima_albrook_etesa_eia_2023", "edemet_ventas_mensuales_2025"} <= set(m["anclas_oficiales_usadas"])
    err = gen.validar_parametros({**copy.deepcopy(P), "parametros": {**copy.deepcopy(PR), "area_m2": {k: v for k, v in PR["area_m2"].items() if k != "fechas_ancla"}}})
    assert any("fechas_ancla" in e for e in err)
