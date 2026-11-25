"""Anclas oficiales: tarifas, factor de emision, escala (tope huella 2021), fechas de cada dato, plausibilidad y procedencia en kpi_summary."""
import csv
import json
import sqlite3
import xml.etree.ElementTree as ET

import pytest

import config
import pipeline_etl as etl

FUENTES = config.cargar_fuentes()
SUP = config.cargar_supuestos()["tarifas_y_emisiones"]
P = config.cargar_parametros()
A = lambda i: config.ancla(FUENTES, i)  # noqa: E731
FECHAS = ("periodo_del_dato", "fecha_publicacion", "fecha_consulta")


def test_tarifas_de_supuestos_son_las_anclas_oficiales():
    esperado = {"tarifa_energia_usd_kwh": (0.1557, "tarifa_energia_edemet_mtd"),
                "tarifa_demanda_usd_kw_mes": (20.23, "tarifa_demanda_edemet_mtd"),
                "cargo_fijo_usd_cliente_mes": (14.32, "cargo_fijo_edemet_mtd"),
                "factor_emision_kg_kwh": (0.1501, "factor_emision_sin_2024")}
    for k, (valor, ancla) in esperado.items():
        assert SUP[k]["valor"] == valor == A(ancla)["valor"], k
        assert SUP[k]["tipo_fuente"] == "oficial" and SUP[k]["ancla"] == ancla and SUP[k]["url"].startswith("https://"), k
        assert all(SUP[k][f] == A(ancla)[f] for f in FECHAS), k  # supuestos.json lleva las mismas tres fechas
    regla = A("factor_potencia_regla")["valor"]
    assert SUP["fp_umbral"]["valor"] == regla["umbral_fp_atraso"] == 0.90
    assert SUP["fp_recargo_fraccion_por_0_01"]["valor"] == regla["recargo_pct_por_cada_0_01_debajo"] / 100 == 0.02
    for k in ("tarifa_energia_edemet_mtd", "tarifa_demanda_edemet_mtd"):  # el valor cae en su rango de sensibilidad
        r = A(k)["rango_sensibilidad"]
        assert r["min"] <= A(k)["valor"] <= r["max"]
    assert "ANACRONISMO" in A("tarifa_energia_edemet_mtd")["nota"] and "2026-07" in A("tarifa_energia_edemet_mtd")["periodo_del_dato"]


def test_factor_de_emision_2024_y_serie_historica():
    f = A("factor_emision_sin_2024")
    assert f["valor"] == 0.1501 and f["estado"] == "confirmado" and "MIPRE-2025-0030765" in f["entidad"] + f["fecha_publicacion"]
    assert SUP["factor_emision_kg_kwh"]["valor"] not in (0.4438, 0.4932)
    assert "2025" in SUP["factor_emision_kg_kwh"]["justificacion"] and "mas reciente" in SUP["factor_emision_kg_kwh"]["justificacion"]
    assert (A("factor_emision_sin_2021")["valor"], A("factor_emision_sin_2022")["valor"], A("factor_emision_sin_2023")["valor"]) == (0.13, 0.1079, 0.1971)


def test_ancla_de_escala_es_el_tope_de_la_huella_2021():
    assert A("huella_fundacion_2021")["valor"] == 450 and A("huella_fundacion_2021")["periodo_del_dato"] == "2021"
    tope = A("tope_escala_fundacion_2021")
    assert tope["estado"] == "deduccion" and tope["valor"] == round(450 / 0.13 * 1000, -3) == 3462000
    r = tope["rango_sensibilidad"]
    assert r["min"] == pytest.approx(450 / 0.1971 * 1000, abs=1000) and r["max"] == pytest.approx(450 / 0.1079 * 1000, abs=1000)


def test_datos_reales_registrados_con_sus_cifras():
    assert A("areas_cds_tesis_2018")["valor"] == {"128": 3511, "108": 1027, "202": 704}
    assert round(sum(A("edemet_ventas_mensuales_2025")["valor"]), 2) == 3903.01
    assert sum(A("utp_campus_vls_2024")["valor"]) == 10772377 and A("utp_gei_2025_total")["valor"] == 15384602
    clima = A("clima_albrook_etesa_eia_2023")["valor"]
    assert len(clima["temperatura_media_mensual_c"]) == 12 and sum(clima["precipitacion_mensual_mm"]) == pytest.approx(1798.7, abs=0.1)
    assert len(A("feriados_2025_panama")["valor"]) == 16 and not {"2025-12-24", "2025-12-31"} & set(A("feriados_2025_panama")["valor"])
    inf = {x["id"]: x for x in FUENTES["inferencias"]}
    assert inf["distribuidora_clayton_edemet"]["estado"] == "probable, no confirmado"
    assert "suposicion" in inf["correspondencia_numero_nombre_edificios"]["estado"]
    assert len(FUENTES["referencias_normativas"]["items"]) == 4


def test_toda_entrada_lleva_las_tres_fechas():
    entradas = FUENTES["fuentes"] + FUENTES["orientaciones_no_confirmadas"] + FUENTES["inferencias"]
    for a in entradas:
        assert all(a.get(f) for f in FECHAS), a["id"]
        assert a["fecha_consulta"] == "2026-11-24", a["id"]
    for a in FUENTES["fuentes"]:
        assert {"id", "valor", "unidad", "anio_fecha", "entidad", "url", "estado", "nota"} <= set(a), a["id"]
        assert a["estado"] in ("confirmado", "confirmado con reservas", "deduccion", "no confirmado")
    # los parametros con ancla oficial copian las fechas de su fuente
    n = 0
    for nombre, p in P["parametros"].items():
        for i in [p["ancla"], *p.get("ancla_por_valor", {}).values()]:
            if i != "supuesto del equipo":
                assert p["fechas_ancla"][i] == {f: A(i)[f] for f in FECHAS}, f"{nombre}:{i}"
                n += 1
    assert n >= 10
    cal = P["calendario_no_laborable"]
    assert cal["fechas_ancla"][cal["ancla"]] == {f: A(cal["ancla"])[f] for f in FECHAS}


def test_factura_xml_usa_tarifa_edemet_sin_contrato_renovable(datos):
    root = ET.parse(datos / "data" / "raw" / "facturacion_mtd_distribuidora_2025.xml").getroot()
    assert "EDEMET" in root.attrib["distribuidora"] and "inferencia" in root.attrib["distribuidora"]
    assert "jul-2026" in root.attrib["nota_valoracion"] and "2024" in root.attrib["nota_valoracion"]
    assert float(root.attrib["tarifa_energia_usd_kwh"]) == 0.1557 and float(root.attrib["tarifa_demanda_usd_kw_mes"]) == 20.23
    assert float(root.attrib["factor_emision_kg_kwh"]) == 0.1501 and "contrato_renovable" not in root.attrib
    n = penalizados = 0
    for f in root.findall("FacturaMensual"):
        g = lambda c: float(f.findtext(c))  # noqa: E731
        assert f.find("kwh_ppa_renovable") is None
        assert g("cargo_energia_usd") == pytest.approx(g("kwh_facturados") * 0.1557, abs=0.006)
        assert g("cargo_demanda_usd") == pytest.approx(g("demanda_maxima_kw") * 20.23, abs=0.006)
        assert g("cargo_fijo_usd") == 14.32
        fp = g("factor_potencia_mensual")
        esperado = max(0.0, (0.90 - fp) / 0.01 * 0.02) * g("cargo_demanda_usd")  # 2 % por cada 0.01 bajo 0.90, sobre demanda
        assert g("penalizacion_fp_usd") == pytest.approx(esperado, abs=0.006)
        assert g("total_factura_usd") == pytest.approx(g("cargo_energia_usd") + g("cargo_demanda_usd") + 14.32 + g("penalizacion_fp_usd"), abs=0.011)
        assert g("emisiones_alcance2_kg_co2") == pytest.approx(g("kwh_facturados") * 0.1501, abs=0.006)
        n += 1
        penalizados += g("penalizacion_fp_usd") > 0
    assert n == 144 and penalizados > 0


def test_recargo_fp_helper():
    assert config.recargo_fp_fraccion(0.90, 0.90, 0.02) == 0 and config.recargo_fp_fraccion(0.95, 0.90, 0.02) == 0
    assert config.recargo_fp_fraccion(0.85, 0.90, 0.02) == pytest.approx(0.10)  # 5 centesimas x 2 %


def test_costo_y_co2_diarios_usan_tarifa_y_factor_oficiales(datos):
    raw = etl.extract()
    rows, _ = etl.clean(raw)
    db = etl.load_sqlite(raw, etl.transform(raw, rows))
    c = sqlite3.connect(db)
    cols = {r[1] for r in c.execute("PRAGMA table_info(fact_facturacion_mensual)")}
    assert "cargo_fijo_usd" in cols and not {"kwh_ppa_renovable", "kwh_red_sin"} & cols
    assert {"area_fuente", "area_fecha"} <= {r[1] for r in c.execute("PRAGMA table_info(dim_edificio)")}
    for kwh, costo, co2 in c.execute("SELECT kwh_total, costo_energia_est_usd, emisiones_kg_co2 FROM fact_consumo_diario LIMIT 500"):
        assert costo == pytest.approx(kwh * 0.1557, abs=0.006) and co2 == pytest.approx(kwh * 0.1501, abs=0.006)
    # estacion seca por dia: ~11-dic a ~26-abr
    seca = dict(c.execute("SELECT date_key, temporada_panama FROM dim_tiempo"))
    assert seca["2025-04-26"].startswith("Seca") and seca["2025-04-27"].startswith("Lluvi") and seca["2025-12-10"].startswith("Lluvi")
    assert seca["2025-12-11"].startswith("Seca")


# ---------------------------------------------------------------- plausibilidad contra el tope de 3,462 MWh
def test_plausibilidad_razon_y_rango_contra_el_tope():
    p = etl.chequeo_plausibilidad(3_462_000)
    assert p["razon_simulado_sobre_ancla"] == 1.0 and p["dentro_de_rango"] and p["ancla_id"] == "tope_escala_fundacion_2021"
    assert p["ancla_estado"] == "deduccion" and p["ancla_mwh"] == 3462.0 and p["rango_aceptado"] == [0.5, 1.0]
    assert "NO es una validacion" in p["nota"] and p["contraste_utp"]["mwh"] == pytest.approx(10772.377)
    assert etl.chequeo_plausibilidad(1_731_000)["dentro_de_rango"] and etl.chequeo_plausibilidad(3_462_000)["dentro_de_rango"]
    assert not etl.chequeo_plausibilidad(1_700_000)["dentro_de_rango"]   # < 0.5x
    assert not etl.chequeo_plausibilidad(3_500_000)["dentro_de_rango"]   # > 1.0x
    assert not etl.chequeo_plausibilidad(9_533_883)["dentro_de_rango"]   # el consumo de la version anterior ya no pasa


def _gold(datos):
    raw = etl.extract()
    rows, _ = etl.clean(raw)
    etl.load_sqlite(raw, etl.transform(raw, rows))
    return raw


def test_kpi_summary_incluye_procedencia_fechas_y_plausibilidad(datos):
    s = etl.build_gold(raw=_gold(datos), fuente="sintetico")
    pr = s["procedencia_datos"]
    assert pr["modo"] == "sintetico_anclado" and pr["semilla"] == 202511 and pr["fecha_consulta_fuentes"] == "2026-11-24"
    ids = {a["id"] for a in pr["anclas_usadas"]}
    assert {"tarifa_energia_edemet_mtd", "tarifa_demanda_edemet_mtd", "cargo_fijo_edemet_mtd", "factor_potencia_regla", "factor_emision_sin_2024",
            "tope_escala_fundacion_2021", "huella_fundacion_2021", "areas_cds_tesis_2018", "clima_albrook_etesa_eia_2023",
            "feriados_2025_panama", "edemet_ventas_mensuales_2025", "utp_campus_vls_2024"} <= ids
    assert all(all(a[f] for f in FECHAS) for a in pr["anclas_usadas"])
    assert pr["metodologia"]["dias_no_laborables"]["total_no_laborables"] == 119
    assert any("anacronismo" in x for x in pr["simplificaciones"]) and any("0.1501" in x and "2025" in x for x in pr["simplificaciones"])
    pl = pr["plausibilidad"]
    assert 0.5 <= pl["razon_simulado_sobre_ancla"] <= 1.0 and pl["dentro_de_rango"]
    assert pl["consumo_simulado_mwh"] == s["kpis_globales"]["consumo_total_mwh"] and 1700 <= pl["consumo_simulado_mwh"] <= 3462
    assert 12000 <= s["kpis_globales"]["area_total_m2"] <= 16000 and "pct_energia_renovable_ppa" not in s["kpis_globales"]
    k = s["kpis_globales"]
    assert k["emisiones_totales_tco2"] == pytest.approx(k["consumo_total_kwh"] * 0.1501 / 1000, rel=1e-3)
    assert k["costo_energia_anual_usd"] == pytest.approx(k["consumo_total_kwh"] * 0.1557, rel=1e-3)
    guardado = json.loads((datos / "data" / "gold" / "kpi_summary.json").read_text(encoding="utf-8"))
    assert guardado["procedencia_datos"]["modo"] == "sintetico_anclado" and len(guardado["procedencia_datos"]["fechas_datos"]) >= 19


def test_tabla_fechas_datos_csv(datos):
    etl.build_gold(raw=_gold(datos), fuente="sintetico")
    filas = list(csv.DictReader(open(datos / "data" / "gold" / "tabla_fechas_datos.csv", encoding="utf-8")))
    assert list(filas[0]) == ["dato", "valor", "periodo", "publicacion", "consulta", "url", "estado"]
    por = {f["dato"]: f for f in filas}
    assert len(filas) >= 22 and all(f["consulta"] == "2026-11-24" and f["periodo"] and f["publicacion"] and f["estado"] for f in filas)
    assert por["huella_fundacion_2021"]["valor"] == "450" and por["factor_emision_sin_2024"]["publicacion"].startswith("2025-08-21")
    assert por["distribuidora_clayton_edemet"]["estado"] == "probable, no confirmado"


def test_pipeline_falla_si_el_consumo_sintetico_sale_del_rango(datos, monkeypatch):
    raw = _gold(datos)
    chico = json.loads(json.dumps(FUENTES))
    next(a for a in chico["fuentes"] if a["id"] == "tope_escala_fundacion_2021")["valor"] = 1_000_000  # razon ~3x
    monkeypatch.setattr(config, "cargar_fuentes", lambda: chico)
    with pytest.raises(etl.PlausibilidadError, match="se exige 0.5x-1.0x"):
        etl.build_gold(raw=raw, fuente="sintetico")
    assert etl.build_gold(raw=raw, fuente="real")["procedencia_datos"]["plausibilidad"]["dentro_de_rango"] is False  # en modo real solo informa
