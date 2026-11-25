"""
Generador de Fuentes de Datos Multi-Formato (SINTETICOS, anclados en cifras oficiales donde existen)
Organizacion: Fundacion Ciudad del Saber (Clayton, Ciudad de Panama). Periodo: Enero - Diciembre 2025 (365 dias, 12 edificios).

COMO SE GENERAN LOS DATOS
  * Todos los rangos (min/max), valores, unidad, ancla y justificacion viven en parametros_generador.json; este script los LEE.
    Nada de eso esta escrito en el codigo.
  * Anclas oficiales (fuentes_oficiales.json, cada una con periodo, fecha de publicacion y fecha de consulta): tarifa MTD de EDEMET
    (energia, demanda, cargo fijo; pliego jul-2026 aplicado a consumo 2025), regla de factor de potencia, factor de emision del SIN
    2024 (el mas reciente; se usa para 2025), areas reales de Ed. 128/108/202 (tesis TalTech 2018), temperatura y humedad medias de
    Albrook (climatologia historica, NO el clima de 2025), feriados 2025 de Panama y estacionalidad mensual de EDEMET 2025.
  * Todo lo demas (EUI por tipo, areas ilustrativas, % fuera de horario, FP por edificio, ruido, defectos inyectados) es
    "supuesto del equipo": no hay fuente oficial.
  * Consumo: por edificio se simula un perfil diario (indice mensual EDEMET x clima x dia laborable/no laborable x falla operativa
    x ruido) y se ESCALA para que el consumo anual sea exactamente EUI objetivo x area (el EUI objetivo esta dentro del rango de su tipo).
    Dia no laborable = fin de semana o feriado (calendario_no_laborable).
  * Defectos de calidad: conteos exactos = round(probabilidad x filas), en posiciones elegidas por la semilla.
  * Determinista: misma semilla + mismo parametros_generador.json => mismos 4 archivos (+ catalogo).
  * Se verifica que TODO valor generado quede dentro de [min, max]; si no, falla. Se escribe data/raw/_metodologia.json
    (semilla, rangos efectivos, defectos inyectados, fecha, version).

Genera 4 fuentes en formatos heterogeneos (+ catalogo de edificios):
  CSV  telemetria_medidores_ami_2025.csv          lecturas diarias AMI (con defectos inyectados)
  JSON sensores_iot_hvac_clima_2025.json          semiestructurado jerarquico (MongoDB)
  XML  facturacion_mtd_distribuidora_2025.xml     factura mensual MTD (distribuidora supuesta: EDEMET, inferencia no confirmada)
  TXT  bitacora_mantenimiento_hvac_2025.txt       texto no estructurado para IA generativa
Uso: python generar_datos_fuente.py [--semilla N] [--forzar] [--parametros ruta.json] [--raw-dir carpeta]
"""

import argparse
import csv
import json
import random
import sys
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path

import config

VERSION_GENERADOR = "3.0"
FECHAS = ("periodo_del_dato", "fecha_publicacion", "fecha_consulta")
ARCHIVOS = ("telemetria_medidores_ami_2025.csv", "sensores_iot_hvac_clima_2025.json",
            "facturacion_mtd_distribuidora_2025.xml", "bitacora_mantenimiento_hvac_2025.txt", "catalogo_edificios_cds.json")
POR_EDIFICIO = ("area_m2", "anio_rehab", "chiller_antiguedad_anios", "pct_fuera_horario_base", "fp_base", "sensibilidad_clima",
                "setpoint_c")
POR_TIPO = ("factor_carga", "mult_finde", "mult_semana")
SUPUESTO = "supuesto del equipo"


# ---------------------------------------------------------------- parametros
def validar_parametros(P, fuentes=None, supuestos=None):
    """Devuelve la lista de errores (vacia si todo es coherente). No lanza: cargar_parametros decide."""
    fuentes = fuentes or config.cargar_fuentes()
    supuestos = supuestos or config.cargar_supuestos()
    ids = {x["id"] for x in fuentes["fuentes"]} | {SUPUESTO}
    pr, err = P["parametros"], []
    bids = [e["building_id"] for e in P["edificios"]]
    tipos = {e["tipo"] for e in P["edificios"]}
    for n, p in pr.items():
        faltan = {"rol", "unidad", "min", "max", "ancla", "justificacion"} - set(p)
        if faltan:
            err.append(f"{n}: faltan campos {sorted(faltan)}")
            continue
        if p["min"] > p["max"]:
            err.append(f"{n}: min > max")
        if p["ancla"] not in ids:
            err.append(f"{n}: ancla {p['ancla']!r} no existe en fuentes_oficiales.json")
        vals = []
        if p["rol"] == "escalar":
            vals = [p["valor"]]
        elif p["rol"] in ("por_edificio", "por_tipo", "por_mes"):
            vals = list(p["valores"].values())
        elif p["rol"] == "tarifa":
            seccion, clave = p["fuente_valor"].split("::")[1:]
            vals = [supuestos[seccion][clave]["valor"]]
        elif p["rol"] != "cota_salida":
            err.append(f"{n}: rol desconocido {p['rol']!r}")
        for v in vals:
            if not p["min"] <= v <= p["max"]:
                err.append(f"{n}: valor {v} fuera de [{p['min']}, {p['max']}]")
    for n in POR_EDIFICIO:
        if set(pr[n]["valores"]) != set(bids):
            err.append(f"{n}: debe tener un valor por cada edificio del catalogo")
    for e in P["edificios"]:
        eui = pr.get(f"eui_{e['tipo']}")
        if not eui or e["building_id"] not in eui["valores"]:
            err.append(f"{e['building_id']}: falta EUI objetivo eui_{e['tipo']}")
    for n in POR_TIPO:
        if not tipos <= set(pr[n]["valores"]):
            err.append(f"{n}: faltan tipos {sorted(tipos - set(pr[n]['valores']))}")
    for n in ("temp_media_mensual_c", "humedad_media_mensual_pct"):
        if set(pr[n]["valores"]) != {str(m) for m in range(1, 13)}:
            err.append(f"{n}: deben estar los 12 meses")
    if set(pr["mult_anomalia"]["valores"]) != {a["building_id"] for a in P["anomalias_operativas"]}:
        err.append("mult_anomalia: debe coincidir con anomalias_operativas")
    oficiales = {x["id"] for x in fuentes["fuentes"]}
    for n, p in pr.items():  # cada ancla oficial lleva sus tres fechas
        for i in [p["ancla"], *p.get("ancla_por_valor", {}).values()]:
            if i not in ids:
                err.append(f"{n}: ancla {i!r} no existe en fuentes_oficiales.json")
            elif i in oficiales and not set(FECHAS) <= set(p.get("fechas_ancla", {}).get(i, {})):
                err.append(f"{n}: falta fechas_ancla[{i}] con {FECHAS}")
    cal = P["calendario_no_laborable"]
    ini = date.fromisoformat(P["periodo"]["inicio"])
    fin = ini + timedelta(days=P["periodo"]["dias"] - 1)
    for f in cal["feriados_2025"]:
        try:
            if not ini <= date.fromisoformat(f["fecha"]) <= fin:
                err.append(f"calendario: {f['fecha']} fuera del periodo")
        except ValueError:
            err.append(f"calendario: fecha invalida {f['fecha']!r}")
    if cal["ancla"] not in oficiales or not set(FECHAS) <= set(cal.get("fechas_ancla", {}).get(cal["ancla"], {})):
        err.append("calendario: falta ancla/fechas")
    return err


def cargar_parametros(path=None):
    P = config.cargar_parametros(path)
    err = validar_parametros(P)
    if err:
        raise ValueError("parametros_generador.json invalido:\n  - " + "\n  - ".join(err))
    return P


def _proteger_raw(raw_dir, forzar):
    """No pisa datos que no fueron generados por este script (p. ej. datos reales)."""
    hay = any((raw_dir / a).exists() for a in ARCHIVOS)
    if hay and not (raw_dir / "_metodologia.json").exists() and not forzar:
        raise FileExistsError(f"{raw_dir} contiene archivos sin _metodologia.json (podrian ser datos reales): no se sobrescriben. "
                              "Use --forzar si son sinteticos y quiere regenerarlos, o --fuente real para usarlos.")


# ---------------------------------------------------------------- generacion
def generar_datos(raw_dir=None, params_path=None, semilla=None, forzar=False, verbose=True):
    P = cargar_parametros(params_path)
    sup = config.cargar_supuestos()["tarifas_y_emisiones"]
    raw_dir = Path(raw_dir or config.raw_dir())
    _proteger_raw(raw_dir, forzar)
    raw_dir.mkdir(parents=True, exist_ok=True)
    semilla = P["semilla"] if semilla is None else int(semilla)
    rng = random.Random(semilla)
    pr = P["parametros"]
    v = lambda n: pr[n]["valor"]  # noqa: E731
    por = lambda n, k: pr[n]["valores"][k]  # noqa: E731
    tarifa = lambda n: sup[n]["valor"]  # noqa: E731

    obs = {}

    def reg(nombre, x):
        o = obs.setdefault(nombre, [x, x])
        o[0], o[1] = min(o[0], x), max(o[1], x)
        return x

    def cl(x, nombre):  # acota a [min, max] del parametro y registra lo observado
        return reg(nombre, min(max(x, pr[nombre]["min"]), pr[nombre]["max"]))

    start = date.fromisoformat(P["periodo"]["inicio"])
    days = [start + timedelta(days=i) for i in range(P["periodo"]["dias"])]
    feriados = {date.fromisoformat(f["fecha"]): f["nombre"] for f in P["calendario_no_laborable"]["feriados_2025"]}
    cuenta = {"dias_total": len(days), "fines_de_semana": sum(d.weekday() >= 5 for d in days), "feriados": len(feriados),
              "feriados_en_dia_habil": sum(f.weekday() < 5 for f in feriados)}
    cuenta["total_no_laborables"] = cuenta["fines_de_semana"] + cuenta["feriados_en_dia_habil"]
    cuenta["dias_laborables"] = cuenta["dias_total"] - cuenta["total_no_laborables"]
    anom_por = {a["building_id"]: (date.fromisoformat(a["inicio"]), date.fromisoformat(a["fin"]), por("mult_anomalia", a["building_id"]),
                                   a["descripcion"]) for a in P["anomalias_operativas"]}
    t_e, t_d, t_f = tarifa("tarifa_energia_usd_kwh"), tarifa("tarifa_demanda_usd_kw_mes"), tarifa("cargo_fijo_usd_cliente_mes")
    fp_um, fp_rec, f_em = tarifa("fp_umbral"), tarifa("fp_recargo_fraccion_por_0_01"), tarifa("factor_emision_kg_kwh")

    base_rows, iot_docs, monthly, catalogo, eui_real = [], [], {}, [], {}
    for ed in P["edificios"]:
        bid, tipo = ed["building_id"], ed["tipo"]
        area, eui = por("area_m2", bid), por(f"eui_{tipo}", bid)
        sens, fc = por("sensibilidad_clima", bid), por("factor_carga", tipo)
        mfin, msem = por("mult_finde", tipo), por("mult_semana", tipo)
        a_ini, a_fin, a_mult, _ = anom_por.get(bid, (None, None, 1.0, ""))

        # 1a pasada: perfil relativo y todos los sorteos aleatorios del dia
        dias = []
        for d in days:
            m = d.month
            nolab = d.weekday() >= 5 or d in feriados  # dia no laborable: fin de semana o feriado
            temp = cl(round(rng.gauss(por("temp_media_mensual_c", str(m)), v("ruido_temp_sigma_c")), 1), "temp_exterior_c")
            hum = cl(round(rng.gauss(por("humedad_media_mensual_pct", str(m)), v("ruido_humedad_sigma_pct")), 1), "humedad_relativa_pct")
            en_anom = a_ini is not None and a_ini <= d <= a_fin
            clima = 1.0 + sens * ((temp - v("temp_referencia_c")) / v("escala_clima_c"))
            raw = (por("indice_mensual_edemet_2025", str(m)) * clima * (mfin if nolab else msem) * (a_mult if en_anom else 1.0)
                   * rng.gauss(1.0, v("ruido_consumo_sigma")))
            dias.append({"d": d, "fin": nolab, "temp": temp, "hum": hum, "anom": en_anom, "raw": raw,
                         "r_off": rng.gauss(0, v("ruido_pct_fuera_horario_sigma")),
                         "r_pico": rng.uniform(v("factor_pico_demanda_min"), v("factor_pico_demanda_max")),
                         "r_fp": rng.gauss(0, v("ruido_fp_sigma")), "r_volt": rng.gauss(v("voltaje_nominal_v"), v("ruido_voltaje_sigma_v")),
                         "r_occ": rng.gauss(0, v("ruido_ocupacion_sigma_pct"))})
        escala = eui * area / sum(x["raw"] for x in dias)  # consumo anual = EUI objetivo x area

        # 2a pasada: valores finales
        kwh_anual = 0.0
        for x in dias:
            d, anom = x["d"], x["anom"]
            kwh = reg("kwh_dia_edificio", round(x["raw"] * escala, 2))
            kwh_anual += kwh
            pct_off = cl(por("pct_fuera_horario_base", bid) + (v("incremento_pct_fuera_horario_anomalia") if anom else 0.0)
                         + (v("incremento_pct_fuera_horario_no_laborable") if x["fin"] else 0.0) + x["r_off"],
                         "pct_fuera_horario_diario")
            kwh_off = round(kwh * pct_off, 2)
            demanda = cl(round((kwh / 24.0) / fc * x["r_pico"], 2), "demanda_max_kw")
            fp = cl(round(por("fp_base", bid) - (v("reduccion_fp_anomalia") if anom else 0.0) + x["r_fp"], 3), "factor_potencia_diario")
            volt = cl(round(x["r_volt"], 1), "voltaje_promedio_v")
            occ = cl(round((v("ocupacion_finde_media_pct") if x["fin"] else v("ocupacion_semana_media_pct")) + x["r_occ"], 1), "ocupacion_pct")

            mm = monthly.setdefault((bid, d.month), {"kwh": 0.0, "max_kw": 0.0, "fp_sum": 0.0, "n": 0})
            mm["kwh"] += kwh
            mm["max_kw"] = max(mm["max_kw"], demanda)
            mm["fp_sum"] += fp
            mm["n"] += 1

            base_rows.append({"fecha_lectura": d.isoformat(), "building_id": bid, "medidor_ami_id": f"AMI-{bid}-M01", "kwh_total": kwh,
                              "kwh_horario_laboral": round(kwh - kwh_off, 2), "kwh_fuera_horario": kwh_off, "demanda_max_kw": demanda,
                              "factor_potencia": fp, "voltaje_promedio_v": volt, "ocupacion_pct": occ})

            setpoint = cl(v("setpoint_anomalia_c") if anom else por("setpoint_c", bid), "setpoint_generado_c")
            cop = cl(round(max(v("cop_minimo"), v("cop_nuevo") - v("cop_degradacion_por_anio") * por("chiller_antiguedad_anios", bid)
                              - (v("cop_penal_anomalia") if anom else 0.0)), 2), "cop_eficiencia")
            horas = v("horas_operacion_24h") if (tipo == "laboratorio" or anom) else (
                v("horas_operacion_finde") if x["fin"] else v("horas_operacion_semana"))
            iot_docs.append({
                "timestamp": f"{d.isoformat()}T14:00:00-05:00", "building_id": bid, "zona_campus": "Sector Clayton - CdS",
                "ambiente": {"temp_exterior_c": x["temp"], "humedad_relativa_pct": x["hum"],
                              "indice_calor_c": round(x["temp"] + 0.09 * (x["hum"] - 65), 1)},
                "hvac": {"estado_chiller": "ALERTA_SOBRECARGA" if anom else "OPERACION_NORMAL", "setpoint_c": setpoint,
                         "horas_operacion_dia": horas, "cop_eficiencia": cop},
                "iluminacion": {"tecnologia": ed["iluminacion"], "sensores_presencia_activos": "sensores" in ed["iluminacion"]},
                "metadata_ingesta": {"protocolo": "MQTT/AWS-IoT-Core", "version_esquema": "2.1"},
            })
        eui_real[bid] = round(kwh_anual / area, 2)
        reg(f"eui_{tipo}", eui_real[bid])  # el EUI realizado tambien debe caer en el rango de su tipo
        catalogo.append({
            "building_id": bid, "nombre": ed["nombre"], "nombre_estado": ed["nombre_estado"], "categoria": ed["categoria"], "tipo": tipo,
            "area_m2": area, "anio_rehab": por("anio_rehab", bid), "cert_leed": ed["cert_leed"],
            "certificacion_estado": ed["certificacion_estado"], "certificacion_fuente": ed["certificacion_fuente"],
            "base_kwh_dia": round(eui * area / len(days), 0), "pct_fuera_horario": por("pct_fuera_horario_base", bid),
            "chiller_antiguedad_anios": por("chiller_antiguedad_anios", bid), "eui_objetivo_kwh_m2_anio": eui,
            "iluminacion": ed["iluminacion"], "area_fuente": ed["area_fuente"], "area_fecha": ed["area_fecha"],
            "datos_ilustrativos": ed["datos_ilustrativos"]})

    # ---- defectos de calidad en el CSV crudo: conteos exactos round(p x filas), posiciones por semilla
    N = len(base_rows)
    n = {k: round(v(f"prob_{k}") * N) for k in ("nulo", "outlier_negativo", "pico_espurio", "fecha_mixta", "duplicado")}
    pos = rng.sample(range(N), n["nulo"] + n["outlier_negativo"] + n["pico_espurio"])
    s_nulo, s_neg = set(pos[:n["nulo"]]), set(pos[n["nulo"]:n["nulo"] + n["outlier_negativo"]])
    s_pico = set(pos[n["nulo"] + n["outlier_negativo"]:])
    s_fecha, s_dup = set(rng.sample(range(N), n["fecha_mixta"])), set(rng.sample(range(N), n["duplicado"]))
    ami_rows = []
    for i, row in enumerate(base_rows):
        row = dict(row)
        if i in s_fecha:
            row["fecha_lectura"] = date.fromisoformat(row["fecha_lectura"]).strftime("%d/%m/%Y")  # formato inconsistente
        if i in s_nulo:
            row["kwh_total"] = row["kwh_horario_laboral"] = row["kwh_fuera_horario"] = ""  # perdida de paquete AMI
        elif i in s_neg:
            row["kwh_total"] = round(-abs(row["kwh_total"]), 2)  # lectura corrupta negativa
        elif i in s_pico:
            row["kwh_total"] = round(row["kwh_total"] * v("mult_pico_espurio"), 2)  # pico espurio por reset de medidor
        ami_rows.append(row)
        if i in s_dup:
            ami_rows.append(dict(row))
    defectos = {"nulos": n["nulo"], "duplicados": n["duplicado"], "outliers_negativos": n["outlier_negativo"],
                "picos_espurios": n["pico_espurio"], "fechas_mixtas": n["fecha_mixta"]}

    # ---- 1. CSV
    with open(raw_dir / ARCHIVOS[0], "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(ami_rows[0].keys()))
        w.writeheader()
        w.writerows(ami_rows)
    # ---- 2. JSON IoT
    with open(raw_dir / ARCHIVOS[1], "w", encoding="utf-8") as f:
        json.dump(iot_docs, f, ensure_ascii=False, indent=2)
    # ---- 3. XML facturacion: toda la energia a tarifa EDEMET MTD (sin contrato renovable)
    root_xml = ET.Element("FacturacionElectricaCampus", {
        "organizacion": "Fundacion Ciudad del Saber", "distribuidora": "EDEMET (inferencia geografica; probable, no confirmada)", "tarifa_regulada": "MTD",
        "pliego": "ASEP 2026-2030, Anexo A Res. AN No. 21781-Elec (ver fuentes_oficiales.json)", "anio": "2025", "moneda": "USD",
        "datos": "SINTETICOS",
        "nota_valoracion": ("Consumo 2025 valorado a la tarifa EDEMET MTD vigente de jul-2026 a jun-2030 (valoracion de referencia); "
                            "factor de emision 2024 (el mas reciente publicado)"),
        "tarifa_energia_usd_kwh": f"{t_e}", "tarifa_demanda_usd_kw_mes": f"{t_d}",
        "cargo_fijo_usd_cliente_mes": f"{t_f}", "fp_umbral": f"{fp_um}", "fp_recargo_fraccion_por_0_01": f"{fp_rec}",
        "factor_emision_kg_kwh": f"{f_em}"})
    for (bid, m), st in sorted(monthly.items()):
        kwh_m, max_kw = round(st["kwh"], 2), round(st["max_kw"], 2)
        fp_m = round(st["fp_sum"] / st["n"], 3)
        c_en, c_dem = round(kwh_m * t_e, 2), round(max_kw * t_d, 2)
        # SIMPLIFICACION: el recargo real aplica a comercializacion y distribucion; aqui se aplica sobre el cargo por demanda
        penal = round(config.recargo_fp_fraccion(fp_m, fp_um, fp_rec) * c_dem, 2)
        total = round(c_en + c_dem + t_f + penal, 2)
        fac = ET.SubElement(root_xml, "FacturaMensual", {"id_factura": f"FAC-2025-{m:02d}-{bid}", "building_id": bid, "mes": str(m)})
        for tag, val in [("kwh_facturados", kwh_m), ("demanda_maxima_kw", max_kw), ("factor_potencia_mensual", fp_m),
                         ("cargo_energia_usd", c_en), ("cargo_demanda_usd", c_dem), ("cargo_fijo_usd", t_f),
                         ("penalizacion_fp_usd", penal), ("total_factura_usd", total),
                         ("emisiones_alcance2_kg_co2", round(kwh_m * f_em, 2))]:
            ET.SubElement(fac, tag).text = f"{val:.3f}" if tag == "factor_potencia_mensual" else f"{val:.2f}"
    ET.indent(root_xml, space="  ")
    ET.ElementTree(root_xml).write(raw_dir / ARCHIVOS[2], encoding="utf-8", xml_declaration=True)
    # ---- 4. TXT bitacora (no estructurado, SINTETICO)
    txt = ["=== BITÁCORA DE MANTENIMIENTO ELÉCTRICO Y HVAC - FUNDACIÓN CIUDAD DEL SABER (2025) ===",
           f"=== DATOS SINTÉTICOS: todos los eventos son ilustrativos (parametros_generador.json, semilla {semilla}) ===", ""]
    for a in P["anomalias_operativas"]:
        mult = anom_por[a["building_id"]][2]
        txt.append(f"[{a['inicio']} a {a['fin']}] EDIFICIO={a['building_id']} | SEVERIDAD=ALTA | "
                   f"DESVIACION_ESTIMADA=+{round((mult - 1) * 100)}% | REPORTE_TECNICO: {a['descripcion']}")
    txt += [f"[{e['fecha']}] EDIFICIO={e['building_id']} | SEVERIDAD={e['severidad']} | REPORTE_TECNICO: {e['reporte']}"
            for e in P["eventos_informativos"]]
    (raw_dir / ARCHIVOS[3]).write_text("\n".join(txt), encoding="utf-8")
    # ---- catalogo
    with open(raw_dir / ARCHIVOS[4], "w", encoding="utf-8") as f:
        json.dump(catalogo, f, ensure_ascii=False, indent=2)

    # ---- verificacion: todo valor generado dentro de [min, max]
    fuera = [f"{k}: observado [{lo}, {hi}] fuera de [{pr[k]['min']}, {pr[k]['max']}]" for k, (lo, hi) in obs.items()
             if lo < pr[k]["min"] or hi > pr[k]["max"]]
    for k in ("prob_nulo", "prob_outlier_negativo", "prob_pico_espurio", "prob_fecha_mixta", "prob_duplicado"):
        tasa = n[k[5:]] / N
        if not pr[k]["min"] <= tasa <= pr[k]["max"]:
            fuera.append(f"{k}: tasa realizada {tasa:.4f} fuera de [{pr[k]['min']}, {pr[k]['max']}]")
    if fuera:
        raise AssertionError("Valores generados fuera de rango:\n  - " + "\n  - ".join(fuera))

    efectivos = {}
    for k, p in pr.items():
        e = {"min": p["min"], "max": p["max"], "unidad": p["unidad"], "ancla": p["ancla"], "rol": p["rol"]}
        if p["rol"] == "tarifa":
            e["valor_usado"] = tarifa(k)
        elif "valor" in p:
            e["valor_usado"] = p["valor"]
        elif "valores" in p:
            e["valores_usados"] = p["valores"]
        efectivos[k] = e
    meta = {"version_generador": VERSION_GENERADOR, "version_parametros": P["version"], "modo": "sintetico_anclado",
            "fecha_generacion": datetime.now().isoformat(timespec="seconds"), "semilla": semilla,
            "parametros_archivo": "parametros_generador.json", "filas_ami_raw": len(ami_rows), "documentos_iot": len(iot_docs),
            "facturas_xml": len(monthly), "defectos_inyectados": defectos,
            "tasas_defectos_realizadas": {k: round(n[k] / N, 5) for k in n},
            "eui_realizado_kwh_m2_anio": eui_real, "area_total_m2": sum(por("area_m2", e["building_id"]) for e in P["edificios"]),
            "dias_no_laborables": cuenta, "feriados_aplicados": sorted(d.isoformat() for d in feriados),
            "rangos_observados_en_datos": {k: {"min": lo, "max": hi} for k, (lo, hi) in sorted(obs.items())},
            "rangos_efectivos": efectivos,
            "anclas_oficiales_usadas": sorted({i for p in pr.values() for i in [p["ancla"], *p.get("ancla_por_valor", {}).values()] if i != SUPUESTO}
                                              | {P["calendario_no_laborable"]["ancla"]})}
    with open(raw_dir / "_metodologia.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    if verbose:
        print("Generación completada exitosamente (datos SINTÉTICOS):")
        print(f"  Semilla:             {semilla}")
        print(f"  CSV AMI rows (raw):  {len(ami_rows)}")
        print(f"  JSON IoT docs:       {len(iot_docs)}")
        print(f"  XML Facturas:        {len(monthly)}")
        print(f"  TXT Bitácora líneas: {len(txt)}")
        print(f"  Defectos inyectados: {defectos}")
        print(f"  Dias no laborables:  {cuenta}")
    return meta


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1], formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--semilla", type=int, help="semilla (defecto: la de parametros_generador.json)")
    ap.add_argument("--forzar", action="store_true", help="sobrescribe data/raw aunque no tenga _metodologia.json")
    ap.add_argument("--parametros", help="ruta alterna de parametros_generador.json")
    ap.add_argument("--raw-dir", help="carpeta de salida (defecto: data/raw)")
    a = ap.parse_args()
    try:
        generar_datos(raw_dir=a.raw_dir, params_path=a.parametros, semilla=a.semilla, forzar=a.forzar)
    except (ValueError, FileExistsError, AssertionError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
