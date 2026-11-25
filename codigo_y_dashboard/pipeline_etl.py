"""
Pipeline ETL - SIG de Gestion Energetica (Fundacion Ciudad del Saber). DATOS SINTETICOS ANCLADOS en cifras oficiales (ver README).
Arquitectura Medallion: Bronze (data/raw) -> Silver (data/silver) -> Gold (SQLite estrella + CSV + kpi_summary.json)

Fuente de datos (--fuente): "sintetico" (defecto) ejecuta generar_datos_fuente.py y luego el ETL; "real" NO ejecuta el generador:
valida que data/raw tenga los 4 archivos (+ catalogo) con el esquema esperado (ver README) y falla con exit != 0 si no.

Etapas (cada una es una funcion que el DAG de Airflow llama por separado):
  extract()          lee 4 formatos: CSV (AMI), JSON (IoT/HVAC), XML (facturas) y TXT (bitacora)
  clean()            fechas ISO, duplicados, nulos (mediana), outliers (> N x base) -> data/silver + auditoria
  transform()        enriquece con IoT, costo, CO2 y z-score; genera alertas (z >= umbral)
  load_sqlite()      modelo estrella + vistas BI en SQLite
  build_gold()       KPIs, proyeccion financiera (supuestos.json), diagnostico IA (genai_diagnostico) y CSV BI
La carga a MongoDB (mongo_store.py) y S3 (s3_publish.py) se orquestan desde el DAG o run_pipeline().
"""
import argparse
import csv
import json
import logging
import os
import re
import sqlite3
import statistics
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import config
import genai_diagnostico

log = logging.getLogger("pipeline")

MESES_ES = ["", "Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
AVISO_SINTETICO = ("Datos SINTETICOS con fines academicos (generar_datos_fuente.py, semilla fija), anclados en cifras oficiales con fecha "
                   "(fuentes_oficiales.json): tarifa EDEMET MTD (pliego jul-2026 aplicado a consumo 2025), regla de factor de potencia, factor de "
                   "emision 2024 (el mas reciente), areas de Ed. 128/108/202 (tesis 2018), clima historico de Albrook, feriados 2025 y estacionalidad "
                   "mensual de EDEMET 2025. Consumo, EUI, demas areas, factor de potencia por edificio, ahorros y CAPEX son supuestos del equipo. "
                   "Solo 6 nombres de edificios son reales; el resto es ilustrativo. EDEMET como distribuidora es probable, no confirmada.")
AVISO_REAL = "Datos provistos por el usuario (--fuente real), validados contra el esquema esperado. Las tarifas/emisiones siguen siendo las anclas oficiales."
FUENTE_ENV = "SIG_FUENTE"
PLAUS_MIN, PLAUS_MAX = 0.5, 1.0  # razon consumo simulado / TOPE de la huella 2021 (orden de magnitud, no validacion)
ANCLA_ESCALA = "tope_escala_fundacion_2021"
ANCLAS_USADAS = ("tarifa_energia_edemet_mtd", "tarifa_demanda_edemet_mtd", "cargo_fijo_edemet_mtd", "factor_potencia_regla",
                 "factor_emision_sin_2024", "huella_fundacion_2021", "factor_emision_sin_2021", ANCLA_ESCALA, "areas_cds_tesis_2018",
                 "clima_albrook_etesa_eia_2023", "feriados_2025_panama", "edemet_ventas_mensuales_2025", "utp_campus_vls_2024",
                 "utp_gei_2025_total")
CAMPOS_FACTURA = ["kwh_facturados", "demanda_maxima_kw", "factor_potencia_mensual", "cargo_energia_usd", "cargo_demanda_usd",
                  "cargo_fijo_usd", "penalizacion_fp_usd", "total_factura_usd", "emisiones_alcance2_kg_co2"]
ARCHIVOS_RAW = ("catalogo_edificios_cds.json", "telemetria_medidores_ami_2025.csv", "sensores_iot_hvac_clima_2025.json",
                "facturacion_mtd_distribuidora_2025.xml", "bitacora_mantenimiento_hvac_2025.txt")
COLS_AMI = ["fecha_lectura", "building_id", "medidor_ami_id", "kwh_total", "kwh_horario_laboral", "kwh_fuera_horario",
            "demanda_max_kw", "factor_potencia", "voltaje_promedio_v", "ocupacion_pct"]
COLS_CATALOGO = ["building_id", "nombre", "categoria", "area_m2", "anio_rehab", "cert_leed", "base_kwh_dia", "pct_fuera_horario",
                 "chiller_antiguedad_anios"]
RE_BITACORA = re.compile(r"^\[\d{4}-\d{2}-\d{2}.*EDIFICIO=[A-Z0-9\-]+.*REPORTE_TECNICO:\s*\S")


class EsquemaInvalido(ValueError):
    """data/raw no cumple el esquema esperado (modo --fuente real)."""


class PlausibilidadError(ValueError):
    """El consumo simulado esta fuera de 0.5x-1.5x del ancla de escala."""


def parse_fecha(s):
    """Devuelve (fecha ISO, True si venia en formato dd/mm/yyyy). ValueError claro si no parsea."""
    s = s.strip()
    try:
        if "/" in s:
            return datetime.strptime(s, "%d/%m/%Y").strftime("%Y-%m-%d"), True
        return datetime.strptime(s, "%Y-%m-%d").strftime("%Y-%m-%d"), False
    except ValueError as exc:
        raise ValueError(f"fecha_lectura invalida: {s!r} (se esperaba yyyy-mm-dd o dd/mm/yyyy)") from exc


# ---------------------------------------------------------------- EXTRACT
def extract(raw_dir=None):
    raw_dir = Path(raw_dir or config.raw_dir())
    with open(raw_dir / "catalogo_edificios_cds.json", encoding="utf-8") as f:
        catalogo = json.load(f)
    with open(raw_dir / "telemetria_medidores_ami_2025.csv", encoding="utf-8") as f:
        ami = list(csv.DictReader(f))
    with open(raw_dir / "sensores_iot_hvac_clima_2025.json", encoding="utf-8") as f:
        iot = json.load(f)
    facturas = {}
    for fac in ET.parse(raw_dir / "facturacion_mtd_distribuidora_2025.xml").getroot().findall("FacturaMensual"):
        d = {c: float(fac.findtext(c)) for c in CAMPOS_FACTURA}
        d["id_factura"] = fac.attrib["id_factura"]
        facturas[(fac.attrib["building_id"], int(fac.attrib["mes"]))] = d
    with open(raw_dir / "bitacora_mantenimiento_hvac_2025.txt", encoding="utf-8") as f:
        lineas = [ln.strip() for ln in f if ln.strip().startswith("[")]
    bitacora = []
    for ln in lineas:
        m_b = re.search(r"EDIFICIO=([A-Z0-9\-]+)", ln)
        m_s = re.search(r"SEVERIDAD=([A-Z]+)", ln)
        m_r = re.search(r"REPORTE_TECNICO:\s*(.+)$", ln)
        m_d = re.search(r"\[(\d{4}-\d{2}-\d{2})(?: a (\d{4}-\d{2}-\d{2}))?\]", ln)
        if m_b and m_r and m_d:
            bitacora.append({"building_id": m_b.group(1), "fecha_inicio": m_d.group(1),
                             "fecha_fin": m_d.group(2) or m_d.group(1),
                             "severidad": m_s.group(1) if m_s else "MEDIA", "reporte_tecnico": m_r.group(1)})
    iot_map = {(d["building_id"], d["timestamp"][:10]): d for d in iot}
    audit = {"raw_ami_rows": len(ami), "iot_json_docs": len(iot), "xml_facturas": len(facturas),
             "txt_bitacora_eventos": len(lineas)}
    if not (ami and iot and facturas and bitacora and catalogo):
        raise ValueError(f"Fuente vacia o ilegible: {audit}, edificios={len(catalogo)}, bitacora_parseada={len(bitacora)}")
    return {"catalogo": catalogo, "cat_map": {e["building_id"]: e for e in catalogo}, "ami": ami,
            "iot": iot, "iot_map": iot_map, "facturas": facturas, "bitacora": bitacora, "audit": audit}


# ---------------------------------------------------------------- FUENTE DE DATOS (sintetico | real)
def _num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def validar_esquema_raw(raw_dir=None, max_detalle=6):
    """Valida que data/raw tenga los archivos y el esquema esperados (ver README). Lanza EsquemaInvalido con TODOS los
    problemas encontrados (maximo `max_detalle` ejemplos por archivo). Devuelve un resumen si es valido."""
    raw_dir = Path(raw_dir or config.raw_dir())
    errores, resumen = [], {}

    def err(archivo, msg):
        errores.append(f"{archivo}: {msg}")

    faltan = [a for a in ARCHIVOS_RAW if not (raw_dir / a).is_file()]
    for a in faltan:
        err(a, f"archivo no encontrado en {raw_dir}")
    cat_ids = set()
    if ARCHIVOS_RAW[0] not in faltan:
        a = ARCHIVOS_RAW[0]
        try:
            cat = json.loads((raw_dir / a).read_text(encoding="utf-8"))
            assert isinstance(cat, list) and cat
        except (ValueError, AssertionError):
            err(a, "debe ser un JSON no vacio con una lista de edificios")
        else:
            for i, e in enumerate(cat):
                f_ = [c for c in COLS_CATALOGO if not isinstance(e, dict) or c not in e]
                if f_:
                    err(a, f"edificio #{i}: faltan campos {f_}")
                    continue
                cat_ids.add(e["building_id"])
                mal = [c for c in ("area_m2", "base_kwh_dia", "pct_fuera_horario", "chiller_antiguedad_anios", "anio_rehab") if not _num(e[c])]
                if mal or not 0 <= e["pct_fuera_horario"] <= 1:
                    err(a, f"edificio {e['building_id']}: campos numericos invalidos {mal or ['pct_fuera_horario fuera de 0-1']}")
            resumen["edificios"] = len(cat)
    if ARCHIVOS_RAW[1] not in faltan:
        a = ARCHIVOS_RAW[1]
        with open(raw_dir / a, encoding="utf-8", newline="") as f:
            rd = csv.DictReader(f)
            falta = [c for c in COLS_AMI if c not in (rd.fieldnames or [])]
            if falta:
                err(a, f"faltan columnas {falta} (esperadas: {COLS_AMI})")
            else:
                malas, n = [], 0
                for n, r in enumerate(rd, 2):
                    try:
                        parse_fecha(r["fecha_lectura"])
                        for c in ("demanda_max_kw", "factor_potencia", "voltaje_promedio_v", "ocupacion_pct"):
                            float(r[c])
                        for c in ("kwh_total", "kwh_horario_laboral", "kwh_fuera_horario"):
                            if r[c].strip():
                                float(r[c])
                        if cat_ids and r["building_id"] not in cat_ids:
                            raise ValueError(f"building_id {r['building_id']!r} no esta en el catalogo")
                    except (ValueError, TypeError, AttributeError) as exc:
                        malas.append(f"fila {n}: {exc}")
                for m in malas[:max_detalle]:
                    err(a, m)
                if len(malas) > max_detalle:
                    err(a, f"... y {len(malas) - max_detalle} filas invalidas mas")
                resumen["filas_ami"] = max(n - 1, 0)
                if n < 2:
                    err(a, "sin filas de datos")
    if ARCHIVOS_RAW[2] not in faltan:
        a = ARCHIVOS_RAW[2]
        try:
            iot = json.loads((raw_dir / a).read_text(encoding="utf-8"))
            assert isinstance(iot, list) and iot
        except (ValueError, AssertionError):
            err(a, "debe ser un JSON no vacio con una lista de documentos")
        else:
            malos = []
            for i, d in enumerate(iot):
                try:
                    assert isinstance(d["timestamp"], str) and len(d["timestamp"]) >= 10 and d["building_id"]
                    assert _num(d["ambiente"]["temp_exterior_c"]) and _num(d["ambiente"]["humedad_relativa_pct"])
                    assert isinstance(d["hvac"]["estado_chiller"], str) and _num(d["hvac"]["cop_eficiencia"])
                except (KeyError, TypeError, AssertionError):
                    malos.append(i)
            if malos:
                err(a, f"{len(malos)} documentos sin los campos requeridos (timestamp, building_id, ambiente.temp_exterior_c, "
                       f"ambiente.humedad_relativa_pct, hvac.estado_chiller, hvac.cop_eficiencia); primeros indices: {malos[:max_detalle]}")
            resumen["docs_iot"] = len(iot)
    if ARCHIVOS_RAW[3] not in faltan:
        a = ARCHIVOS_RAW[3]
        try:
            facs = ET.parse(raw_dir / a).getroot().findall("FacturaMensual")
        except ET.ParseError as exc:
            err(a, f"XML mal formado: {exc}")
        else:
            if not facs:
                err(a, "sin elementos <FacturaMensual>")
            malas = []
            for fac in facs:
                f_ = [k for k in ("id_factura", "building_id", "mes") if k not in fac.attrib]
                f_ += [c for c in CAMPOS_FACTURA if fac.findtext(c) is None]
                try:
                    [float(fac.findtext(c)) for c in CAMPOS_FACTURA]
                except (TypeError, ValueError):
                    f_.append("(valor no numerico)")
                if f_:
                    malas.append(f"{fac.attrib.get('id_factura', '?')}: {f_}")
            for m in malas[:max_detalle]:
                err(a, f"<FacturaMensual> incompleta {m}")
            resumen["facturas_xml"] = len(facs)
    if ARCHIVOS_RAW[4] not in faltan:
        a = ARCHIVOS_RAW[4]
        lineas = [ln.strip() for ln in (raw_dir / a).read_text(encoding="utf-8").splitlines() if ln.strip().startswith("[")]
        if not any(RE_BITACORA.search(ln) for ln in lineas):
            err(a, "ninguna linea con formato '[yyyy-mm-dd] EDIFICIO=ID | ... | REPORTE_TECNICO: texto'")
        resumen["eventos_bitacora"] = len(lineas)
    if errores:
        raise EsquemaInvalido(f"data/raw ({raw_dir}) no cumple el esquema esperado:\n  - " + "\n  - ".join(errores))
    return resumen


def preparar_fuente(fuente=None):
    """Primera tarea del flujo. sintetico: ejecuta el generador. real: NO lo ejecuta; solo valida data/raw."""
    fuente = fuente or os.environ.get(FUENTE_ENV) or "sintetico"
    if fuente == "real":
        res = validar_esquema_raw()
        log.info("Fuente REAL: esquema de data/raw valido (%s); el generador NO se ejecuta", res)
        return {"fuente": "real", **res}
    import generar_datos_fuente as gen
    meta = gen.generar_datos(verbose=False)
    log.info("Fuente SINTETICA: datos regenerados (semilla %s, defectos %s)", meta["semilla"], meta["defectos_inyectados"])
    return {"fuente": "sintetico", "semilla": meta["semilla"], "defectos_inyectados": meta["defectos_inyectados"]}


# ---------------------------------------------------------------- CLEAN
def clean_ami(raw_ami, cat_map, outlier_mult=3.0):
    """Limpieza pura (sin E/S). Devuelve (filas silver, contadores de auditoria)."""
    audit = {"fechas_normalizadas": 0, "duplicados_eliminados": 0, "nulos_imputados": 0, "outliers_corregidos": 0}
    vistos, validos, prelim = set(), defaultdict(list), []
    for row in raw_ami:
        bid = row["building_id"]
        if bid not in cat_map:
            raise ValueError(f"building_id {bid!r} no existe en el catalogo")
        fecha, mixta = parse_fecha(row["fecha_lectura"])
        audit["fechas_normalizadas"] += mixta
        if (bid, fecha) in vistos:
            audit["duplicados_eliminados"] += 1
            continue
        vistos.add((bid, fecha))
        kwh = None
        if row["kwh_total"].strip() == "":
            audit["nulos_imputados"] += 1
        else:
            v = float(row["kwh_total"])
            if v <= 0 or v > cat_map[bid]["base_kwh_dia"] * outlier_mult:
                audit["outliers_corregidos"] += 1
            else:
                kwh = v
                validos[bid].append(v)
        opt = lambda c: float(row[c]) if row[c].strip() else None  # noqa: E731
        prelim.append({"fecha": fecha, "building_id": bid, "medidor_ami_id": row["medidor_ami_id"],
                       "kwh_total": kwh, "kwh_horario_laboral": opt("kwh_horario_laboral"),
                       "kwh_fuera_horario": opt("kwh_fuera_horario"), "demanda_max_kw": float(row["demanda_max_kw"]),
                       "factor_potencia": float(row["factor_potencia"]),
                       "voltaje_promedio_v": float(row["voltaje_promedio_v"]),
                       "ocupacion_pct": float(row["ocupacion_pct"])})
    # sin lecturas validas para un edificio se usa el consumo base del catalogo
    mediana = {b: statistics.median(v) for b, v in validos.items()}
    for r in prelim:
        bid = r["building_id"]
        pct_off = cat_map[bid]["pct_fuera_horario"]
        if r["kwh_total"] is None:
            r["kwh_total"] = round(mediana.get(bid, cat_map[bid]["base_kwh_dia"]), 2)
            r["kwh_fuera_horario"] = r["kwh_horario_laboral"] = None
        if r["kwh_fuera_horario"] is None or r["kwh_horario_laboral"] is None:
            r["kwh_fuera_horario"] = round(r["kwh_total"] * pct_off, 2)
            r["kwh_horario_laboral"] = round(r["kwh_total"] - r["kwh_fuera_horario"], 2)
    return prelim, audit


def clean(raw, silver_dir=None, supuestos=None):
    sup = supuestos or config.cargar_supuestos()
    silver_dir = Path(silver_dir or config.silver_dir())
    silver_dir.mkdir(parents=True, exist_ok=True)
    rows, a = clean_ami(raw["ami"], raw["cat_map"], config.valor(sup, "deteccion_anomalias", "outlier_multiplo_base"))
    audit = {**raw["audit"], **a, "silver_ami_rows": len(rows)}
    with open(silver_dir / "ami_limpio_2025.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    with open(silver_dir / "auditoria_calidad.json", "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)
    return rows, audit


def read_silver(silver_dir=None):
    num = {"kwh_total", "kwh_horario_laboral", "kwh_fuera_horario", "demanda_max_kw", "factor_potencia",
           "voltaje_promedio_v", "ocupacion_pct"}
    with open(Path(silver_dir or config.silver_dir()) / "ami_limpio_2025.csv", encoding="utf-8") as f:
        return [{k: (float(v) if k in num else v) for k, v in r.items()} for r in csv.DictReader(f)]


# ---------------------------------------------------------------- TRANSFORM
def _es_seca(dt):
    """Estacion seca aproximada de Panama: ~11-dic a ~26-abr (el resto es lluviosa)."""
    return (dt.month, dt.day) >= (12, 11) or (dt.month, dt.day) <= (4, 26)


def transform(raw, silver_rows, supuestos=None):
    sup = supuestos or config.cargar_supuestos()
    t = lambda c: config.valor(sup, "tarifas_y_emisiones", c)  # noqa: E731
    z_umbral = config.valor(sup, "deteccion_anomalias", "z_score_umbral")
    por_ed = defaultdict(list)
    for r in silver_rows:
        por_ed[r["building_id"]].append(r["kwh_total"])
    media = {b: statistics.mean(v) for b, v in por_ed.items()}
    desv = {b: statistics.stdev(v) for b, v in por_ed.items()}
    tiempo, hechos, alertas = {}, [], []
    for r in silver_rows:
        f_iso, bid = r["fecha"], r["building_id"]
        dt = datetime.strptime(f_iso, "%Y-%m-%d")
        if f_iso not in tiempo:
            tiempo[f_iso] = (f_iso, dt.year, dt.month, MESES_ES[dt.month], f"T{(dt.month - 1) // 3 + 1}",
                             "Seca (Verano)" if _es_seca(dt) else "Lluviosa (Invierno)",
                             dt.weekday(), 1 if dt.weekday() >= 5 else 0)
        iot = raw["iot_map"][(bid, f_iso)]
        kwh = r["kwh_total"]
        costo = round(kwh * t("tarifa_energia_usd_kwh"), 2)  # solo cargo de energia (demanda, fijo y FP se facturan por mes)
        co2 = round(kwh * t("factor_emision_kg_kwh"), 2)  # 0.1501 kg/kWh a toda la energia
        z = round((kwh - media[bid]) / desv[bid], 2)
        flag = 1 if z >= z_umbral else 0
        hechos.append((f_iso, bid, kwh, r["kwh_horario_laboral"], r["kwh_fuera_horario"], r["demanda_max_kw"],
                       r["factor_potencia"], r["ocupacion_pct"], iot["ambiente"]["temp_exterior_c"],
                       iot["ambiente"]["humedad_relativa_pct"], iot["hvac"]["cop_eficiencia"],
                       iot["hvac"]["estado_chiller"], costo, co2, z, flag))
        if flag:
            # recuperacion (no generacion): causa registrada en la bitacora para ese edificio y fecha
            causa = "Sin entrada en bitacora para esa fecha; requiere inspeccion BMS/HVAC."
            for e in raw["bitacora"]:
                if e["building_id"] == bid and e["fecha_inicio"] <= f_iso <= e["fecha_fin"]:
                    causa = e["reporte_tecnico"]
                    break
            alertas.append((f_iso, bid, "ANOMALIA_CONSUMO_PICO", "ALTA", round((kwh / media[bid] - 1.0) * 100, 1),
                            "Abrir orden CMMS y notificar a Operaciones (accion recomendada; integracion CMMS no implementada)",
                            causa))
    return {"tiempo": list(tiempo.values()), "hechos": hechos, "alertas": alertas}


# ---------------------------------------------------------------- LOAD (SQLite)
SCHEMA = """
CREATE TABLE dim_edificio (building_id TEXT PRIMARY KEY, nombre TEXT NOT NULL, categoria TEXT NOT NULL,
    area_m2 REAL NOT NULL, anio_rehab INTEGER NOT NULL, cert_leed TEXT NOT NULL, certificacion_estado TEXT NOT NULL,
    nombre_estado TEXT NOT NULL, area_fuente TEXT NOT NULL, area_fecha TEXT NOT NULL, chiller_antiguedad_anios INTEGER NOT NULL);
CREATE TABLE dim_tiempo (date_key TEXT PRIMARY KEY, anio INTEGER NOT NULL, mes INTEGER NOT NULL,
    mes_nombre TEXT NOT NULL, trimestre TEXT NOT NULL, temporada_panama TEXT NOT NULL, dia_semana INTEGER NOT NULL,
    es_fin_semana INTEGER NOT NULL);
CREATE TABLE fact_consumo_diario (fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_key TEXT NOT NULL REFERENCES dim_tiempo(date_key), building_id TEXT NOT NULL REFERENCES dim_edificio(building_id),
    kwh_total REAL NOT NULL, kwh_horario_laboral REAL NOT NULL, kwh_fuera_horario REAL NOT NULL,
    demanda_max_kw REAL NOT NULL, factor_potencia REAL NOT NULL, ocupacion_pct REAL NOT NULL,
    temp_exterior_c REAL NOT NULL, humedad_relativa_pct REAL NOT NULL, cop_chiller REAL NOT NULL,
    estado_chiller TEXT NOT NULL, costo_energia_est_usd REAL NOT NULL, emisiones_kg_co2 REAL NOT NULL,
    z_score_consumo REAL NOT NULL, flag_anomalia INTEGER NOT NULL);
CREATE TABLE fact_facturacion_mensual (id_factura TEXT PRIMARY KEY,
    building_id TEXT NOT NULL REFERENCES dim_edificio(building_id), mes INTEGER NOT NULL,
    kwh_facturados REAL NOT NULL, demanda_maxima_kw REAL NOT NULL, factor_potencia_mensual REAL NOT NULL,
    cargo_energia_usd REAL NOT NULL, cargo_demanda_usd REAL NOT NULL, cargo_fijo_usd REAL NOT NULL,
    penalizacion_fp_usd REAL NOT NULL, total_factura_usd REAL NOT NULL, emisiones_alcance2_kg_co2 REAL NOT NULL);
CREATE TABLE fact_alertas_automatizadas (alerta_id INTEGER PRIMARY KEY AUTOINCREMENT, fecha TEXT NOT NULL,
    building_id TEXT NOT NULL, tipo_alerta TEXT NOT NULL, severidad TEXT NOT NULL, desviacion_pct REAL NOT NULL,
    accion_recomendada TEXT NOT NULL, causa_bitacora TEXT NOT NULL);

CREATE VIEW vw_bi_ranking_edificios AS
WITH fac_agg AS (
    SELECT building_id, ROUND(SUM(kwh_facturados), 0) AS kwh_anual, ROUND(MAX(demanda_maxima_kw), 1) AS demanda_pico_kw,
           ROUND(AVG(factor_potencia_mensual), 3) AS fp_promedio, ROUND(SUM(cargo_energia_usd), 2) AS costo_energia_usd,
           ROUND(SUM(cargo_demanda_usd), 2) AS costo_demanda_usd, ROUND(SUM(cargo_fijo_usd), 2) AS costo_fijo_usd,
           ROUND(SUM(penalizacion_fp_usd), 2) AS penalizacion_fp_usd,
           ROUND(SUM(total_factura_usd), 2) AS costo_total_anual_usd,
           ROUND(SUM(emisiones_alcance2_kg_co2) / 1000.0, 2) AS emisiones_tco2
    FROM fact_facturacion_mensual GROUP BY building_id),
dia_agg AS (
    SELECT building_id, ROUND(SUM(kwh_fuera_horario) * 100.0 / SUM(kwh_total), 1) AS pct_fuera_horario,
           ROUND(SUM(kwh_fuera_horario), 0) AS kwh_fuera_horario_anual, ROUND(AVG(cop_chiller), 2) AS cop_promedio,
           SUM(flag_anomalia) AS dias_anomalia
    FROM fact_consumo_diario GROUP BY building_id)
SELECT e.building_id, e.nombre, e.categoria, e.area_m2, e.area_fuente, e.area_fecha, e.cert_leed, e.certificacion_estado, e.nombre_estado,
       e.chiller_antiguedad_anios, f.kwh_anual, ROUND(f.kwh_anual / e.area_m2, 1) AS eui_kwh_m2, f.demanda_pico_kw, f.fp_promedio,
       f.costo_energia_usd, f.costo_demanda_usd, f.costo_fijo_usd, f.penalizacion_fp_usd, f.costo_total_anual_usd, f.emisiones_tco2,
       d.pct_fuera_horario, d.kwh_fuera_horario_anual, d.cop_promedio, d.dias_anomalia
FROM dim_edificio e JOIN fac_agg f ON e.building_id = f.building_id JOIN dia_agg d ON e.building_id = d.building_id
ORDER BY f.kwh_anual DESC;

CREATE VIEW vw_bi_tendencia_mensual AS
WITH fac_m AS (
    SELECT mes, ROUND(SUM(kwh_facturados), 0) AS kwh_mes, ROUND(SUM(demanda_maxima_kw), 1) AS suma_demandas_kw,
           ROUND(SUM(total_factura_usd), 2) AS costo_total_usd, ROUND(SUM(cargo_demanda_usd), 2) AS costo_demanda_usd,
           ROUND(SUM(penalizacion_fp_usd), 2) AS penal_fp_usd,
           ROUND(SUM(emisiones_alcance2_kg_co2) / 1000.0, 2) AS emisiones_tco2
    FROM fact_facturacion_mensual GROUP BY mes),
cli_m AS (
    SELECT t.mes, t.mes_nombre,
           CASE WHEN SUM(CASE WHEN t.temporada_panama LIKE 'Seca%' THEN 1 ELSE 0 END) * 2 >= COUNT(*) THEN 'Seca (Verano)'
                ELSE 'Lluviosa (Invierno)' END AS temporada, ROUND(AVG(d.temp_exterior_c), 1) AS temp_prom_c,
           ROUND(AVG(d.humedad_relativa_pct), 1) AS humedad_prom_pct
    FROM fact_consumo_diario d JOIN dim_tiempo t ON d.date_key = t.date_key GROUP BY t.mes)
SELECT f.mes, c.mes_nombre, c.temporada, f.kwh_mes, f.suma_demandas_kw, f.costo_total_usd,
       f.costo_demanda_usd, f.penal_fp_usd, f.emisiones_tco2, c.temp_prom_c, c.humedad_prom_pct
FROM fac_m f JOIN cli_m c ON f.mes = c.mes ORDER BY f.mes;

CREATE VIEW vw_bi_detalle_mensual AS
WITH d AS (
    SELECT x.building_id, t.mes, ROUND(SUM(x.kwh_total), 2) AS kwh_diario, ROUND(SUM(x.kwh_fuera_horario), 2) AS kwh_fuera_horario,
           SUM(x.flag_anomalia) AS dias_anomalia, ROUND(AVG(x.temp_exterior_c), 2) AS temp_prom_c
    FROM fact_consumo_diario x JOIN dim_tiempo t ON x.date_key = t.date_key GROUP BY x.building_id, t.mes)
SELECT f.building_id, f.mes, f.kwh_facturados AS kwh_mes,
       f.demanda_maxima_kw, f.factor_potencia_mensual AS fp_mensual, f.cargo_energia_usd AS costo_energia_usd,
       f.cargo_demanda_usd AS costo_demanda_usd, f.cargo_fijo_usd AS costo_fijo_usd, f.penalizacion_fp_usd, f.total_factura_usd AS costo_total_usd,
       f.emisiones_alcance2_kg_co2 AS emisiones_kg_co2, d.kwh_diario, d.kwh_fuera_horario, d.dias_anomalia, d.temp_prom_c
FROM fact_facturacion_mensual f JOIN d ON d.building_id = f.building_id AND d.mes = f.mes
ORDER BY f.building_id, f.mes;
"""


def load_sqlite(raw, tf, db_path=None):
    db_path = Path(db_path or config.gold_dir() / "cds_energia_sig.db")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.executemany("INSERT INTO dim_edificio VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                         [(e["building_id"], e["nombre"], e["categoria"], e["area_m2"], e["anio_rehab"], e["cert_leed"],
                           e.get("certificacion_estado", "sin dato"), e.get("nombre_estado", "sin dato"),
                           e.get("area_fuente", "sin dato"), e.get("area_fecha", "sin dato"), e["chiller_antiguedad_anios"]) for e in raw["catalogo"]])
        conn.executemany("INSERT INTO dim_tiempo VALUES (?,?,?,?,?,?,?,?)", tf["tiempo"])
        conn.executemany("""INSERT INTO fact_consumo_diario (date_key, building_id, kwh_total, kwh_horario_laboral,
            kwh_fuera_horario, demanda_max_kw, factor_potencia, ocupacion_pct, temp_exterior_c, humedad_relativa_pct,
            cop_chiller, estado_chiller, costo_energia_est_usd, emisiones_kg_co2, z_score_consumo, flag_anomalia)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", tf["hechos"])
        conn.executemany("""INSERT INTO fact_alertas_automatizadas (fecha, building_id, tipo_alerta, severidad,
            desviacion_pct, accion_recomendada, causa_bitacora) VALUES (?,?,?,?,?,?,?)""", tf["alertas"])
        conn.executemany("INSERT INTO fact_facturacion_mensual VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                         [(f["id_factura"], b, m, f["kwh_facturados"], f["demanda_maxima_kw"], f["factor_potencia_mensual"],
                           f["cargo_energia_usd"], f["cargo_demanda_usd"], f["cargo_fijo_usd"], f["penalizacion_fp_usd"],
                           f["total_factura_usd"], f["emisiones_alcance2_kg_co2"]) for (b, m), f in sorted(raw["facturas"].items())])
        conn.commit()
    finally:
        conn.close()
    return db_path


def _query(conn, sql):
    cur = conn.execute(sql)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def leer_alertas(db_path=None):
    conn = sqlite3.connect(Path(db_path or config.gold_dir() / "cds_energia_sig.db"))
    try:
        return _query(conn, "SELECT fecha, building_id, tipo_alerta, severidad, desviacion_pct, accion_recomendada, "
                            "causa_bitacora FROM fact_alertas_automatizadas ORDER BY fecha, building_id")
    finally:
        conn.close()


# ---------------------------------------------------------------- GOLD
def _csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def chequeo_plausibilidad(consumo_kwh, fuentes=None, minimo=PLAUS_MIN, maximo=PLAUS_MAX):
    """Compara el consumo anual del campus con el TOPE de la huella 2021 de la Fundacion (450 tCO2e / 0.13 tCO2e/MWh = 3,462 MWh,
    deduccion del equipo: techo si toda la huella fuera electricidad). Se exige 0.5x-1.0x del tope. Es una comparacion de ORDEN DE MAGNITUD,
    no una validacion. El campus de la UTP (10,772,377 kWh en 2024) solo se reporta como CONTRASTE."""
    fuentes = fuentes or config.cargar_fuentes()
    ancla = config.ancla(fuentes, ANCLA_ESCALA)
    razon = consumo_kwh / ancla["valor"]
    rng = ancla["rango_sensibilidad"]
    utp = config.ancla(fuentes, "utp_campus_vls_2024")
    return {"consumo_simulado_mwh": round(consumo_kwh / 1000.0, 1), "ancla_id": ancla["id"], "ancla_mwh": ancla["valor"] / 1000.0,
            "ancla_estado": ancla["estado"], "ancla_periodo": ancla["periodo_del_dato"],
            "ancla_rango_mwh": [rng["min"] / 1000.0, rng["max"] / 1000.0],
            "razon_simulado_sobre_ancla": round(razon, 3), "rango_aceptado": [minimo, maximo],
            "dentro_de_rango": minimo <= razon <= maximo,
            "contraste_utp": {"ancla_id": utp["id"], "periodo": utp["periodo_del_dato"], "mwh": sum(utp["valor"]) / 1000.0,
                              "razon_simulado_sobre_utp": round(consumo_kwh / sum(utp["valor"]), 3),
                              "nota": "Solo contraste: la UTP es otra institucion y mucho mayor; no se exige ninguna razon."},
            "nota": "Comparacion de orden de magnitud contra un TOPE deducido de la huella 2021 (450 tCO2e; alcances desconocidos); NO es una validacion."}


def _txt(v):
    return json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v)


def filas_tabla_fechas(fuentes=None):
    """Una fila por dato de fuentes_oficiales.json: dato, valor, periodo, publicacion, consulta, URL, estado."""
    fuentes = fuentes or config.cargar_fuentes()
    filas = []
    for a in fuentes["fuentes"]:
        v = _txt(a["valor"])
        filas.append({"dato": a["id"], "valor": v if len(v) <= 90 else v[:87] + "...", "periodo": a["periodo_del_dato"],
                      "publicacion": a["fecha_publicacion"], "consulta": a["fecha_consulta"], "url": a["url"] or "no indicada",
                      "estado": a["estado"]})
    for a in fuentes["orientaciones_no_confirmadas"] + fuentes["inferencias"]:
        filas.append({"dato": a["id"], "valor": _txt(a.get("valor", a["concepto"]))[:90], "periodo": a["periodo_del_dato"],
                      "publicacion": a["fecha_publicacion"], "consulta": a["fecha_consulta"], "url": a.get("referencia", "no indicada"),
                      "estado": a["estado"]})
    return filas


def procedencia_datos(fuente, total_kwh, fuentes=None):
    fuentes = fuentes or config.cargar_fuentes()
    meta_path = config.raw_dir() / "_metodologia.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
    claves = ("id", "valor", "unidad", "estado", "entidad", "url", "periodo_del_dato", "fecha_publicacion", "fecha_consulta")
    anclas = [{k: a[k] for k in claves} for a in (config.ancla(fuentes, i) for i in ANCLAS_USADAS)]
    return {
        "modo": "sintetico_anclado" if fuente == "sintetico" else "real",
        "semilla": meta.get("semilla") if fuente == "sintetico" else None,
        "metodologia": ({"archivo": "data/raw/_metodologia.json", "version_generador": meta.get("version_generador"),
                         "fecha_generacion": meta.get("fecha_generacion"), "defectos_inyectados": meta.get("defectos_inyectados"),
                         "dias_no_laborables": meta.get("dias_no_laborables")}
                        if fuente == "sintetico" and meta else None),
        "fecha_consulta_fuentes": fuentes["fecha_consulta"],
        "anclas_usadas": anclas,
        "fechas_datos": filas_tabla_fechas(fuentes),
        "inferencias_no_confirmadas": [{"concepto": x["concepto"], "estado": x["estado"]} for x in fuentes["inferencias"]],
        "supuestos_del_equipo": fuentes["sin_fuente_oficial"]["conceptos"],
        "simplificaciones": ["Recargo por factor de potencia aplicado sobre el cargo por demanda (la regla real aplica a comercializacion y distribucion).",
                             "Cada edificio se factura como un cliente MTD con cargo fijo propio.",
                             "Consumo 2025 valorado a la tarifa EDEMET vigente de jul-2026 a jun-2030 (valoracion de referencia; anacronismo declarado).",
                             "Factor de emision 2024 (0.1501) aplicado a 2025: es el mas reciente publicado, no existe factor 2025.",
                             "Clima: climatologia historica de Albrook (66 anios), no el clima real de 2025.",
                             "Consumo escalado con un tope deducido de la huella 2021 de la Fundacion (alcances desconocidos)."],
        "plausibilidad": chequeo_plausibilidad(total_kwh, fuentes),
    }


def build_gold(db_path=None, raw=None, supuestos=None, fuente=None):
    """KPIs + proyeccion + diagnostico IA + CSV BI. Lee del SQLite (fuente unica de verdad)."""
    sup = supuestos or config.cargar_supuestos()
    fuente = fuente or os.environ.get(FUENTE_ENV) or "sintetico"
    raw = raw or extract()
    gold = config.gold_dir()
    gold.mkdir(parents=True, exist_ok=True)
    db_path = Path(db_path or gold / "cds_energia_sig.db")
    conn = sqlite3.connect(db_path)
    try:
        ranking = _query(conn, "SELECT * FROM vw_bi_ranking_edificios")
        mensual = _query(conn, "SELECT * FROM vw_bi_tendencia_mensual")
        detalle = _query(conn, "SELECT * FROM vw_bi_detalle_mensual")
        pct_off_campus = conn.execute("SELECT ROUND(SUM(kwh_fuera_horario) * 100.0 / SUM(kwh_total), 1) FROM fact_consumo_diario").fetchone()[0]
    finally:
        conn.close()
    alertas = leer_alertas(db_path)
    with open(config.silver_dir() / "auditoria_calidad.json", encoding="utf-8") as f:
        audit = json.load(f)

    tot = lambda c: sum(x[c] for x in ranking)  # noqa: E731
    total_kwh, total_costo, total_area = tot("kwh_anual"), tot("costo_total_anual_usd"), tot("area_m2")
    total_dem, total_penal, total_co2 = tot("costo_demanda_usd"), tot("penalizacion_fp_usd"), round(tot("emisiones_tco2"), 2)
    proc = procedencia_datos(fuente, total_kwh)
    if fuente == "sintetico" and not proc["plausibilidad"]["dentro_de_rango"]:
        p = proc["plausibilidad"]
        raise PlausibilidadError(f"Consumo simulado {p['consumo_simulado_mwh']} MWh = {p['razon_simulado_sobre_ancla']}x el ancla de escala "
                                 f"({p['ancla_mwh']} MWh); se exige {PLAUS_MIN}x-{PLAUS_MAX}x. Revise parametros_generador.json (EUI/areas).")
    _csv(gold / "tabla_fechas_datos.csv", proc["fechas_datos"])
    _csv(gold / "bi_ranking_edificios_2025.csv", ranking)
    _csv(gold / "bi_tendencia_mensual_2025.csv", mensual)

    a = lambda c: config.valor(sup, "ahorro", c)  # noqa: E731
    ahorro_bms = round(total_costo * a("bms_cargas_fantasma_pct"), 2)
    ahorro_peak = round(total_dem * a("peak_shaving_demanda_pct"), 2)
    ahorro_fp = round(total_penal * a("eliminacion_penalizacion_fp_pct"), 2)
    ahorro_hvac = round(sum(x["costo_energia_usd"] for x in ranking[:3]) * a("retrofit_hvac_top3_pct"), 2)
    ahorro = round(ahorro_bms + ahorro_peak + ahorro_fp + ahorro_hvac, 2)
    capex = config.valor(sup, "inversion", "capex_usd")
    payback = round(capex / ahorro, 2)
    ref = next((x for x in ranking if x["building_id"] == "CDS-DRM"), ranking[-1])

    summary = {
        "organizacion": "Fundación Ciudad del Saber (Clayton, Panamá)",
        "periodo": "Enero - Diciembre 2025",
        "aviso_datos": AVISO_SINTETICO if fuente == "sintetico" else AVISO_REAL,
        "procedencia_datos": proc,
        "auditoria_calidad": audit,
        "kpis_globales": {
            "total_edificios": len(ranking), "area_total_m2": total_area, "consumo_total_kwh": total_kwh,
            "consumo_total_mwh": round(total_kwh / 1000.0, 1), "eui_promedio_kwh_m2": round(total_kwh / total_area, 1),
            "eui_referencia_hospedaje": ref["eui_kwh_m2"], "eui_referencia_edificio": ref["nombre"],
            "eui_maximo": ranking[0]["eui_kwh_m2"],
            "costo_total_anual_usd": round(total_costo, 2), "costo_energia_anual_usd": round(tot("costo_energia_usd"), 2),
            "costo_demanda_anual_usd": round(total_dem, 2), "costo_fijo_anual_usd": round(tot("costo_fijo_usd"), 2),
            "pct_costo_demanda": round(total_dem * 100.0 / total_costo, 1),
            "penalizacion_fp_anual_usd": round(total_penal, 2), "emisiones_totales_tco2": total_co2,
            "pct_fuera_horario_campus": pct_off_campus,
            "concentracion_top3_pct": round(sum(x["kwh_anual"] for x in ranking[:3]) * 100.0 / total_kwh, 1),
            "concentracion_top4_pct": round(sum(x["kwh_anual"] for x in ranking[:4]) * 100.0 / total_kwh, 1),
            "alertas_anomalias_detectadas": len(alertas),
        },
        "impacto_financiero_proyectado": {
            "ahorro_bms_cargas_fantasma_usd": ahorro_bms, "ahorro_peak_shaving_demanda_usd": ahorro_peak,
            "ahorro_eliminacion_penal_fp_usd": ahorro_fp, "ahorro_retrofit_hvac_top3_usd": ahorro_hvac,
            "ahorro_total_anual_usd": ahorro, "porcentaje_ahorro_sobre_factura": round(ahorro * 100.0 / total_costo, 1),
            "inversion_estimada_usd": capex, "payback_anios": payback, "payback_meses": round(payback * 12, 1),
            "roi_3_anios_pct": round(((ahorro * 3 - capex) / capex) * 100, 1),
            "co2_evitado_anual_tco2": round(total_co2 * (ahorro / total_costo), 1),
            "nota": ("Proyeccion basada en supuestos.json: las tarifas y el factor de emision (0.1501 kg/kWh) son oficiales; los porcentajes de "
                     "ahorro y el CAPEX son supuestos del equipo. CO2 evitado = emisiones totales x (ahorro / factura), aproximacion proporcional. "
                     "No es un resultado medido."),
        },
        "supuestos": sup,
        "ranking_edificios": ranking,
        "tendencia_mensual": mensual,
        "detalle_mensual": detalle,
        "alertas": alertas,
        "bitacora_eventos": raw["bitacora"],
    }
    summary["diagnostico_gerencial"] = genai_diagnostico.generar_diagnostico_gerencial(summary, raw["bitacora"])
    with open(gold / "kpi_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    return summary


def run_pipeline(guardar_nosql=True, fuente=None):
    """Fuente de datos -> extract -> clean -> transform -> SQLite -> gold (+ Mongo opcional/fallback)."""
    fuente = fuente or os.environ.get(FUENTE_ENV) or "sintetico"
    os.environ[FUENTE_ENV] = fuente
    preparar_fuente(fuente)
    raw = extract()
    rows, _ = clean(raw)
    tf = transform(raw, rows)
    db = load_sqlite(raw, tf)
    if guardar_nosql:
        import mongo_store
        mongo_store.guardar_alertas_y_telemetria(leer_alertas(db), raw["iot"])
    s = build_gold(db, raw, fuente=fuente)
    print(f"=== PIPELINE ETL EJECUTADO (fuente: {fuente}) ===")
    print(json.dumps(s["procedencia_datos"]["plausibilidad"], indent=2, ensure_ascii=False))
    for k in ("auditoria_calidad", "kpis_globales", "impacto_financiero_proyectado"):
        print(json.dumps(s[k], indent=2, ensure_ascii=False))
    print(f"\nDiagnostico gerencial [{s['diagnostico_gerencial']['etiqueta']}]:\n{s['diagnostico_gerencial']['texto']}")
    print("\nTop 5 Edificios por Consumo Anual:")
    for r in s["ranking_edificios"][:5]:
        print(f"  {r['building_id']} | {r['nombre']:38s} | {r['kwh_anual']:10,.0f} kWh | EUI={r['eui_kwh_m2']:5.1f} | {r['costo_total_anual_usd']:10,.2f} USD | FP={r['fp_promedio']}")
    return s


def main(argv=None):
    ap = argparse.ArgumentParser(description="Pipeline ETL del SIG energetico (Ciudad del Saber)")
    ap.add_argument("--fuente", choices=("sintetico", "real"), default=os.environ.get(FUENTE_ENV) or "sintetico",
                    help="sintetico: genera data/raw y corre el ETL; real: NO genera, valida el esquema de data/raw y corre el ETL")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    try:
        run_pipeline(fuente=a.fuente)
    except (EsquemaInvalido, PlausibilidadError, FileExistsError) as exc:
        print(f"ERROR ({type(exc).__name__}): {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
