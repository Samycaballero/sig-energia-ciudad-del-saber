"""
Pipeline ETL - SIG de Gestion Energetica (Fundacion Ciudad del Saber). DATOS SINTETICOS (ver README).
Arquitectura Medallion: Bronze (data/raw) -> Silver (data/silver) -> Gold (SQLite estrella + CSV + kpi_summary.json)

Etapas (cada una es una funcion que el DAG de Airflow llama por separado):
  extract()          lee 4 formatos: CSV (AMI), JSON (IoT/HVAC), XML (facturas) y TXT (bitacora)
  clean()            fechas ISO, duplicados, nulos (mediana), outliers (> N x base) -> data/silver + auditoria
  transform()        enriquece con IoT, costo, CO2 y z-score; genera alertas (z >= umbral)
  load_sqlite()      modelo estrella + vistas BI en SQLite
  build_gold()       KPIs, proyeccion financiera (supuestos.json), diagnostico IA (genai_diagnostico) y CSV BI
La carga a MongoDB (mongo_store.py) y S3 (s3_publish.py) se orquestan desde el DAG o run_pipeline().
"""
import csv
import json
import logging
import os
import re
import sqlite3
import statistics
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import config
import genai_diagnostico

log = logging.getLogger("pipeline")

MESES_ES = ["", "Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
AVISO_DATOS = ("Datos sinteticos generados con fines academicos (generar_datos_fuente.py, semilla fija). "
               "Los nombres de edificios provienen de un catalogo; las lecturas no son mediciones reales.")


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
        campos = ["kwh_facturados", "kwh_ppa_renovable", "kwh_red_sin", "demanda_maxima_kw",
                  "factor_potencia_mensual", "cargo_energia_usd", "cargo_demanda_usd",
                  "penalizacion_fp_usd", "total_factura_usd", "emisiones_alcance2_kg_co2"]
        d = {c: float(fac.findtext(c)) for c in campos}
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
                             "Seca (Verano)" if dt.month in (1, 2, 3, 4) else "Lluviosa (Invierno)",
                             dt.weekday(), 1 if dt.weekday() >= 5 else 0)
        iot = raw["iot_map"][(bid, f_iso)]
        kwh, ppa = r["kwh_total"], raw["cat_map"][bid]["pct_ppa"]
        costo = round(kwh * ppa * t("tarifa_ppa_usd_kwh") + kwh * (1 - ppa) * t("tarifa_red_usd_kwh"), 2)
        co2 = round(kwh * (1 - ppa) * t("factor_emision_red_kg_kwh") + kwh * ppa * t("factor_emision_ppa_kg_kwh"), 2)
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
    area_m2 REAL NOT NULL, anio_rehab INTEGER NOT NULL, cert_leed TEXT NOT NULL, pct_ppa_renovable REAL NOT NULL,
    chiller_antiguedad_anios INTEGER NOT NULL);
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
    kwh_facturados REAL NOT NULL, kwh_ppa_renovable REAL NOT NULL, kwh_red_sin REAL NOT NULL,
    demanda_maxima_kw REAL NOT NULL, factor_potencia_mensual REAL NOT NULL, cargo_energia_usd REAL NOT NULL,
    cargo_demanda_usd REAL NOT NULL, penalizacion_fp_usd REAL NOT NULL, total_factura_usd REAL NOT NULL,
    emisiones_alcance2_kg_co2 REAL NOT NULL);
CREATE TABLE fact_alertas_automatizadas (alerta_id INTEGER PRIMARY KEY AUTOINCREMENT, fecha TEXT NOT NULL,
    building_id TEXT NOT NULL, tipo_alerta TEXT NOT NULL, severidad TEXT NOT NULL, desviacion_pct REAL NOT NULL,
    accion_recomendada TEXT NOT NULL, causa_bitacora TEXT NOT NULL);

CREATE VIEW vw_bi_ranking_edificios AS
WITH fac_agg AS (
    SELECT building_id, ROUND(SUM(kwh_facturados), 0) AS kwh_anual, ROUND(SUM(kwh_ppa_renovable), 0) AS kwh_ppa,
           ROUND(SUM(kwh_red_sin), 0) AS kwh_red, ROUND(MAX(demanda_maxima_kw), 1) AS demanda_pico_kw,
           ROUND(AVG(factor_potencia_mensual), 3) AS fp_promedio, ROUND(SUM(cargo_energia_usd), 2) AS costo_energia_usd,
           ROUND(SUM(cargo_demanda_usd), 2) AS costo_demanda_usd, ROUND(SUM(penalizacion_fp_usd), 2) AS penalizacion_fp_usd,
           ROUND(SUM(total_factura_usd), 2) AS costo_total_anual_usd,
           ROUND(SUM(emisiones_alcance2_kg_co2) / 1000.0, 2) AS emisiones_tco2
    FROM fact_facturacion_mensual GROUP BY building_id),
dia_agg AS (
    SELECT building_id, ROUND(SUM(kwh_fuera_horario) * 100.0 / SUM(kwh_total), 1) AS pct_fuera_horario,
           ROUND(SUM(kwh_fuera_horario), 0) AS kwh_fuera_horario_anual, ROUND(AVG(cop_chiller), 2) AS cop_promedio,
           SUM(flag_anomalia) AS dias_anomalia
    FROM fact_consumo_diario GROUP BY building_id)
SELECT e.building_id, e.nombre, e.categoria, e.area_m2, e.cert_leed, e.chiller_antiguedad_anios, f.kwh_anual,
       ROUND(f.kwh_anual / e.area_m2, 1) AS eui_kwh_m2, f.kwh_ppa, f.kwh_red, f.demanda_pico_kw, f.fp_promedio,
       f.costo_energia_usd, f.costo_demanda_usd, f.penalizacion_fp_usd, f.costo_total_anual_usd, f.emisiones_tco2,
       d.pct_fuera_horario, d.kwh_fuera_horario_anual, d.cop_promedio, d.dias_anomalia
FROM dim_edificio e JOIN fac_agg f ON e.building_id = f.building_id JOIN dia_agg d ON e.building_id = d.building_id
ORDER BY f.kwh_anual DESC;

CREATE VIEW vw_bi_tendencia_mensual AS
WITH fac_m AS (
    SELECT mes, ROUND(SUM(kwh_facturados), 0) AS kwh_mes, ROUND(SUM(kwh_ppa_renovable), 0) AS kwh_ppa,
           ROUND(SUM(kwh_red_sin), 0) AS kwh_red, ROUND(SUM(demanda_maxima_kw), 1) AS suma_demandas_kw,
           ROUND(SUM(total_factura_usd), 2) AS costo_total_usd, ROUND(SUM(cargo_demanda_usd), 2) AS costo_demanda_usd,
           ROUND(SUM(penalizacion_fp_usd), 2) AS penal_fp_usd,
           ROUND(SUM(emisiones_alcance2_kg_co2) / 1000.0, 2) AS emisiones_tco2
    FROM fact_facturacion_mensual GROUP BY mes),
cli_m AS (
    SELECT t.mes, t.mes_nombre, t.temporada_panama AS temporada, ROUND(AVG(d.temp_exterior_c), 1) AS temp_prom_c,
           ROUND(AVG(d.humedad_relativa_pct), 1) AS humedad_prom_pct
    FROM fact_consumo_diario d JOIN dim_tiempo t ON d.date_key = t.date_key GROUP BY t.mes)
SELECT f.mes, c.mes_nombre, c.temporada, f.kwh_mes, f.kwh_ppa, f.kwh_red, f.suma_demandas_kw, f.costo_total_usd,
       f.costo_demanda_usd, f.penal_fp_usd, f.emisiones_tco2, c.temp_prom_c, c.humedad_prom_pct
FROM fac_m f JOIN cli_m c ON f.mes = c.mes ORDER BY f.mes;

CREATE VIEW vw_bi_detalle_mensual AS
WITH d AS (
    SELECT x.building_id, t.mes, ROUND(SUM(x.kwh_total), 2) AS kwh_diario, ROUND(SUM(x.kwh_fuera_horario), 2) AS kwh_fuera_horario,
           SUM(x.flag_anomalia) AS dias_anomalia, ROUND(AVG(x.temp_exterior_c), 2) AS temp_prom_c
    FROM fact_consumo_diario x JOIN dim_tiempo t ON x.date_key = t.date_key GROUP BY x.building_id, t.mes)
SELECT f.building_id, f.mes, f.kwh_facturados AS kwh_mes, f.kwh_ppa_renovable AS kwh_ppa, f.kwh_red_sin AS kwh_red,
       f.demanda_maxima_kw, f.factor_potencia_mensual AS fp_mensual, f.cargo_energia_usd AS costo_energia_usd,
       f.cargo_demanda_usd AS costo_demanda_usd, f.penalizacion_fp_usd, f.total_factura_usd AS costo_total_usd,
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
        conn.executemany("INSERT INTO dim_edificio VALUES (?,?,?,?,?,?,?,?)",
                         [(e["building_id"], e["nombre"], e["categoria"], e["area_m2"], e["anio_rehab"],
                           e["cert_leed"], e["pct_ppa"], e["chiller_antiguedad_anios"]) for e in raw["catalogo"]])
        conn.executemany("INSERT INTO dim_tiempo VALUES (?,?,?,?,?,?,?,?)", tf["tiempo"])
        conn.executemany("""INSERT INTO fact_consumo_diario (date_key, building_id, kwh_total, kwh_horario_laboral,
            kwh_fuera_horario, demanda_max_kw, factor_potencia, ocupacion_pct, temp_exterior_c, humedad_relativa_pct,
            cop_chiller, estado_chiller, costo_energia_est_usd, emisiones_kg_co2, z_score_consumo, flag_anomalia)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", tf["hechos"])
        conn.executemany("""INSERT INTO fact_alertas_automatizadas (fecha, building_id, tipo_alerta, severidad,
            desviacion_pct, accion_recomendada, causa_bitacora) VALUES (?,?,?,?,?,?,?)""", tf["alertas"])
        conn.executemany("INSERT INTO fact_facturacion_mensual VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                         [(f["id_factura"], b, m, f["kwh_facturados"], f["kwh_ppa_renovable"], f["kwh_red_sin"],
                           f["demanda_maxima_kw"], f["factor_potencia_mensual"], f["cargo_energia_usd"],
                           f["cargo_demanda_usd"], f["penalizacion_fp_usd"], f["total_factura_usd"],
                           f["emisiones_alcance2_kg_co2"]) for (b, m), f in sorted(raw["facturas"].items())])
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


def build_gold(db_path=None, raw=None, supuestos=None):
    """KPIs + proyeccion + diagnostico IA + CSV BI. Lee del SQLite (fuente unica de verdad)."""
    sup = supuestos or config.cargar_supuestos()
    raw = raw or extract()
    gold = config.gold_dir()
    gold.mkdir(parents=True, exist_ok=True)
    db_path = Path(db_path or gold / "cds_energia_sig.db")
    conn = sqlite3.connect(db_path)
    try:
        ranking = _query(conn, "SELECT * FROM vw_bi_ranking_edificios")
        mensual = _query(conn, "SELECT * FROM vw_bi_tendencia_mensual")
        detalle = _query(conn, "SELECT * FROM vw_bi_detalle_mensual")
    finally:
        conn.close()
    alertas = leer_alertas(db_path)
    with open(config.silver_dir() / "auditoria_calidad.json", encoding="utf-8") as f:
        audit = json.load(f)
    _csv(gold / "bi_ranking_edificios_2025.csv", ranking)
    _csv(gold / "bi_tendencia_mensual_2025.csv", mensual)

    tot = lambda c: sum(x[c] for x in ranking)  # noqa: E731
    total_kwh, total_costo, total_area = tot("kwh_anual"), tot("costo_total_anual_usd"), tot("area_m2")
    total_dem, total_penal, total_co2 = tot("costo_demanda_usd"), tot("penalizacion_fp_usd"), round(tot("emisiones_tco2"), 2)
    a = lambda c: config.valor(sup, "ahorro", c)  # noqa: E731
    ahorro_bms = round(total_costo * a("bms_cargas_fantasma_pct"), 2)
    ahorro_peak = round(total_dem * a("peak_shaving_demanda_pct"), 2)
    ahorro_fp = round(total_penal * a("eliminacion_penalizacion_fp_pct"), 2)
    ahorro_hvac = round(sum(x["costo_energia_usd"] for x in ranking[:3]) * a("retrofit_hvac_top3_pct"), 2)
    ahorro = round(ahorro_bms + ahorro_peak + ahorro_fp + ahorro_hvac, 2)
    capex = config.valor(sup, "inversion", "capex_usd")
    payback = round(capex / ahorro, 2)

    summary = {
        "organizacion": "Fundación Ciudad del Saber (Clayton, Panamá)",
        "periodo": "Enero - Diciembre 2025",
        "aviso_datos": AVISO_DATOS,
        "auditoria_calidad": audit,
        "kpis_globales": {
            "total_edificios": len(ranking), "area_total_m2": total_area, "consumo_total_kwh": total_kwh,
            "consumo_total_mwh": round(total_kwh / 1000.0, 1), "eui_promedio_kwh_m2": round(total_kwh / total_area, 1),
            "eui_benchmark_leed_platinum": next(x["eui_kwh_m2"] for x in ranking if x["building_id"] == "CDS-DRM"),
            "eui_maximo_indicasat": ranking[0]["eui_kwh_m2"],
            "costo_total_anual_usd": round(total_costo, 2), "costo_demanda_anual_usd": round(total_dem, 2),
            "pct_costo_demanda": round(total_dem * 100.0 / total_costo, 1),
            "penalizacion_fp_anual_usd": round(total_penal, 2), "emisiones_totales_tco2": total_co2,
            "pct_energia_renovable_ppa": round(tot("kwh_ppa") * 100.0 / total_kwh, 1),
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
            "nota": "Proyeccion basada en supuestos.json (ver fuente/justificacion de cada uno); no es un resultado medido.",
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


def run_pipeline(guardar_nosql=True):
    """Ejecuta extract -> clean -> transform -> SQLite -> gold (+ Mongo opcional/fallback)."""
    raw = extract()
    rows, _ = clean(raw)
    tf = transform(raw, rows)
    db = load_sqlite(raw, tf)
    if guardar_nosql:
        import mongo_store
        mongo_store.guardar_alertas_y_telemetria(leer_alertas(db), raw["iot"])
    s = build_gold(db, raw)
    print("=== PIPELINE ETL EJECUTADO ===")
    for k in ("auditoria_calidad", "kpis_globales", "impacto_financiero_proyectado"):
        print(json.dumps(s[k], indent=2, ensure_ascii=False))
    print(f"\nDiagnostico gerencial [{s['diagnostico_gerencial']['etiqueta']}]:\n{s['diagnostico_gerencial']['texto']}")
    print("\nTop 5 Edificios por Consumo Anual:")
    for r in s["ranking_edificios"][:5]:
        print(f"  {r['building_id']} | {r['nombre']:38s} | {r['kwh_anual']:10,.0f} kWh | EUI={r['eui_kwh_m2']:5.1f} | ${r['costo_total_anual_usd']:10,.2f} | FP={r['fp_promedio']}")
    return s


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    run_pipeline()
