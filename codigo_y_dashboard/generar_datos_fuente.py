"""
Generador de Fuentes de Datos Multi-Formato (Documentadas)
Organización: Fundación Ciudad del Saber (Clayton, Ciudad de Panamá)
Periodo: Enero - Diciembre 2025 (365 días, 12 edificios; DATOS SINTETICOS con fines academicos)

Genera 4 fuentes en formatos heterogéneos:
1. CSV  -> data/raw/telemetria_medidores_ami_2025.csv (estructurado tabular, lecturas diarias/horarias resumidas)
2. JSON -> data/raw/sensores_iot_hvac_clima_2025.json (semiestructurado jerárquico para MongoDB)
3. XML  -> data/raw/facturacion_mtd_distribuidora_2025.xml (facturación mensual con tarifa de media tensión + PPA; nombres de distribuidora y proveedor son supuestos)
4. TXT  -> data/raw/bitacora_mantenimiento_hvac_2025.txt (texto no estructurado para análisis con IA Generativa)

Incluye anomalías de calidad controladas (nulos, duplicados, outliers y formatos de fecha mixtos)
para evidenciar el proceso real de limpieza y validación en el pipeline ETL.
"""

import csv
import json
import math
import os
import random
import xml.etree.ElementTree as ET
from datetime import date, timedelta

random.seed(202511)

import config

RAW_DIR = str(config.raw_dir())
os.makedirs(RAW_DIR, exist_ok=True)

# 12 edificios del catalogo (nombres basados en la Fundación Ciudad del Saber; consumos SINTETICOS)
EDIFICIOS = [
    {
        "building_id": "CDS-208",
        "nombre": "Edificio 208 - INDICASAT-AIP",
        "categoria": "Laboratorios I+D",
        "area_m2": 4850,
        "anio_rehab": 2008,
        "cert_leed": "Sin certificación",
        "base_kwh_dia": 4650,
        "pct_fuera_horario": 0.44,  # Operación 24/7 por ultracongeladores y cuartos fríos
        "fp_base": 0.87,            # Factor de potencia bajo por cargas inductivas de laboratorio
        "pct_ppa": 0.45,
        "sensibilidad_clima": 0.16,
        "chiller_antiguedad_anios": 14,
    },
    {
        "building_id": "CDS-184",
        "nombre": "Edificio 184 - Centro de Convenciones",
        "categoria": "Eventos y Convenciones",
        "area_m2": 5200,
        "anio_rehab": 2006,
        "cert_leed": "Sin certificación",
        "base_kwh_dia": 3420,
        "pct_fuera_horario": 0.31,
        "fp_base": 0.88,
        "pct_ppa": 0.55,
        "sensibilidad_clima": 0.24,
        "chiller_antiguedad_anios": 15,
    },
    {
        "building_id": "CDS-227",
        "nombre": "Edificio 227 - FSU Panamá",
        "categoria": "Académico Histórico",
        "area_m2": 4400,
        "anio_rehab": 2004,
        "cert_leed": "Sin certificación",
        "base_kwh_dia": 2890,
        "pct_fuera_horario": 0.34,  # HVAC antiguo operando en tardes/noches
        "fp_base": 0.89,
        "pct_ppa": 0.50,
        "sensibilidad_clima": 0.22,
        "chiller_antiguedad_anios": 16,
    },
    {
        "building_id": "CDS-205",
        "nombre": "Edificio 205 - Sede SENACYT",
        "categoria": "Oficinas Institucionales",
        "area_m2": 3950,
        "anio_rehab": 2010,
        "cert_leed": "Sin certificación",
        "base_kwh_dia": 2260,
        "pct_fuera_horario": 0.29,
        "fp_base": 0.91,
        "pct_ppa": 0.60,
        "sensibilidad_clima": 0.19,
        "chiller_antiguedad_anios": 11,
    },
    {
        "building_id": "CDS-128",
        "nombre": "Edificio 128 - Casa de la ONU",
        "categoria": "Organismos Internacionales",
        "area_m2": 4600,
        "anio_rehab": 2014,
        "cert_leed": "LEED Gold",
        "base_kwh_dia": 2050,
        "pct_fuera_horario": 0.22,
        "fp_base": 0.94,
        "pct_ppa": 0.75,
        "sensibilidad_clima": 0.14,
        "chiller_antiguedad_anios": 8,
    },
    {
        "building_id": "CDS-PLZ",
        "nombre": "La Plaza Ciudad del Saber",
        "categoria": "Comercial y Gastronomía",
        "area_m2": 3100,
        "anio_rehab": 2012,
        "cert_leed": "Sin certificación",
        "base_kwh_dia": 2380,
        "pct_fuera_horario": 0.38,  # Refrigeración comercial y cenas
        "fp_base": 0.88,
        "pct_ppa": 0.50,
        "sensibilidad_clima": 0.18,
        "chiller_antiguedad_anios": 12,
    },
    {
        "building_id": "CDS-104",
        "nombre": "Edificio 104 - Sede Fundación CdS",
        "categoria": "Oficinas Administrativas",
        "area_m2": 3400,
        "anio_rehab": 2011,
        "cert_leed": "LEED Silver",
        "base_kwh_dia": 1680,
        "pct_fuera_horario": 0.26,
        "fp_base": 0.93,
        "pct_ppa": 0.75,
        "sensibilidad_clima": 0.15,
        "chiller_antiguedad_anios": 9,
    },
    {
        "building_id": "CDS-234",
        "nombre": "Edificio 234 - Centro de Innovación",
        "categoria": "Tecnología e Innovación",
        "area_m2": 2950,
        "anio_rehab": 2015,
        "cert_leed": "LEED Silver",
        "base_kwh_dia": 1720,
        "pct_fuera_horario": 0.33,  # Servidores locales y coworking extendido
        "fp_base": 0.92,
        "pct_ppa": 0.70,
        "sensibilidad_clima": 0.16,
        "chiller_antiguedad_anios": 7,
    },
    {
        "building_id": "CDS-109",
        "nombre": "Edificio 109 - Innova 109",
        "categoria": "Tecnología e Innovación",
        "area_m2": 2200,
        "anio_rehab": 2016,
        "cert_leed": "LEED Silver",
        "base_kwh_dia": 1290,
        "pct_fuera_horario": 0.28,
        "fp_base": 0.93,
        "pct_ppa": 0.70,
        "sensibilidad_clima": 0.15,
        "chiller_antiguedad_anios": 6,
    },
    {
        "building_id": "CDS-ATN",
        "nombre": "El Ateneo - Auditorio Cultural",
        "categoria": "Eventos y Cultura",
        "area_m2": 1950,
        "anio_rehab": 2013,
        "cert_leed": "Sin certificación",
        "base_kwh_dia": 1140,
        "pct_fuera_horario": 0.35,  # Funciones nocturnas y climatización previa
        "fp_base": 0.90,
        "pct_ppa": 0.60,
        "sensibilidad_clima": 0.21,
        "chiller_antiguedad_anios": 11,
    },
    {
        "building_id": "CDS-DRM",
        "nombre": "Dormitorios Ciudad del Saber",
        "categoria": "Hospedaje Sostenible",
        "area_m2": 3600,
        "anio_rehab": 2015,
        "cert_leed": "LEED Platinum",
        "base_kwh_dia": 1080,  # Benchmark bioclimático + solar térmico
        "pct_fuera_horario": 0.36,
        "fp_base": 0.96,
        "pct_ppa": 0.85,
        "sensibilidad_clima": 0.09,
        "chiller_antiguedad_anios": 5,
    },
    {
        "building_id": "CDS-173",
        "nombre": "Edificio 173 - Casa Museo",
        "categoria": "Patrimonio y Cultura",
        "area_m2": 1150,
        "anio_rehab": 2017,
        "cert_leed": "Rehabilitación Eco",
        "base_kwh_dia": 460,
        "pct_fuera_horario": 0.18,
        "fp_base": 0.95,
        "pct_ppa": 0.80,
        "sensibilidad_clima": 0.12,
        "chiller_antiguedad_anios": 6,
    },
]

# Estacionalidad climática de Panamá (temporada seca: ene-abr más calurosa -> mayor carga HVAC)
TEMP_MES_PANAMA = {
    1: 30.8, 2: 31.6, 3: 32.4, 4: 32.1,
    5: 30.2, 6: 29.4, 7: 29.5, 8: 29.3,
    9: 29.0, 10: 28.6, 11: 28.9, 12: 29.9,
}
HUMEDAD_MES_PANAMA = {
    1: 72, 2: 68, 3: 66, 4: 73,
    5: 83, 6: 86, 7: 85, 8: 86,
    9: 87, 10: 88, 11: 86, 12: 79,
}

# Tarifas ILUSTRATIVAS (supuesto del equipo, no verificado; ver supuestos.json). Moneda: USD
TARIFA_RED_KWH = 0.178        # USD por kWh de la red (supuesto)
TARIFA_PPA_KWH = 0.128        # USD por kWh de un PPA renovable hipotético (supuesto)
TARIFA_DEMANDA_KW = 14.85     # USD por kW-mes de demanda máxima (supuesto)
FACTOR_EMISION_SIN = 0.165    # kg CO2eq/kWh (ilustrativo, supuesto del equipo)
FACTOR_EMISION_PPA = 0.018    # kg CO2eq/kWh (ciclo de vida generación renovable)

# Anomalías operativas reales simuladas (para que el pipeline y la IA las detecten)
ANOMALIAS_OPERATIVAS = [
    ("CDS-208", date(2025, 3, 12), date(2025, 3, 21), 1.32, "Fuga de refrigerante en Chiller #2 y falla de válvula de bypass en laboratorios de biología molecular."),
    ("CDS-184", date(2025, 4, 8),  date(2025, 4, 14), 1.48, "Congreso Internacional de Energía + climatización encendida 24h sin programación BMS."),
    ("CDS-227", date(2025, 8, 18), date(2025, 8, 29), 1.36, "Falla en sensor de retorno de manejadora AHU-03; enfriamiento continuo durante fines de semana."),
    ("CDS-PLZ", date(2025, 10, 4), date(2025, 10, 13), 1.29, "Compresor de cuarto frío en área gastronómica operando en ciclo continuo por termostato dañado."),
    ("CDS-205", date(2025, 6, 10), date(2025, 6, 17), 1.27, "Desconfiguración del temporizador de iluminación y chillers tras mantenimiento eléctrico programado."),
]


def generar_datos():
    start = date(2025, 1, 1)
    days = [start + timedelta(days=i) for i in range(365)]

    ami_rows = []
    iot_docs = []
    monthly_agg = {}  # (building_id, month) -> stats
    maintenance_logs = []

    defect_counts = {"nulos": 0, "duplicados": 0, "outliers_negativos": 0, "fechas_mixtas": 0}

    for ed in EDIFICIOS:
        bid = ed["building_id"]
        for d in days:
            m = d.month
            is_weekend = d.weekday() >= 5
            temp_ext = round(TEMP_MES_PANAMA[m] + random.gauss(0, 0.9), 1)
            humedad = round(min(97, max(55, HUMEDAD_MES_PANAMA[m] + random.gauss(0, 3.2))), 1)

            # Multiplicador climático respecto a 29.0 °C
            clima_mult = 1.0 + ed["sensibilidad_clima"] * ((temp_ext - 29.0) / 3.0)

            # Multiplicador fin de semana
            if ed["categoria"] in ("Laboratorios I+D", "Hospedaje Sostenible", "Comercial y Gastronomía"):
                wknd_mult = 0.88 if is_weekend else 1.03
            elif ed["categoria"] in ("Eventos y Convenciones", "Eventos y Cultura"):
                wknd_mult = 1.12 if is_weekend else 0.95
            else:
                wknd_mult = 0.46 if is_weekend else 1.15

            # Revisar anomalías operativas
            anom_mult = 1.0
            anom_flag = False
            for abid, d_ini, d_fin, mult, _desc in ANOMALIAS_OPERATIVAS:
                if abid == bid and d_ini <= d <= d_fin:
                    anom_mult = mult
                    anom_flag = True

            noise = random.gauss(1.0, 0.045)
            kwh_total = round(ed["base_kwh_dia"] * clima_mult * wknd_mult * anom_mult * noise, 2)

            pct_off = min(0.65, max(0.10, ed["pct_fuera_horario"] + (0.08 if anom_flag else 0.0) + random.gauss(0, 0.02)))
            kwh_off = round(kwh_total * pct_off, 2)
            kwh_lab = round(kwh_total - kwh_off, 2)

            # Demanda pico (kW) en ventana de 15 min
            load_factor = 0.52 if ed["categoria"] in ("Eventos y Convenciones", "Eventos y Cultura") else (
                0.76 if ed["categoria"] == "Laboratorios I+D" else 0.64
            )
            demanda_kw = round((kwh_total / 24.0) / load_factor * random.uniform(0.95, 1.08), 2)
            fp = round(min(0.99, max(0.79, ed["fp_base"] - (0.03 if anom_flag else 0.0) + random.gauss(0, 0.012))), 3)
            voltaje = round(random.gauss(208.0, 1.8), 1)
            ocupacion = round(min(100, max(5, (22 if is_weekend else 78) + random.gauss(0, 9))), 1)

            # Acumular mensual para facturación XML (sobre datos verdaderos)
            key = (bid, m)
            if key not in monthly_agg:
                monthly_agg[key] = {"kwh": 0.0, "max_kw": 0.0, "fp_sum": 0.0, "n": 0}
            monthly_agg[key]["kwh"] += kwh_total
            monthly_agg[key]["max_kw"] = max(monthly_agg[key]["max_kw"], demanda_kw)
            monthly_agg[key]["fp_sum"] += fp
            monthly_agg[key]["n"] += 1

            # --- Inyección documentada de defectos de calidad en CSV crudo ---
            fecha_str = d.isoformat()
            if random.random() < 0.025:
                fecha_str = d.strftime("%d/%m/%Y")  # formato de fecha inconsistente
                defect_counts["fechas_mixtas"] += 1

            kwh_csv = kwh_total
            fp_csv = fp
            r_def = random.random()
            if r_def < 0.012:
                kwh_csv = ""  # valor nulo por pérdida de paquete AMI
                defect_counts["nulos"] += 1
            elif r_def < 0.018:
                kwh_csv = round(-abs(kwh_total), 2)  # lectura corrupta negativa
                defect_counts["outliers_negativos"] += 1
            elif r_def < 0.022:
                kwh_csv = round(kwh_total * 9.5, 2)  # pico espurio de reset de medidor
                defect_counts["outliers_negativos"] += 1

            row = {
                "fecha_lectura": fecha_str,
                "building_id": bid,
                "medidor_ami_id": f"AMI-{bid}-M01",
                "kwh_total": kwh_csv,
                "kwh_horario_laboral": kwh_lab if kwh_csv != "" else "",
                "kwh_fuera_horario": kwh_off if kwh_csv != "" else "",
                "demanda_max_kw": demanda_kw,
                "factor_potencia": fp_csv,
                "voltaje_promedio_v": voltaje,
                "ocupacion_pct": ocupacion,
            }
            ami_rows.append(row)

            # Inyectar duplicados (~1.1%)
            if random.random() < 0.011:
                ami_rows.append(dict(row))
                defect_counts["duplicados"] += 1

            # --- Documento JSON IoT/HVAC (MongoDB Time-Series / Semiestructurado) ---
            setpoint = 21.5 if ed["cert_leed"] == "Sin certificación" else 23.0
            if anom_flag:
                setpoint = 19.0
            iot_docs.append({
                "timestamp": f"{d.isoformat()}T14:00:00-05:00",
                "building_id": bid,
                "zona_campus": "Sector Clayton - CdS",
                "ambiente": {
                    "temp_exterior_c": temp_ext,
                    "humedad_relativa_pct": humedad,
                    "indice_calor_c": round(temp_ext + 0.09 * (humedad - 65), 1),
                },
                "hvac": {
                    "estado_chiller": "ALERTA_SOBRECARGA" if anom_flag else "OPERACION_NORMAL",
                    "setpoint_c": setpoint,
                    "horas_operacion_dia": 24 if (ed["categoria"] == "Laboratorios I+D" or anom_flag) else (10 if is_weekend else 13),
                    "cop_eficiencia": round(max(2.1, 4.4 - 0.11 * ed["chiller_antiguedad_anios"] - (0.6 if anom_flag else 0)), 2),
                },
                "iluminacion": {
                    "tecnologia": "LED Inteligente" if "LEED" in ed["cert_leed"] else "Mixta Fluorescente/LED",
                    "sensores_presencia_activos": "LEED" in ed["cert_leed"],
                },
                "metadata_ingesta": {"protocolo": "MQTT/AWS-IoT-Core", "version_esquema": "2.1"},
            })

    # 1. Guardar CSV AMI
    csv_path = os.path.join(RAW_DIR, "telemetria_medidores_ami_2025.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(ami_rows[0].keys()))
        writer.writeheader()
        writer.writerows(ami_rows)

    # 2. Guardar JSON IoT/HVAC (MongoDB)
    json_path = os.path.join(RAW_DIR, "sensores_iot_hvac_clima_2025.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(iot_docs, f, ensure_ascii=False, indent=2)

    # 3. Guardar XML Facturación Mensual (tarifa de media tensión + PPA; nombres supuestos)
    root_xml = ET.Element("FacturacionElectricaCampus", {
        "organizacion": "Fundacion Ciudad del Saber",
        "distribuidora": "distribuidora (supuesto)",
        "tarifa_regulada": "MTD",
        "contrato_renovable": "proveedor de energía renovable (supuesto)",
        "anio": "2025",
        "moneda": "USD",
    })
    ed_map = {e["building_id"]: e for e in EDIFICIOS}
    for (bid, m), st in sorted(monthly_agg.items()):
        ed = ed_map[bid]
        kwh_m = round(st["kwh"], 2)
        max_kw_m = round(st["max_kw"], 2)
        fp_m = round(st["fp_sum"] / st["n"], 3)
        pct_ppa = ed["pct_ppa"]
        kwh_ppa = round(kwh_m * pct_ppa, 2)
        kwh_red = round(kwh_m - kwh_ppa, 2)
        costo_energia = round(kwh_ppa * TARIFA_PPA_KWH + kwh_red * TARIFA_RED_KWH, 2)
        costo_demanda = round(max_kw_m * TARIFA_DEMANDA_KW, 2)
        # Penalización (umbral supuesto) cuando FP < 0.90 (0.5% por cada 0.01 debajo de 0.90 sobre energía + demanda)
        penal_fp = round((0.90 - fp_m) * 50.0 * 0.01 * (costo_energia + costo_demanda), 2) if fp_m < 0.90 else 0.0
        costo_total = round(costo_energia + costo_demanda + penal_fp, 2)
        emisiones = round(kwh_red * FACTOR_EMISION_SIN + kwh_ppa * FACTOR_EMISION_PPA, 2)

        fac = ET.SubElement(root_xml, "FacturaMensual", {
            "id_factura": f"FAC-2025-{m:02d}-{bid}",
            "building_id": bid,
            "mes": str(m),
        })
        for tag, val in [
            ("kwh_facturados", f"{kwh_m:.2f}"),
            ("kwh_ppa_renovable", f"{kwh_ppa:.2f}"),
            ("kwh_red_sin", f"{kwh_red:.2f}"),
            ("demanda_maxima_kw", f"{max_kw_m:.2f}"),
            ("factor_potencia_mensual", f"{fp_m:.3f}"),
            ("cargo_energia_usd", f"{costo_energia:.2f}"),
            ("cargo_demanda_usd", f"{costo_demanda:.2f}"),
            ("penalizacion_fp_usd", f"{penal_fp:.2f}"),
            ("total_factura_usd", f"{costo_total:.2f}"),
            ("emisiones_alcance2_kg_co2", f"{emisiones:.2f}"),
        ]:
            el = ET.SubElement(fac, tag)
            el.text = val

    xml_path = os.path.join(RAW_DIR, "facturacion_mtd_distribuidora_2025.xml")
    ET.indent(root_xml, space="  ")
    ET.ElementTree(root_xml).write(xml_path, encoding="utf-8", xml_declaration=True)

    # 4. Guardar TXT Bitácora de Mantenimiento (No estructurado para IA Generativa)
    txt_lines = [
        "=== BITÁCORA DE MANTENIMIENTO ELÉCTRICO Y HVAC - FUNDACIÓN CIUDAD DEL SABER (2025) ===",
        "",
    ]
    for abid, d_ini, d_fin, mult, desc in ANOMALIAS_OPERATIVAS:
        txt_lines.append(
            f"[{d_ini.isoformat()} a {d_fin.isoformat()}] EDIFICIO={abid} | SEVERIDAD=ALTA | "
            f"DESVIACION_ESTIMADA=+{int((mult-1)*100)}% | REPORTE_TECNICO: {desc}"
        )
    txt_lines.extend([
        "[2025-05-03] EDIFICIO=CDS-DRM | SEVERIDAD=BAJA | REPORTE_TECNICO: Limpieza preventiva de paneles solares térmicos en Dormitorios LEED Platinum; eficiencia óptima 98%.",
        "[2025-07-19] EDIFICIO=CDS-128 | SEVERIDAD=BAJA | REPORTE_TECNICO: Calibración semestral del sistema BMS en Casa de la ONU; ahorro sostenido en horario nocturno.",
        "[2025-09-11] EDIFICIO=CDS-208 | SEVERIDAD=MEDIA | REPORTE_TECNICO: Banco de capacitores en subestación de INDICASAT-AIP presenta degradación en 2 celdas; factor de potencia debajo de 0.90.",
        "[2025-11-05] EDIFICIO=CDS-184 | SEVERIDAD=MEDIA | REPORTE_TECNICO: Se recomienda instalar variadores de frecuencia (VFD) en bombas secundarias del Centro de Convenciones para mitigar picos de demanda.",
    ])
    txt_path = os.path.join(RAW_DIR, "bitacora_mantenimiento_hvac_2025.txt")
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(txt_lines))

    # Guardar catálogo de edificios
    meta_path = os.path.join(RAW_DIR, "catalogo_edificios_cds.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(EDIFICIOS, f, ensure_ascii=False, indent=2)

    print("Generación completada exitosamente:")
    print(f"  CSV AMI rows (raw):  {len(ami_rows)}")
    print(f"  JSON IoT docs:       {len(iot_docs)}")
    print(f"  XML Facturas:        {len(monthly_agg)}")
    print(f"  TXT Bitácora líneas: {len(txt_lines)}")
    print(f"  Defectos inyectados: {defect_counts}")


if __name__ == "__main__":
    generar_datos()
