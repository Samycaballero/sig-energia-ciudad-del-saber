"""
Generador de graficos (300 DPI) y dashboard HTML interactivo autocontenido.
Lee exclusivamente data/gold/kpi_summary.json (salida de pipeline_etl.py) y usa rutas relativas a este archivo
(o SIG_DATA_DIR / SIG_OUT_DIR). Uso: python generar_dashboard_y_graficos.py
"""

import json
import textwrap
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch
import numpy as np

import config

FIG_DIR = config.out_dir() / "graficos"

# Paleta Corporativa Energía y Sostenibilidad (Ciudad del Saber)
C_DARK = "#0F2C38"      # Azul petróleo oscuro
C_TEAL = "#0F766E"      # Verde petróleo / energía
C_EMERALD = "#10B981"   # Verde renovable / LEED
C_BLUE = "#0284C7"      # Azul AWS / datos
C_AMBER = "#D97706"     # Ámbar alerta / energía solar
C_RED = "#DC2626"       # Rojo crítico / penalización
C_SLATE = "#334155"     # Gris texto
C_MUTED = "#64748B"     # Gris secundario
C_LIGHT = "#F1F5F9"     # Fondo suave
C_BG = "#FFFFFF"

plt.rcParams["font.family"] = "DejaVu Sans"
plt.rcParams["axes.edgecolor"] = "#CBD5E1"
plt.rcParams["axes.linewidth"] = 0.8


def save_fig(fig, name):
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIG_DIR / name, dpi=300, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"  Guardado: {name}")


def fig1_arquitectura(DATA):
    """Arquitectura OBJETIVO del SIG; el subtitulo indica que parte esta implementada en el prototipo."""
    k = DATA["kpis_globales"]
    fin = DATA["impacto_financiero_proyectado"]
    fig, ax = plt.subplots(figsize=(13.5, 6.2), facecolor=C_BG)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 52)
    ax.axis("off")

    # Título interno
    ax.text(50, 49.6, "ARQUITECTURA INTEGRAL DEL SIG DE GESTIÓN ENERGÉTICA · FUNDACIÓN CIUDAD DEL SABER",
            ha="center", va="center", fontsize=12.5, fontweight="bold", color=C_DARK)
    ax.text(50, 47.0, "Arquitectura objetivo. Prototipo implementado: ETL Python + SQLite + MongoDB/S3/LLM opcionales + dashboard HTML (resto = propuesta)",
            ha="center", va="center", fontsize=8.8, color=C_MUTED)

    def draw_col_header(x, w, num, title, color):
        box = FancyBboxPatch((x, 41.2), w, 3.8, boxstyle="round,pad=0.25,rounding_size=0.8",
                             facecolor=color, edgecolor="none")
        ax.add_patch(box)
        ax.text(x + w / 2, 43.1, f"{num} · {title}", ha="center", va="center",
                fontsize=7.3, fontweight="bold", color="white")

    def draw_card(x, y, w, h, title, body, border_c, fill_c="#FFFFFF", title_c=None):
        box = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.25,rounding_size=0.9",
                             facecolor=fill_c, edgecolor=border_c, linewidth=1.4)
        ax.add_patch(box)
        ax.text(x + w / 2, y + h - 1.55, title, ha="center", va="top",
                fontsize=8.0, fontweight="bold", color=title_c or border_c)
        ax.text(x + w / 2, y + h - 3.6, body, ha="center", va="top",
                fontsize=6.9, color=C_SLATE, linespacing=1.3)

    # Columna 1: 4 Fuentes Multi-Formato
    draw_col_header(1.5, 16.5, "01", "FUENTES (4 FORMATOS)", C_DARK)
    draw_card(1.5, 32.2, 16.5, 7.6, "Telemetría AMI (CSV)",
              "Submedidores eléctricos\n12 edificios · kWh, kW,\nfactor de potencia, V", C_BLUE, "#F0F9FF")
    draw_card(1.5, 23.0, 16.5, 7.6, "Sensores IoT / HVAC (JSON)",
              "Temp. exterior, humedad,\nestado chillers, COP,\nMQTT → AWS IoT Core", C_TEAL, "#F0FDFA")
    draw_card(1.5, 13.8, 16.5, 7.6, "Facturación MTD (XML)",
              "Distribuidora (supuesto)\n+ PPA renovable (supuesto)\nTarifa MTD, demanda y CO₂", C_AMBER, "#FFFBEB")
    draw_card(1.5, 4.6, 16.5, 7.6, "Bitácoras HVAC (TXT)",
              "Reportes técnicos en\nlenguaje natural de\nfallas y mantenimiento", "#6D28D9", "#F5F3FF")

    # Columna 2: Ingesta, Calidad y Orquestación AWS
    draw_col_header(21.5, 18.0, "02", "PIPELINE & ORQUEST.", C_BLUE)
    draw_card(21.5, 29.0, 18.0, 10.8, "Amazon S3 (Medallion)",
              "• Capa Bronze (Raw crudo)\n• Capa Silver (Limpio ISO)\n• Capa Gold (KPIs curados)\nRespaldo 3-2-1 y cifrado KMS", C_BLUE, "#F8FAFC")
    draw_card(21.5, 16.8, 18.0, 10.6, "AWS Glue / Python ETL",
              "Auditoría automatizada:\n• Deduplicación y fechas\n• Imputación de nulos\n• Control de outliers", C_TEAL, "#F8FAFC")
    draw_card(21.5, 4.6, 18.0, 10.6, "Apache Airflow / MWAA",
              "+ AWS Step Functions\nOrquestación DAG diario\n02:00 AM · reintentos y\nmonitoreo CloudWatch", C_DARK, "#F8FAFC")

    # Columna 3: Almacenamiento Híbrido
    draw_col_header(43.0, 18.0, "03", "PERSISTENCIA HÍBRIDA", C_TEAL)
    draw_card(43.0, 22.8, 18.0, 17.0, "Amazon RDS (PostgreSQL)",
              "Modelo Estrella SQL (prototipo: SQLite):\n• fact_consumo_diario\n• fact_facturacion_mensual\n• dim_edificio (12 edificios)\n• dim_tiempo (estacional)\n• Vistas BI optimizadas\nMulti-AZ + PITR", C_BLUE, "#EFF6FF")
    draw_card(43.0, 4.6, 18.0, 16.6, "MongoDB Atlas (NoSQL)",
              "Colecciones del prototipo:\n• alertas\n• telemetria_iot (JSON)\nSin servidor: fallback JSON\nFlexible para esquemas\nIoT heterogéneos", C_EMERALD, "#ECFDF5", "#047857")

    # Columna 4: Inteligencia Artificial y Automatización
    draw_col_header(64.5, 16.5, "04", "IA GEN. & AUTOM.", "#6D28D9")
    draw_card(64.5, 22.8, 16.5, 17.0, "IA Generativa y ML",
              f"• Z-Score (implementado)\n  ({k['alertas_anomalias_detectadas']} anomalías detectadas)\n• LLM opcional (Anthropic/\n  Bedrock) + RAG bitácora\n• Sin clave: plantilla\n  (modo offline)", "#6D28D9", "#F5F3FF")
    draw_card(64.5, 4.6, 16.5, 16.6, "Automatización Negocio",
              "Propuesta (no implementada):\n• Órdenes CMMS por alerta\n• Pre-alerta Peak Shaving\n• Reporte periódico\n  de indicadores", C_AMBER, "#FFFBEB", "#B45309")

    # Columna 5: Consumo y Decisiones
    draw_col_header(84.5, 14.0, "05", "DECISIÓN BI", C_EMERALD)
    draw_card(84.5, 22.8, 14.0, 17.0, "Dashboard Power BI",
              "Panel Ejecutivo:\n• Ranking kWh & EUI\n• Estacionalidad\n• Cargas fantasma\n• Factor de potencia\n• Alertas detectadas", C_EMERALD, "#ECFDF5", "#047857")
    draw_card(84.5, 4.6, 14.0, 16.6, "Comité Directivo",
              f"Roles del Equipo:\n• CTO (Líder TI)\n• Ing. Datos / BI\n• Ger. Operaciones\n• Dir. Finanzas/ESG\nAhorro supuesto: -{fin['porcentaje_ahorro_sobre_factura']}%", C_DARK, "#F1F5F9")

    # Flechas conectoras limpias
    arrow_kw = dict(arrowstyle="-|>,head_width=0.35,head_length=0.55", color="#64748B", lw=1.6)
    for y in [36.0, 26.8, 17.6, 8.4]:
        ax.annotate("", xy=(21.3, y), xytext=(18.2, y), arrowprops=arrow_kw)
    for y in [31.5, 13.0]:
        ax.annotate("", xy=(42.8, y), xytext=(39.7, y), arrowprops=arrow_kw)
    for y in [31.5, 13.0]:
        ax.annotate("", xy=(64.3, y), xytext=(61.2, y), arrowprops=arrow_kw)
    for y in [31.5, 13.0]:
        ax.annotate("", xy=(84.3, y), xytext=(81.2, y), arrowprops=arrow_kw)

    # Barra inferior de gobernanza
    gov = FancyBboxPatch((1.5, 0.4), 97.0, 2.8, boxstyle="round,pad=0.15,rounding_size=0.6",
                         facecolor="#0F2C38", edgecolor="none")
    ax.add_patch(gov)
    ax.text(50, 1.8,
            "GOBERNANZA Y CUMPLIMIENTO: AWS IAM (RBAC)  ·  Cifrado TLS 1.3 / KMS  ·  Ley 81 de 2019 (Datos)  ·  Ley 69 de 2012 (UREE)  ·  ISO 50001",
            ha="center", va="center", fontsize=7.0, fontweight="bold", color="#E2E8F0")

    save_fig(fig, "fig1_arquitectura_sig.png")


def fig2_ranking_consumo_eui(DATA):
    """Panel dual: Ranking de Consumo Anual (MWh) y EUI (kWh/m²/año) vs Benchmark LEED Platinum."""
    ranking = DATA["ranking_edificios"]
    names = [r["nombre"].replace("Edificio ", "Edif. ").replace(" - ", "\n") for r in ranking][::-1]
    short_names = [
        r["nombre"].replace("Edificio ", "Ed. ").replace("Ciudad del Saber", "CdS")
        for r in ranking
    ][::-1]
    mwh = [r["kwh_anual"] / 1000.0 for r in ranking][::-1]
    eui = [r["eui_kwh_m2"] for r in ranking][::-1]
    leed = [r["cert_leed"] for r in ranking][::-1]
    costos = [r["costo_total_anual_usd"] for r in ranking][::-1]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.2, 6.2), facecolor=C_BG, sharey=True)
    fig.subplots_adjust(wspace=0.12)

    y_pos = np.arange(len(short_names))

    # Colores por criticidad (Top 4 críticos en rojo/ámbar, LEED en esmeralda, resto en azul petróleo)
    bar_colors_1 = []
    for i, r in enumerate(ranking[::-1]):
        rank_pos = len(ranking) - i
        if rank_pos <= 2:
            bar_colors_1.append(C_RED)
        elif rank_pos <= 4:
            bar_colors_1.append(C_AMBER)
        elif "Platinum" in r["cert_leed"] or "Gold" in r["cert_leed"]:
            bar_colors_1.append(C_EMERALD)
        else:
            bar_colors_1.append(C_TEAL)

    bars1 = ax1.barh(y_pos, mwh, color=bar_colors_1, height=0.68, edgecolor="none")
    ax1.set_yticks(y_pos)
    ax1.set_yticklabels(short_names, fontsize=8.5, color=C_DARK, fontweight="bold")
    ax1.set_xlabel("Consumo Eléctrico Anual (MWh / año)", fontsize=9.2, fontweight="bold", color=C_SLATE)
    ax1.set_title("A. Consumo Eléctrico Total y Costo Anual (2025)", fontsize=10.5, fontweight="bold", color=C_DARK, pad=10)
    ax1.set_xlim(0, max(mwh) * 1.32)
    ax1.grid(axis="x", linestyle="--", alpha=0.4, color="#94A3B8")
    ax1.set_axisbelow(True)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    for b, val, cost in zip(bars1, mwh, costos):
        ax1.text(val + 25, b.get_y() + b.get_height() / 2,
                 f"{val:,.0f} MWh  (${cost/1000:,.0f}k)",
                 va="center", ha="left", fontsize=7.6, color=C_DARK, fontweight="bold")

    # Panel B: EUI (kWh/m²/año)
    bar_colors_2 = [
        C_RED if v >= 280 else (C_AMBER if v >= 230 else (C_EMERALD if v <= 165 else C_BLUE))
        for v in eui
    ]
    bars2 = ax2.barh(y_pos, eui, color=bar_colors_2, height=0.68, edgecolor="none")
    ax2.set_xlabel("Intensidad Energética EUI (kWh / m² / año)", fontsize=9.2, fontweight="bold", color=C_SLATE)
    ax2.set_title("B. Intensidad Energética (EUI) vs. Benchmark LEED Platinum", fontsize=10.5, fontweight="bold", color=C_DARK, pad=10)
    ax2.set_xlim(0, max(eui) * 1.25)
    ax2.grid(axis="x", linestyle="--", alpha=0.4, color="#94A3B8")
    ax2.set_axisbelow(True)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    bench = DATA["kpis_globales"]["eui_benchmark_leed_platinum"]
    prom = DATA["kpis_globales"]["eui_promedio_kwh_m2"]
    ax2.axvline(bench, color=C_EMERALD, linestyle="--", linewidth=1.8, label=f"Benchmark LEED Platinum ({bench} kWh/m²)")
    ax2.axvline(prom, color=C_DARK, linestyle=":", linewidth=1.6, label=f"Promedio Campus ({prom} kWh/m²)")
    ax2.legend(loc="lower right", fontsize=8.0, frameon=True, facecolor="white", edgecolor="#CBD5E1")

    for b, val in zip(bars2, eui):
        ax2.text(val + 5, b.get_y() + b.get_height() / 2,
                 f"{val:.1f}", va="center", ha="left", fontsize=8.0, color=C_DARK, fontweight="bold")

    fig.suptitle(f"Diagnóstico de Consumo por Edificio: Top 4 Concentra el {DATA['kpis_globales']['concentracion_top4_pct']}% de la Demanda del Campus",
                 fontsize=12.2, fontweight="bold", color=C_DARK, y=0.99)
    save_fig(fig, "fig2_ranking_consumo_eui.png")


def fig3_estacionalidad_clima(DATA):
    """Tendencia mensual de consumo (PPA Renovable vs Red SIN) y Temperatura Exterior Panamá."""
    mensual = DATA["tendencia_mensual"]
    meses = [m["mes_nombre"] for m in mensual]
    ppa_mwh = [m["kwh_ppa"] / 1000.0 for m in mensual]
    red_mwh = [m["kwh_red"] / 1000.0 for m in mensual]
    tot_mwh = [m["kwh_mes"] / 1000.0 for m in mensual]
    temps = [m["temp_prom_c"] for m in mensual]

    fig, ax1 = plt.subplots(figsize=(12.0, 5.2), facecolor=C_BG)
    x = np.arange(len(meses))

    # Sombrear temporada seca (Ene-Abr)
    ax1.axvspan(-0.5, 3.5, color="#FEF3C7", alpha=0.45, zorder=0)
    ax1.text(1.5, max(tot_mwh) * 1.13, "TEMPORADA SECA (Mayor carga HVAC)",
             ha="center", va="center", fontsize=8.5, fontweight="bold", color="#B45309")
    ax1.text(7.5, max(tot_mwh) * 1.13, "TEMPORADA LLUVIOSA (Menor radiación térmica)",
             ha="center", va="center", fontsize=8.5, fontweight="bold", color=C_TEAL)

    b1 = ax1.bar(x, ppa_mwh, width=0.58, color=C_EMERALD, label="Energía renovable PPA (proveedor supuesto)", zorder=2)
    b2 = ax1.bar(x, red_mwh, width=0.58, bottom=ppa_mwh, color=C_BLUE, label="Energía red regulada SIN (distribuidora supuesta)", zorder=2)

    for i, tot in enumerate(tot_mwh):
        ax1.text(i, tot + 12, f"{tot:.0f}", ha="center", va="bottom", fontsize=7.8, fontweight="bold", color=C_DARK)

    ax1.set_xticks(x)
    ax1.set_xticklabels(meses, fontsize=9.0, fontweight="bold", color=C_DARK)
    ax1.set_ylabel("Consumo Mensual del Campus (MWh)", fontsize=9.5, fontweight="bold", color=C_DARK)
    ax1.set_ylim(0, max(tot_mwh) * 1.22)
    ax1.grid(axis="y", linestyle="--", alpha=0.35, color="#94A3B8")
    ax1.spines["top"].set_visible(False)

    ax2 = ax1.twinx()
    l1 = ax2.plot(x, temps, color=C_RED, marker="o", linewidth=2.4, markersize=6.5, label="Temperatura Exterior Promedio (°C)", zorder=4)
    for i, t in enumerate(temps):
        ax2.text(i, t + 0.24, f"{t:.1f}°C", ha="center", va="bottom", fontsize=7.4, color=C_RED, fontweight="bold",
                 bbox=dict(boxstyle="round,pad=0.16", facecolor="white", edgecolor="none", alpha=0.82))
    ax2.set_ylabel("Temperatura Exterior Promedio (°C)", fontsize=9.5, fontweight="bold", color=C_RED)
    ax2.set_ylim(26.5, 34.5)
    ax2.spines["top"].set_visible(False)

    # Leyenda unificada
    h1, lb1 = ax1.get_legend_handles_labels()
    h2, lb2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, lb1 + lb2, loc="lower center", bbox_to_anchor=(0.5, -0.22),
               ncol=3, frameon=True, facecolor="white", edgecolor="#CBD5E1", fontsize=8.5)

    ax1.set_title("Estacionalidad Climática en Panamá vs. Consumo Mensual del Campus (2025)",
                  fontsize=12.0, fontweight="bold", color=C_DARK, pad=12)
    save_fig(fig, "fig3_estacionalidad_clima.png")


def fig4_desperdicio_y_fp(DATA):
    """Diagnóstico operativo: % Consumo Fuera de Horario y Factor de Potencia vs umbral supuesto (0.90)."""
    ranking = DATA["ranking_edificios"]
    codes = [r["building_id"].replace("CDS-", "Ed.") for r in ranking]
    off_pct = [r["pct_fuera_horario"] for r in ranking]
    fps = [r["fp_promedio"] for r in ranking]
    penals = [r["penalizacion_fp_usd"] for r in ranking]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.0, 5.2), facecolor=C_BG)
    x = np.arange(len(codes))

    # Panel A: % Fuera de Horario
    c_off = [C_RED if v >= 35 else (C_AMBER if v >= 28 else C_EMERALD) for v in off_pct]
    bars1 = ax1.bar(x, off_pct, color=c_off, width=0.62)
    ax1.axhline(25.0, color=C_DARK, linestyle="--", linewidth=1.4, label="Meta Operativa BMS (≤ 25%)")
    ax1.set_xticks(x)
    ax1.set_xticklabels(codes, rotation=38, ha="right", fontsize=8.2, fontweight="bold", color=C_DARK)
    ax1.set_ylabel("% del Consumo Total Fuera de Horario", fontsize=9.0, fontweight="bold", color=C_SLATE)
    ax1.set_title("A. Proporción de Consumo Fuera de Horario (Cargas Base/Fantasma)", fontsize=10.0, fontweight="bold", color=C_DARK)
    ax1.set_ylim(0, 54)
    ax1.grid(axis="y", linestyle="--", alpha=0.35)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    ax1.legend(loc="upper right", fontsize=8.0)
    for b, v in zip(bars1, off_pct):
        ax1.text(b.get_x() + b.get_width() / 2, v + 0.9, f"{v:.1f}%", ha="center", va="bottom", fontsize=7.4, fontweight="bold", color=C_DARK)

    # Panel B: Factor de Potencia vs umbral supuesto 0.90
    c_fp = [C_RED if v < 0.90 else (C_AMBER if v < 0.92 else C_EMERALD) for v in fps]
    bars2 = ax2.bar(x, fps, color=c_fp, width=0.62)
    ax2.axhline(0.90, color=C_RED, linestyle="--", linewidth=1.8, label="Umbral de penalización supuesto (FP = 0.90)")
    ax2.set_xticks(x)
    ax2.set_xticklabels(codes, rotation=38, ha="right", fontsize=8.2, fontweight="bold", color=C_DARK)
    ax2.set_ylabel("Factor de Potencia Promedio Anual", fontsize=9.0, fontweight="bold", color=C_SLATE)
    ax2.set_title(f"B. Factor de Potencia por Edificio ({sum(1 for p in penals if p >= 100)} con Penalización Relevante)", fontsize=10.0, fontweight="bold", color=C_DARK)
    ax2.set_ylim(0.82, 1.00)
    ax2.grid(axis="y", linestyle="--", alpha=0.35)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    ax2.legend(loc="upper left", fontsize=8.0)  # arriba a la izquierda: sin tapar barras ni etiquetas
    for b, v, p in zip(bars2, fps, penals):
        has_sig_penal = p >= 100
        if has_sig_penal:
            ax2.text(b.get_x() + b.get_width() / 2, v + 0.003, f"{v:.2f}", ha="center", va="bottom",
                     fontsize=7.0, fontweight="bold", color=C_DARK)
            ax2.text(b.get_x() + b.get_width() / 2, 0.828, f"-${p/1000:.1f}k", ha="center", va="bottom",
                     rotation=90, fontsize=7.6, fontweight="bold", color="#FEF08A")
        else:
            ax2.text(b.get_x() + b.get_width() / 2, v + 0.003, f"{v:.2f}", ha="center", va="bottom",
                     fontsize=7.0, fontweight="bold", color=C_DARK)

    fig.suptitle("Ineficiencias Operativas Identificadas por el Pipeline de Datos: Horarios y Calidad de Energía",
                 fontsize=11.8, fontweight="bold", color=C_DARK, y=1.01)
    save_fig(fig, "fig4_desperdicio_y_fp.png")


def fig5_anomalias_y_roi(DATA):
    """Desglose del Ahorro Anual Proyectado (4 Palancas) y Flujo Acumulado de Retorno (ROI / Payback)."""
    fin = DATA["impacto_financiero_proyectado"]
    palancas = [
        "Control BMS y\nCargas Fantasma",
        "Retrofit HVAC/VFD\n(Top 3 Edificios)",
        "Peak Shaving\n(Demanda kW)",
        "Corrección Factor\nde Potencia",
    ]
    montos = [
        fin["ahorro_bms_cargas_fantasma_usd"] / 1000.0,
        fin["ahorro_retrofit_hvac_top3_usd"] / 1000.0,
        fin["ahorro_peak_shaving_demanda_usd"] / 1000.0,
        fin["ahorro_eliminacion_penal_fp_usd"] / 1000.0,
    ]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.0, 5.1), facecolor=C_BG)

    # Panel A: Barras palancas de ahorro
    cols = [C_TEAL, C_BLUE, C_AMBER, C_EMERALD]
    bars = ax1.bar(palancas, montos, color=cols, width=0.56)
    ax1.set_ylabel("Ahorro Anual Proyectado (Miles de USD)", fontsize=9.0, fontweight="bold", color=C_SLATE)
    ax1.set_title(f"A. Desglose del Ahorro Anual: {fin['ahorro_total_anual_usd']:,.0f} USD (-{fin['porcentaje_ahorro_sobre_factura']}%)",
                  fontsize=10.2, fontweight="bold", color=C_DARK)
    ax1.set_ylim(0, max(montos) * 1.25)
    ax1.grid(axis="y", linestyle="--", alpha=0.35)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    ax1.tick_params(axis="x", labelsize=8.3)
    for b, m in zip(bars, montos):
        pct = m * 1000.0 * 100.0 / fin["ahorro_total_anual_usd"]
        ax1.text(b.get_x() + b.get_width() / 2, m + 3.5, f"${m:,.1f}k\n({pct:.1f}%)",
                 ha="center", va="bottom", fontsize=8.3, fontweight="bold", color=C_DARK)

    # Panel B: Flujo acumulado 36 meses (Payback y ROI)
    meses = np.arange(0, 37)
    ahorro_mes = fin["ahorro_total_anual_usd"] / 12.0
    capex = fin["inversion_estimada_usd"]
    flujo_acum = (-capex + meses * ahorro_mes) / 1000.0

    ax2.axhline(0, color=C_DARK, linewidth=1.2)
    ax2.fill_between(meses, flujo_acum, 0, where=(flujo_acum < 0), color=C_RED, alpha=0.16, interpolate=True)
    ax2.fill_between(meses, flujo_acum, 0, where=(flujo_acum >= 0), color=C_EMERALD, alpha=0.22, interpolate=True)
    ax2.plot(meses, flujo_acum, color=C_TEAL, linewidth=2.6)

    pb_m = fin["payback_meses"]
    ax2.scatter([pb_m], [0], color=C_AMBER, s=90, zorder=5, edgecolor=C_DARK, linewidth=1.2)
    ax2.annotate(f"Payback: {pb_m} meses\n(Punto de equilibrio)",
                 xy=(pb_m, 0), xytext=(pb_m + 3.5, -130),
                 arrowprops=dict(arrowstyle="->", color=C_DARK, lw=1.4),
                 fontsize=8.5, fontweight="bold", color=C_DARK,
                 bbox=dict(boxstyle="round,pad=0.35", facecolor="#FFFBEB", edgecolor=C_AMBER))

    ax2.scatter([36], [flujo_acum[-1]], color=C_EMERALD, s=90, zorder=5, edgecolor=C_DARK, linewidth=1.2)
    ax2.annotate(f"Beneficio Neto 3 Años:\n+{flujo_acum[-1]:,.1f}k USD\n(ROI: {fin['roi_3_anios_pct']}%)",
                 xy=(36, flujo_acum[-1]), xytext=(21.5, flujo_acum[-1] - 110),
                 arrowprops=dict(arrowstyle="->", color=C_TEAL, lw=1.4),
                 fontsize=8.4, fontweight="bold", color="#065F46",
                 bbox=dict(boxstyle="round,pad=0.35", facecolor="#ECFDF5", edgecolor=C_EMERALD))

    ax2.set_xlabel("Horizonte de Evaluación (Meses)", fontsize=9.0, fontweight="bold", color=C_SLATE)
    ax2.set_ylabel("Flujo de Caja Acumulado (Miles de USD)", fontsize=9.0, fontweight="bold", color=C_SLATE)
    ax2.set_title(f"B. Retorno de Inversión (CAPEX supuesto {capex/1000:,.0f}k USD · Horizonte 36 Meses)", fontsize=10.2, fontweight="bold", color=C_DARK)
    ax2.set_xlim(0, 38)
    ax2.grid(True, linestyle="--", alpha=0.35)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    fig.suptitle(f"Caso de Negocio (Supuestos del Equipo): Viabilidad Financiera y Mitigación de {fin['co2_evitado_anual_tco2']} tCO₂eq/Año",
                 fontsize=11.8, fontweight="bold", color=C_DARK, y=1.01)
    save_fig(fig, "fig5_anomalias_y_roi.png")


def fig6_dashboard_mockup(DATA):
    """Vista ejecutiva de alta resolución del Dashboard Power BI / Web para incrustar en Word y PPTX."""
    k = DATA["kpis_globales"]
    fin = DATA["impacto_financiero_proyectado"]
    ranking = DATA["ranking_edificios"]
    mensual = DATA["tendencia_mensual"]

    fig = plt.figure(figsize=(13.6, 7.6), facecolor="#0B192C")
    gs = fig.add_gridspec(3, 12, height_ratios=[0.75, 2.2, 2.2], hspace=0.45, wspace=0.45,
                          left=0.04, right=0.97, top=0.89, bottom=0.06)

    # Header bar
    fig.text(0.04, 0.952, "SIG ENERGÍA · FUNDACIÓN CIUDAD DEL SABER  |  PANEL GERENCIAL (réplica estática del dashboard HTML)",
             fontsize=11.6, fontweight="bold", color="#FFFFFF")
    fig.text(0.04, 0.916, "Periodo: Ene–Dic 2025  ·  12 edificios  ·  DATOS SINTÉTICOS  ·  Pipeline Python + SQLite (MongoDB/S3/LLM opcionales)",
             fontsize=8.3, color="#94A3B8")

    # Row 0: 4 KPI Cards
    kpi_cards = [
        ("CONSUMO ANUAL CAMPUS", f"{k['consumo_total_mwh']:,.1f} MWh", f"Top 4 concentra {k['concentracion_top4_pct']}%", "#38BDF8"),
        ("FACTURACIÓN ANUAL MTD+PPA", f"{k['costo_total_anual_usd']/1e6:.2f} M USD", f"Demanda: {k['pct_costo_demanda']}% | Penal. FP: ${k['penalizacion_fp_anual_usd']:,.0f}", "#F59E0B"),
        ("INTENSIDAD ENERGÉTICA (EUI)", f"{k['eui_promedio_kwh_m2']} kWh/m²", f"Benchmark LEED Plat.: {k['eui_benchmark_leed_platinum']} kWh/m²", "#10B981"),
        ("AHORRO PROYECTADO & CO₂", f"{fin['ahorro_total_anual_usd']/1000:.1f}k USD / año", f"Payback: {fin['payback_meses']} m · -{fin['co2_evitado_anual_tco2']} tCO₂", "#A78BFA"),
    ]
    for idx, (lbl, val, sub, acc) in enumerate(kpi_cards):
        ax = fig.add_subplot(gs[0, idx*3:(idx+1)*3])
        ax.set_facecolor("#13263E")
        for sp in ax.spines.values():
            sp.set_color("#1E3A5F")
            sp.set_linewidth(1.2)
        ax.set_xticks([]); ax.set_yticks([])
        ax.add_patch(patches.Rectangle((0, 0), 0.035, 1, transform=ax.transAxes, color=acc))
        ax.text(0.09, 0.78, lbl, transform=ax.transAxes, fontsize=7.5, fontweight="bold", color="#94A3B8")
        ax.text(0.09, 0.42, val, transform=ax.transAxes, fontsize=14.5, fontweight="bold", color="#FFFFFF")
        ax.text(0.09, 0.13, sub, transform=ax.transAxes, fontsize=7.3, color=acc, fontweight="bold")

    # Row 1 Left: Top 6 Edificios
    ax_top = fig.add_subplot(gs[1, 0:7])
    ax_top.set_facecolor("#13263E")
    top6 = ranking[:6][::-1]
    short_map = {
        "CDS-208": "Ed. 208 INDICASAT-AIP",
        "CDS-184": "Ed. 184 Convenciones",
        "CDS-227": "Ed. 227 FSU Panamá",
        "CDS-PLZ": "La Plaza CdS",
        "CDS-205": "Ed. 205 SENACYT",
        "CDS-128": "Ed. 128 Casa ONU",
    }
    lbls = [short_map.get(r["building_id"], r["building_id"]) for r in top6]
    vals = [r["kwh_anual"]/1000.0 for r in top6]
    cols = ["#38BDF8", "#38BDF8", "#F59E0B", "#F59E0B", "#EF4444", "#EF4444"]
    bars = ax_top.barh(lbls, vals, color=cols, height=0.62)
    ax_top.set_title("Top 6 Edificios de Mayor Consumo Eléctrico (MWh / año)", color="#F8FAFC", fontsize=9.8, fontweight="bold", loc="left", pad=8)
    ax_top.tick_params(colors="#CBD5E1", labelsize=8)
    ax_top.set_xlim(0, max(vals) * 1.28)
    ax_top.grid(axis="x", color="#1E3A5F", linestyle="--", alpha=0.6)
    for sp in ["top", "right"]: ax_top.spines[sp].set_visible(False)
    for sp in ["bottom", "left"]: ax_top.spines[sp].set_color("#1E3A5F")
    for b, v, r in zip(bars, vals, top6):
        ax_top.text(v + 25, b.get_y() + b.get_height()/2, f"{v:,.0f} MWh (EUI {r['eui_kwh_m2']})",
                    va="center", fontsize=7.5, color="#F8FAFC", fontweight="bold")

    # Row 1 Right: Composición de Factura (Donut)
    ax_pie = fig.add_subplot(gs[1, 7:12])
    ax_pie.set_facecolor("#13263E")
    energia_usd = sum(r["costo_energia_usd"] for r in ranking)
    demanda_usd = k["costo_demanda_anual_usd"]
    penal_usd = k["penalizacion_fp_anual_usd"]
    sizes = [energia_usd, demanda_usd, penal_usd]
    pie_labels = [
        f"Energía kWh\n({energia_usd*100/k['costo_total_anual_usd']:.1f}%)",
        f"Demanda kW\n({demanda_usd*100/k['costo_total_anual_usd']:.1f}%)",
        f"Penal. FP\n({penal_usd*100/k['costo_total_anual_usd']:.1f}%)",
    ]
    wedges, texts = ax_pie.pie(sizes, labels=pie_labels, colors=["#0EA5E9", "#F59E0B", "#EF4444"],
                               startangle=140, wedgeprops=dict(width=0.42, edgecolor="#13263E"),
                               textprops=dict(color="#E2E8F0", fontsize=8, fontweight="bold"))
    ax_pie.text(0, 0, f"${k['costo_total_anual_usd']/1e6:.2f}M\nTotal", ha="center", va="center",
                color="#FFFFFF", fontsize=10, fontweight="bold")
    ax_pie.set_title("Estructura de Facturación Tarifa MTD + PPA", color="#F8FAFC", fontsize=9.8, fontweight="bold", pad=6)

    # Row 2 Left: Tendencia Mensual
    ax_m = fig.add_subplot(gs[2, 0:7])
    ax_m.set_facecolor("#13263E")
    ms = [m["mes_nombre"] for m in mensual]
    tots = [m["kwh_mes"]/1000.0 for m in mensual]
    ax_m.plot(ms, tots, color="#10B981", marker="o", linewidth=2.3, markersize=5.5, label="Consumo Total (MWh)")
    ax_m.fill_between(range(len(ms)), tots, min(tots)*0.88, color="#10B981", alpha=0.18)
    ax_m.set_title("Evolución Mensual del Consumo Eléctrico (MWh) · Pico en Temporada Seca", color="#F8FAFC", fontsize=9.8, fontweight="bold", loc="left", pad=8)
    ax_m.tick_params(colors="#CBD5E1", labelsize=8)
    ax_m.grid(True, color="#1E3A5F", linestyle="--", alpha=0.6)
    for sp in ["top", "right"]: ax_m.spines[sp].set_visible(False)
    for sp in ["bottom", "left"]: ax_m.spines[sp].set_color("#1E3A5F")
    ax_m.set_ylim(min(tots)*0.88, max(tots)*1.08)

    # Row 2 Right: Panel de Copiloto IA Generativa (Bedrock) + Alertas
    ax_ai = fig.add_subplot(gs[2, 7:12])
    ax_ai.set_facecolor("#172B47")
    for sp in ax_ai.spines.values():
        sp.set_color("#7C3AED")
        sp.set_linewidth(1.4)
    ax_ai.set_xticks([]); ax_ai.set_yticks([])
    diag = DATA["diagnostico_gerencial"]
    ax_ai.text(0.05, 0.88, "[IA] COPILOTO · " + diag["etiqueta"].upper(), transform=ax_ai.transAxes,
               fontsize=7.4, fontweight="bold", color="#C4B5FD")
    lineas = [ln for ln in diag["texto"].splitlines() if ln.strip()][:4]
    insights = "\n".join(textwrap.fill(ln, 58, subsequent_indent="  ", max_lines=3, placeholder="…") for ln in lineas)
    ax_ai.text(0.05, 0.78, insights, transform=ax_ai.transAxes, fontsize=6.4, color="#E2E8F0", va="top", linespacing=1.3)

    save_fig(fig, "fig6_dashboard_mockup.png")


def generar_dashboard_html(DATA):
    """Dashboard autocontenido: inyecta el JSON en dashboard_template.html (sin dependencias externas)."""
    plantilla = (config.BASE_DIR / "dashboard_template.html").read_text(encoding="utf-8")
    datos = json.dumps(DATA, ensure_ascii=False).replace("<", "\\u003c")  # evita cerrar el <script>
    out = config.out_dir() / "dashboard_interactivo.html"
    out.write_text(plantilla.replace("__SUMMARY_JSON__", datos), encoding="utf-8")
    print(f"  Guardado: {out}")


def main():
    with open(config.gold_dir() / "kpi_summary.json", encoding="utf-8") as f:
        DATA = json.load(f)
    print("Generando 6 figuras de alta resolución (300 DPI)...")
    for fn in (fig1_arquitectura, fig2_ranking_consumo_eui, fig3_estacionalidad_clima,
               fig4_desperdicio_y_fp, fig5_anomalias_y_roi, fig6_dashboard_mockup):
        fn(DATA)
    print("Generando Dashboard Interactivo HTML...")
    generar_dashboard_html(DATA)
    return {"figuras": 6, "dashboard": str(config.out_dir() / "dashboard_interactivo.html"),
            "copiloto": DATA["diagnostico_gerencial"]["etiqueta"]}


if __name__ == "__main__":
    main()
