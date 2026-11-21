"""Diagnostico gerencial con IA generativa (RAG simple sobre la bitacora TXT + KPIs).

Modos (se elige UNO y se etiqueta en la salida; nunca se simula otro):
  anthropic : ANTHROPIC_API_KEY definida -> API de Anthropic (modelo ANTHROPIC_MODEL, por defecto claude-haiku-5-5)
  bedrock   : BEDROCK_MODEL_ID definido + credenciales AWS -> Amazon Bedrock (API Converse)
  offline   : sin clave/credenciales -> plantilla determinista, etiquetada "modo offline (plantilla)"
GENAI_MODE=offline|anthropic|bedrock fuerza un modo. Si el LLM falla en ejecucion, se cae a plantilla
con aviso en el log y en la etiqueta (no silencioso).
"""
import logging
import os

log = logging.getLogger("genai")

ETIQUETA_OFFLINE = "modo offline (plantilla)"
MODELO_ANTHROPIC_DEFECTO = "claude-haiku-5-5"

SYSTEM_PROMPT = (
    "Eres analista de eficiencia energetica de un campus en Panama. Redacta en espanol un diagnostico "
    "gerencial breve (maximo 5 vinetas que empiezan con '- ') usando SOLO los datos del contexto. "
    "No inventes cifras ni eventos; si la bitacora no explica algo, dilo. Los datos son sinteticos."
)


def seleccionar_modo(env=None) -> str:
    env = os.environ if env is None else env
    forzado = (env.get("GENAI_MODE") or "").strip().lower()
    if forzado in ("offline", "anthropic", "bedrock"):
        return forzado
    if env.get("ANTHROPIC_API_KEY"):
        return "anthropic"
    if env.get("BEDROCK_MODEL_ID") and _hay_credenciales_aws(env):
        return "bedrock"
    return "offline"


def _hay_credenciales_aws(env) -> bool:
    if env.get("AWS_ACCESS_KEY_ID") and env.get("AWS_SECRET_ACCESS_KEY"):
        return True
    try:
        import boto3
        return boto3.Session().get_credentials() is not None
    except Exception:
        return False


# ---------------- RAG: recuperacion ----------------
def recuperar_entradas(bitacora, building_ids=None, desde=None, hasta=None, max_n=12):
    """Entradas de bitacora filtradas por edificio y solape de fechas (ISO yyyy-mm-dd)."""
    out = []
    for e in bitacora:
        if building_ids and e["building_id"] not in building_ids:
            continue
        if desde and e["fecha_fin"] < desde:
            continue
        if hasta and e["fecha_inicio"] > hasta:
            continue
        out.append(e)
    out.sort(key=lambda e: (e["severidad"] != "ALTA", e["fecha_inicio"]))
    return out[:max_n]


def construir_contexto(summary, bitacora, max_n=12):
    """Texto de contexto: KPIs + top edificios + alertas por edificio + bitacora recuperada."""
    k = summary["kpis_globales"]
    rk = summary["ranking_edificios"]
    alertas = summary.get("alertas", [])
    top = rk[:4]
    por_ed = {}
    for a in alertas:
        por_ed[a["building_id"]] = por_ed.get(a["building_id"], 0) + 1
    ids = [r["building_id"] for r in top] + [b for b in por_ed if b not in {r["building_id"] for r in top}]
    entradas = recuperar_entradas(bitacora, ids, max_n=max_n)
    lineas = [
        f"Periodo: {summary['periodo']}. Edificios: {k['total_edificios']}.",
        f"Consumo total: {k['consumo_total_mwh']} MWh; costo anual: {k['costo_total_anual_usd']} USD; "
        f"EUI promedio: {k['eui_promedio_kwh_m2']} kWh/m2; EUI referencia LEED Platinum: {k['eui_benchmark_leed_platinum']}.",
        f"Los 4 edificios de mayor consumo concentran {k['concentracion_top4_pct']} % del consumo.",
        f"Penalizacion por factor de potencia: {k['penalizacion_fp_anual_usd']} USD; alertas de anomalia: {k['alertas_anomalias_detectadas']}.",
        "Top edificios (id, nombre, MWh, EUI, FP promedio, dias con anomalia):",
    ]
    for r in top:
        lineas.append(f"- {r['building_id']} {r['nombre']}: {r['kwh_anual']/1000:.0f} MWh, EUI {r['eui_kwh_m2']}, "
                      f"FP {r['fp_promedio']}, {r['dias_anomalia']} dias anomalos")
    if por_ed:
        lineas.append("Alertas por edificio: " + ", ".join(f"{b}={n}" for b, n in sorted(por_ed.items())))
    lineas.append("Entradas recuperadas de la bitacora de mantenimiento:")
    for e in entradas:
        lineas.append(f"- [{e['fecha_inicio']} a {e['fecha_fin']}] {e['building_id']} ({e['severidad']}): {e['reporte_tecnico']}")
    return "\n".join(lineas), entradas


# ---------------- generacion ----------------
def _plantilla(summary, entradas):
    k = summary["kpis_globales"]
    rk = summary["ranking_edificios"]
    t = rk[0]
    ratio = t["eui_kwh_m2"] / k["eui_benchmark_leed_platinum"]
    pts = [
        f"- {t['nombre']} ({t['building_id']}) es el mayor consumidor: {t['kwh_anual']/1000:.0f} MWh/anio, "
        f"EUI {t['eui_kwh_m2']} kWh/m2 ({ratio:.1f}x la referencia LEED Platinum del campus).",
        f"- Los 4 edificios de mayor consumo concentran {k['concentracion_top4_pct']} % del consumo total.",
        f"- Se detectaron {k['alertas_anomalias_detectadas']} dias con anomalia de consumo (z-score) y "
        f"{k['penalizacion_fp_anual_usd']:,.0f} USD de penalizacion por factor de potencia.",
    ]
    altas = [e for e in entradas if e["severidad"] == "ALTA"]
    for e in altas[:2]:
        pts.append(f"- Bitacora {e['building_id']} ({e['fecha_inicio']}): {e['reporte_tecnico']}")
    return "\n".join(pts)


def _llamar_anthropic(contexto, env):
    import anthropic
    modelo = env.get("ANTHROPIC_MODEL") or MODELO_ANTHROPIC_DEFECTO
    cliente = anthropic.Anthropic(api_key=env["ANTHROPIC_API_KEY"])
    r = cliente.messages.create(model=modelo, max_tokens=700, system=SYSTEM_PROMPT,
                                messages=[{"role": "user", "content": contexto}])
    return "".join(b.text for b in r.content if getattr(b, "type", "") == "text"), modelo


def _llamar_bedrock(contexto, env):
    import boto3
    modelo = env["BEDROCK_MODEL_ID"]
    region = env.get("AWS_REGION") or env.get("AWS_DEFAULT_REGION") or "us-east-1"
    cli = boto3.client("bedrock-runtime", region_name=region)
    r = cli.converse(modelId=modelo, system=[{"text": SYSTEM_PROMPT}],
                     messages=[{"role": "user", "content": [{"text": contexto}]}],
                     inferenceConfig={"maxTokens": 700})
    return r["output"]["message"]["content"][0]["text"], modelo


def generar_diagnostico_gerencial(summary, bitacora, env=None):
    """Devuelve {modo, etiqueta, modelo, texto, entradas_bitacora_usadas, error}."""
    env = os.environ if env is None else env
    modo = seleccionar_modo(env)
    contexto, entradas = construir_contexto(summary, bitacora)
    res = {"modo": modo, "etiqueta": ETIQUETA_OFFLINE, "modelo": None, "error": None,
           "entradas_bitacora_usadas": len(entradas)}
    if modo != "offline":
        try:
            texto, modelo = (_llamar_anthropic if modo == "anthropic" else _llamar_bedrock)(contexto, env)
            res.update(texto=texto.strip(), modelo=modelo,
                       etiqueta=f"{'Anthropic API' if modo == 'anthropic' else 'Amazon Bedrock'} ({modelo})")
            log.info("Diagnostico generado con %s", res["etiqueta"])
            return res
        except Exception as exc:  # explicito: se informa y se etiqueta la salida como plantilla
            log.warning("FALLO LLM (%s): %s -> se usa plantilla offline", modo, exc)
            res.update(modo="offline", error=f"{modo}: {exc}")
    else:
        log.info("IA generativa en %s (sin ANTHROPIC_API_KEY ni Bedrock configurado)", ETIQUETA_OFFLINE)
    res["texto"] = _plantilla(summary, entradas)
    return res
