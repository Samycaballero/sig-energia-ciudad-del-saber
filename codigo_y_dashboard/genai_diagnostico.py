"""Diagnostico gerencial con IA generativa (RAG simple sobre la bitacora TXT + KPIs).

Modos (se elige UNO y se etiqueta con el proveedor realmente usado; nunca se simula otro).
Prioridad automatica: anthropic > gemini > ollama > bedrock > offline
  anthropic : ANTHROPIC_API_KEY -> API de Anthropic (ANTHROPIC_MODEL, defecto claude-haiku-5-5)
  gemini    : GEMINI_API_KEY (o GOOGLE_API_KEY) -> Google Gemini API, nivel gratuito (REST generateContent).
              GEMINI_MODEL fija el modelo; si no, se descubre con GET /models (preferencia flash-lite > flash).
  ollama    : OLLAMA_MODEL (p.ej. llama3.2:3b) -> servidor local Ollama (OLLAMA_HOST, defecto http://localhost:11434).
              Solo se activa si OLLAMA_MODEL esta definido (no se sondea la red).
  bedrock   : BEDROCK_MODEL_ID + credenciales AWS -> Amazon Bedrock (API Converse)
  offline   : plantilla determinista, etiquetada "modo offline (plantilla)"
SIG_LLM=anthropic|gemini|ollama|bedrock|offline fuerza un modo (GENAI_MODE se acepta como alias antiguo).
Si el LLM falla (401/403/429/timeout/sin servidor), se cae a plantilla con WARNING en el log y la etiqueta
offline (no silencioso). Las claves nunca se registran ni van en la URL (Gemini: header x-goog-api-key).
"""
import json
import logging
import os
import re
import time
import urllib.error
import urllib.request

log = logging.getLogger("genai")

ETIQUETA_OFFLINE = "modo offline (plantilla)"
MODELO_ANTHROPIC_DEFECTO = "claude-haiku-5-5"
MODOS = ("anthropic", "gemini", "ollama", "bedrock", "offline")
GEMINI_BASE_DEFECTO = "https://generativelanguage.googleapis.com/v1beta"
# Respaldo si GET /models falla o no lista ninguno: estable y con nivel gratuito segun la pagina de precios de Google.
MODELO_GEMINI_DEFECTO = "gemini-3.5-flash-lite"
OLLAMA_HOST_DEFECTO = "http://localhost:11434"
_dormir = time.sleep  # sustituible en tests

SYSTEM_PROMPT = (
    "Eres analista de eficiencia energetica de un campus en Panama. Redacta en espanol un diagnostico "
    "gerencial breve (maximo 5 vinetas que empiezan con '- ') usando SOLO los datos del contexto. "
    "No inventes cifras ni eventos; si la bitacora no explica algo, dilo. Los datos son sinteticos."
)


def _clave_gemini(env):
    return env.get("GEMINI_API_KEY") or env.get("GOOGLE_API_KEY")


def seleccionar_modo(env=None) -> str:
    env = os.environ if env is None else env
    forzado = (env.get("SIG_LLM") or env.get("GENAI_MODE") or "").strip().lower()
    if forzado in MODOS:
        return forzado
    if forzado:
        log.warning("SIG_LLM=%r no es valido (%s); se usa seleccion automatica", forzado, "|".join(MODOS))
    if env.get("ANTHROPIC_API_KEY"):
        return "anthropic"
    if _clave_gemini(env):
        return "gemini"
    if env.get("OLLAMA_MODEL"):
        return "ollama"
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
        f"EUI promedio: {k['eui_promedio_kwh_m2']} kWh/m2; EUI de referencia interna (Complejo de Hospedaje): {k['eui_referencia_hospedaje']}.",
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
    ratio = t["eui_kwh_m2"] / k["eui_referencia_hospedaje"]
    pts = [
        f"- {t['nombre']} ({t['building_id']}) es el mayor consumidor: {t['kwh_anual']/1000:.0f} MWh/anio, "
        f"EUI {t['eui_kwh_m2']} kWh/m2 ({ratio:.1f}x el EUI de referencia interna del Complejo de Hospedaje).",
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


def _enmascarar(texto, env):
    for k in ("ANTHROPIC_API_KEY", "GEMINI_API_KEY", "GOOGLE_API_KEY"):
        if env.get(k):
            texto = texto.replace(env[k], "***")
    return texto


def _timeout(env, var, defecto):
    try:
        return float(env.get(var) or defecto)
    except ValueError:
        return defecto


def _http_json(url, cuerpo, headers, timeout):
    """JSON <-> JSON con urllib (GET si no hay cuerpo). 1 reintento con espera si 429 (Retry-After, tope 30 s)."""
    datos = None if cuerpo is None else json.dumps(cuerpo).encode("utf-8")
    hdr = {"Content-Type": "application/json", **headers}
    for intento in (1, 2):
        req = urllib.request.Request(url, data=datos, headers=hdr)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            detalle = e.read().decode("utf-8", "replace")[:300]
            if e.code == 429 and intento == 1:
                try:
                    espera = min(float(e.headers.get("Retry-After") or 5), 30.0)
                except ValueError:
                    espera = 5.0
                log.warning("HTTP 429 (limite de uso): reintento unico en %.0f s", espera)
                _dormir(espera)
                continue
            raise RuntimeError(f"HTTP {e.code} {e.reason}: {detalle}") from None


def _elegir_modelo_gemini(nombres):
    """De nombres 'models/x' elige el estable mas nuevo: flash-lite primero, luego flash (sin preview/tts/live)."""
    cands = []
    for n in nombres:
        m = re.fullmatch(r"(?:models/)?(gemini-(\d+(?:\.\d+)*)-flash(-lite)?)", n)
        if m:
            cands.append((bool(m.group(3)), tuple(int(x) for x in m.group(2).split(".")), m.group(1)))
    return max(cands)[2] if cands else None


def _llamar_gemini(contexto, env):
    if not _clave_gemini(env):
        raise RuntimeError("falta GEMINI_API_KEY (o GOOGLE_API_KEY)")
    base = (env.get("GEMINI_BASE_URL") or GEMINI_BASE_DEFECTO).rstrip("/")
    hdr = {"x-goog-api-key": _clave_gemini(env)}  # header, no ?key=: la clave no queda en URLs ni logs
    t = _timeout(env, "GEMINI_TIMEOUT", 60)
    modelo = env.get("GEMINI_MODEL")
    if not modelo:
        try:
            lista = _http_json(f"{base}/models?pageSize=200", None, hdr, t)
            modelo = _elegir_modelo_gemini([m["name"] for m in lista.get("models", [])
                                            if "generateContent" in m.get("supportedGenerationMethods", [])])
        except RuntimeError as exc:
            if "HTTP 401" in str(exc) or "HTTP 403" in str(exc):
                raise  # clave invalida: no tiene sentido seguir
        except (OSError, ValueError):
            pass  # sin descubrimiento: se usa el modelo por defecto y la llamada real decide
        modelo = modelo or MODELO_GEMINI_DEFECTO
    r = _http_json(f"{base}/models/{modelo}:generateContent", {
        "systemInstruction": {"parts": [{"text": SYSTEM_PROMPT}]},
        "contents": [{"role": "user", "parts": [{"text": contexto}]}],
        # margen amplio: los modelos con "thinking" gastan parte del limite razonando
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 2048}}, hdr, t)
    cand = (r.get("candidates") or [{}])[0]
    texto = "".join(p.get("text", "") for p in cand.get("content", {}).get("parts", []) if not p.get("thought")).strip()
    if not texto:
        raise RuntimeError(f"respuesta vacia (finishReason={cand.get('finishReason')}, "
                           f"blockReason={r.get('promptFeedback', {}).get('blockReason')})")
    return texto, modelo


def _llamar_ollama(contexto, env):
    host = (env.get("OLLAMA_HOST") or OLLAMA_HOST_DEFECTO).strip().rstrip("/")
    if "://" not in host:
        host = "http://" + host
    modelo = env["OLLAMA_MODEL"]
    r = _http_json(f"{host}/api/chat", {
        "model": modelo, "stream": False,
        "messages": [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": contexto}],
        "options": {"temperature": 0.2, "num_predict": 700}}, {}, _timeout(env, "OLLAMA_TIMEOUT", 180))
    texto = (r.get("message") or {}).get("content", "").strip()
    if not texto:
        raise RuntimeError(f"respuesta vacia (done_reason={r.get('done_reason')})")
    return texto, modelo


ETIQUETAS = {"anthropic": "Anthropic API", "gemini": "Google Gemini", "ollama": "Ollama local",
             "bedrock": "Amazon Bedrock"}
LLAMADAS = {"anthropic": _llamar_anthropic, "gemini": _llamar_gemini, "ollama": _llamar_ollama,
            "bedrock": _llamar_bedrock}


def generar_diagnostico_gerencial(summary, bitacora, env=None):
    """Devuelve {modo, etiqueta, modelo, texto, entradas_bitacora_usadas, error}."""
    env = os.environ if env is None else env
    modo = seleccionar_modo(env)
    contexto, entradas = construir_contexto(summary, bitacora)
    res = {"modo": modo, "etiqueta": ETIQUETA_OFFLINE, "modelo": None, "error": None,
           "entradas_bitacora_usadas": len(entradas)}
    if modo != "offline":
        try:
            texto, modelo = LLAMADAS[modo](contexto, env)
            if not texto.strip():
                raise RuntimeError("respuesta vacia")
            res.update(texto=texto.strip(), modelo=modelo, etiqueta=f"{ETIQUETAS[modo]} ({modelo})")
            log.info("Diagnostico generado con %s", res["etiqueta"])
            return res
        except Exception as exc:  # explicito: se informa y se etiqueta la salida como plantilla
            msg = _enmascarar(str(exc) or type(exc).__name__, env)
            log.warning("FALLO LLM (%s): %s -> se usa plantilla offline", modo, msg)
            res.update(modo="offline", error=f"{modo}: {msg}")
    else:
        log.info("IA generativa en %s (sin proveedor configurado; ver SIG_LLM y .env.example)", ETIQUETA_OFFLINE)
    res["texto"] = _plantilla(summary, entradas)
    return res
