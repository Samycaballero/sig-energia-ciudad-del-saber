"""Proveedores gratuitos (Gemini, Ollama) contra SERVIDORES HTTP FALSOS en 127.0.0.1.
Validan cableado, formato de peticion, parseo, fallback y no-fuga de la clave; NO prueban los servicios reales."""
import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

import genai_diagnostico as g

ROOT = Path(__file__).resolve().parent.parent
SUMMARY = json.loads((ROOT / "data" / "gold" / "kpi_summary.json").read_text(encoding="utf-8"))
BIT = SUMMARY["bitacora_eventos"]
CLAVE = "AIzaSyFAKE-clave-secreta-123"
OFFLINE = "modo offline (plantilla)"

MODELOS = {"models": [
    {"name": "models/gemini-2.5-flash", "supportedGenerationMethods": ["generateContent"]},
    {"name": "models/gemini-3.5-flash-lite", "supportedGenerationMethods": ["generateContent"]},
    {"name": "models/gemini-3.1-flash-lite", "supportedGenerationMethods": ["generateContent"]},
    {"name": "models/gemini-3.8-flash", "supportedGenerationMethods": ["generateContent"]},
    {"name": "models/gemini-3.9-flash-lite-preview", "supportedGenerationMethods": ["generateContent"]},
    {"name": "models/gemini-3.8-flash-tts", "supportedGenerationMethods": ["generateContent"]},
    {"name": "models/gemini-9.0-flash-lite", "supportedGenerationMethods": ["embedContent"]},
    {"name": "models/gemini-2.5-pro", "supportedGenerationMethods": ["generateContent"]},
]}
OK_GEMINI = {"candidates": [{"content": {"parts": [{"text": "razonando", "thought": True},
                                                    {"text": "- texto de Gemini falso"}]}, "finishReason": "STOP"}]}
OK_OLLAMA = {"model": "llama3.2:3b", "message": {"role": "assistant", "content": "- texto de Ollama falso"}, "done": True}


@pytest.fixture
def srv():
    """Servidor falso. srv.resp[(metodo, prefijo_ruta)] = lista de (status, cuerpo[, demora]); se consume en orden
    (la ultima se repite). srv.reqs registra (metodo, ruta, headers, cuerpo)."""
    class H(BaseHTTPRequestHandler):
        def _atender(self, metodo):
            n = int(self.headers.get("Content-Length") or 0)
            cuerpo = json.loads(self.rfile.read(n)) if n else None
            s.reqs.append((metodo, self.path, dict(self.headers), cuerpo))
            for (m, pref), cola in s.resp.items():
                if m == metodo and self.path.startswith(pref):
                    item = cola.pop(0) if len(cola) > 1 else cola[0]
                    status, body, *demora = item
                    if demora:
                        time.sleep(demora[0])
                    raw = json.dumps(body).encode()
                    try:
                        self.send_response(status)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(raw)))
                        self.end_headers()
                        self.wfile.write(raw)
                    except OSError:
                        pass
                    return
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self):
            self._atender("GET")

        def do_POST(self):
            self._atender("POST")

        def log_message(self, *a):
            pass

    class S:
        resp, reqs = {}, []
    s = S()
    s.resp, s.reqs = {}, []
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
    httpd.daemon_threads = True
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    s.url = f"http://127.0.0.1:{httpd.server_port}"
    yield s
    httpd.shutdown()
    httpd.server_close()


@pytest.fixture(autouse=True)
def _sin_espera(monkeypatch):
    esperas = []
    monkeypatch.setattr(g, "_dormir", esperas.append)
    return esperas


def _gem(srv, **extra):
    return {"GEMINI_API_KEY": CLAVE, "GEMINI_BASE_URL": srv.url + "/v1beta", "GEMINI_TIMEOUT": "5", **extra}


def _gen(env):
    return g.generar_diagnostico_gerencial(SUMMARY, BIT, env=env)


# ---------------- seleccion ----------------
def test_prioridad_y_modos_forzados():
    sel = g.seleccionar_modo
    assert sel({"GEMINI_API_KEY": "k"}) == "gemini" and sel({"GOOGLE_API_KEY": "k"}) == "gemini"
    assert sel({"OLLAMA_MODEL": "llama3.2:3b"}) == "ollama"
    assert sel({"OLLAMA_HOST": "http://x:1"}) == "offline"  # sin OLLAMA_MODEL no se activa ni se sondea
    todo = {"ANTHROPIC_API_KEY": "a", "GEMINI_API_KEY": "g", "OLLAMA_MODEL": "m",
            "BEDROCK_MODEL_ID": "b", "AWS_ACCESS_KEY_ID": "x", "AWS_SECRET_ACCESS_KEY": "y"}
    assert sel(todo) == "anthropic"
    assert sel({k: v for k, v in todo.items() if k != "ANTHROPIC_API_KEY"}) == "gemini"
    assert sel({k: v for k, v in todo.items() if k not in ("ANTHROPIC_API_KEY", "GEMINI_API_KEY")}) == "ollama"
    for forzado in ("gemini", "ollama", "anthropic", "bedrock", "offline"):
        assert sel({**todo, "SIG_LLM": forzado.upper()}) == forzado
    assert sel({**todo, "SIG_LLM": "basura"}) == "anthropic"


def test_elegir_modelo_prefiere_flash_lite_estable_mas_nuevo():
    nombres = [m["name"] for m in MODELOS["models"] if "generateContent" in m["supportedGenerationMethods"]]
    assert g._elegir_modelo_gemini(nombres) == "gemini-3.5-flash-lite"
    assert g._elegir_modelo_gemini(["models/gemini-2.5-flash", "models/gemini-3.8-flash"]) == "gemini-3.8-flash"
    assert g._elegir_modelo_gemini(["models/gemini-2.5-pro", "models/gemma-3"]) is None


# ---------------- Gemini ----------------
def test_gemini_descubre_modelo_parsea_y_no_pone_la_clave_en_la_url(srv):
    srv.resp[("GET", "/v1beta/models")] = [(200, MODELOS)]
    srv.resp[("POST", "/v1beta/models/")] = [(200, OK_GEMINI)]
    r = _gen(_gem(srv))
    assert r["modo"] == "gemini" and r["etiqueta"] == "Google Gemini (gemini-3.5-flash-lite)"
    assert r["modelo"] == "gemini-3.5-flash-lite" and r["texto"] == "- texto de Gemini falso" and r["error"] is None
    lista, gen = srv.reqs
    assert gen[1] == "/v1beta/models/gemini-3.5-flash-lite:generateContent"
    for _, ruta, hdr, _ in srv.reqs:
        assert CLAVE not in ruta and {k.lower(): v for k, v in hdr.items()}["x-goog-api-key"] == CLAVE
    cuerpo = gen[3]
    assert cuerpo["systemInstruction"]["parts"][0]["text"] == g.SYSTEM_PROMPT
    assert "Fuga de refrigerante" in cuerpo["contents"][0]["parts"][0]["text"]  # mismo contexto RAG


def test_gemini_model_override_y_google_api_key(srv):
    srv.resp[("POST", "/v1beta/models/")] = [(200, OK_GEMINI)]
    env = _gem(srv, GEMINI_MODEL="gemini-3.8-flash")
    env["GOOGLE_API_KEY"], _ = env.pop("GEMINI_API_KEY"), None
    r = _gen(env)
    assert r["etiqueta"] == "Google Gemini (gemini-3.8-flash)"
    assert [q[0] for q in srv.reqs] == ["POST"]  # sin descubrimiento


def test_gemini_sin_descubrimiento_usa_modelo_por_defecto(srv):
    srv.resp[("GET", "/v1beta/models")] = [(500, {"error": {}})]
    srv.resp[("POST", "/v1beta/models/")] = [(200, OK_GEMINI)]
    assert _gen(_gem(srv))["modelo"] == g.MODELO_GEMINI_DEFECTO


@pytest.mark.parametrize("status", [401, 403])
def test_gemini_401_403_cae_a_plantilla_sin_filtrar_clave(srv, caplog, status):
    srv.resp[("GET", "/v1beta/models")] = [(status, {"error": {"message": f"API key {CLAVE} not valid"}})]
    with caplog.at_level(logging.DEBUG):
        r = _gen(_gem(srv))
    assert r["modo"] == "offline" and r["etiqueta"] == OFFLINE and r["modelo"] is None
    assert f"HTTP {status}" in r["error"] and "plantilla" not in r["error"]
    assert r["texto"] == g._plantilla(SUMMARY, g.construir_contexto(SUMMARY, BIT)[1])
    assert "FALLO LLM (gemini)" in caplog.text
    assert CLAVE not in caplog.text and CLAVE not in json.dumps(r)


def test_gemini_429_reintenta_una_vez_y_se_recupera(srv, _sin_espera):
    srv.resp[("POST", "/v1beta/models/")] = [(429, {"error": {"status": "RESOURCE_EXHAUSTED"}}), (200, OK_GEMINI)]
    r = _gen(_gem(srv, GEMINI_MODEL="gemini-3.5-flash-lite"))
    assert r["modo"] == "gemini" and len(srv.reqs) == 2 and len(_sin_espera) == 1


def test_gemini_429_persistente_cae_a_plantilla(srv, caplog, _sin_espera):
    srv.resp[("POST", "/v1beta/models/")] = [(429, {"error": {"status": "RESOURCE_EXHAUSTED"}})]
    with caplog.at_level(logging.WARNING):
        r = _gen(_gem(srv, GEMINI_MODEL="gemini-3.5-flash-lite"))
    assert len(srv.reqs) == 2 and len(_sin_espera) == 1  # exactamente 1 reintento
    assert r["modo"] == "offline" and r["etiqueta"] == OFFLINE and "HTTP 429" in r["error"]
    assert "FALLO LLM" in caplog.text and CLAVE not in caplog.text


def test_gemini_timeout_cae_a_plantilla(srv):
    srv.resp[("POST", "/v1beta/models/")] = [(200, OK_GEMINI, 1.5)]
    r = _gen(_gem(srv, GEMINI_MODEL="gemini-3.5-flash-lite", GEMINI_TIMEOUT="0.3"))
    assert r["modo"] == "offline" and r["etiqueta"] == OFFLINE and r["error"].startswith("gemini:")


def test_gemini_respuesta_vacia_o_bloqueada_no_se_etiqueta_como_llm(srv):
    srv.resp[("POST", "/v1beta/models/")] = [(200, {"promptFeedback": {"blockReason": "SAFETY"}})]
    r = _gen(_gem(srv, GEMINI_MODEL="gemini-3.5-flash-lite"))
    assert r["etiqueta"] == OFFLINE and "SAFETY" in r["error"]


def test_gemini_clave_en_cuerpo_de_error_se_enmascara(srv):
    srv.resp[("POST", "/v1beta/models/")] = [(400, {"error": {"message": f"bad {CLAVE}"}})]
    r = _gen(_gem(srv, GEMINI_MODEL="gemini-3.5-flash-lite"))
    assert CLAVE not in r["error"] and "***" in r["error"]


# ---------------- Ollama ----------------
def test_ollama_ok_etiqueta_y_formato(srv):
    srv.resp[("POST", "/api/chat")] = [(200, OK_OLLAMA)]
    r = _gen({"OLLAMA_MODEL": "llama3.2:3b", "OLLAMA_HOST": srv.url.replace("http://", "")})  # host sin esquema
    assert r["modo"] == "ollama" and r["etiqueta"] == "Ollama local (llama3.2:3b)"
    assert r["texto"] == "- texto de Ollama falso" and r["error"] is None
    (_, ruta, _, cuerpo), = srv.reqs
    assert ruta == "/api/chat" and cuerpo["stream"] is False and cuerpo["model"] == "llama3.2:3b"
    assert cuerpo["messages"][0] == {"role": "system", "content": g.SYSTEM_PROMPT}
    assert "Fuga de refrigerante" in cuerpo["messages"][1]["content"]


def test_ollama_sin_servidor_cae_a_plantilla(caplog):
    with caplog.at_level(logging.WARNING):
        r = _gen({"OLLAMA_MODEL": "llama3.2:3b", "OLLAMA_HOST": "http://127.0.0.1:1", "OLLAMA_TIMEOUT": "2"})
    assert r["modo"] == "offline" and r["etiqueta"] == OFFLINE and r["error"].startswith("ollama:")
    assert "FALLO LLM (ollama)" in caplog.text


def test_ollama_modelo_inexistente_404_cae_a_plantilla(srv):
    srv.resp[("POST", "/api/chat")] = [(404, {"error": "model 'x' not found"})]
    r = _gen({"OLLAMA_MODEL": "x", "OLLAMA_HOST": srv.url})
    assert r["etiqueta"] == OFFLINE and "404" in r["error"]


def test_ollama_timeout_cae_a_plantilla(srv):
    srv.resp[("POST", "/api/chat")] = [(200, OK_OLLAMA, 1.5)]
    r = _gen({"OLLAMA_MODEL": "m", "OLLAMA_HOST": srv.url, "OLLAMA_TIMEOUT": "0.3"})
    assert r["etiqueta"] == OFFLINE


# ---------------- forzado ----------------
def test_sig_llm_offline_no_llama_a_nadie(srv):
    r = _gen(_gem(srv, SIG_LLM="offline"))
    assert r["modo"] == "offline" and r["etiqueta"] == OFFLINE and r["error"] is None and srv.reqs == []


def test_sig_llm_ollama_gana_a_gemini(srv):
    srv.resp[("POST", "/api/chat")] = [(200, OK_OLLAMA)]
    r = _gen(_gem(srv, SIG_LLM="ollama", OLLAMA_MODEL="llama3.2:3b", OLLAMA_HOST=srv.url))
    assert r["modo"] == "ollama" and all(q[1] == "/api/chat" for q in srv.reqs)


def test_sig_llm_gemini_sin_clave_falla_explicito():
    r = _gen({"SIG_LLM": "gemini"})
    assert r["etiqueta"] == OFFLINE and r["error"].startswith("gemini:")
