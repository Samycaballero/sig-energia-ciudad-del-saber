import json
import logging
import sys
import types
from pathlib import Path

import genai_diagnostico as g

ROOT = Path(__file__).resolve().parent.parent
SUMMARY = json.loads((ROOT / "data" / "gold" / "kpi_summary.json").read_text(encoding="utf-8"))
BIT = SUMMARY["bitacora_eventos"]


def test_seleccion_de_modo():
    assert g.seleccionar_modo({}) == "offline"
    assert g.seleccionar_modo({"ANTHROPIC_API_KEY": "k"}) == "anthropic"
    assert g.seleccionar_modo({"BEDROCK_MODEL_ID": "m", "AWS_ACCESS_KEY_ID": "a", "AWS_SECRET_ACCESS_KEY": "b"}) == "bedrock"
    assert g.seleccionar_modo({"ANTHROPIC_API_KEY": "k", "BEDROCK_MODEL_ID": "m"}) == "anthropic"
    assert g.seleccionar_modo({"ANTHROPIC_API_KEY": "k", "GENAI_MODE": "offline"}) == "offline"


def test_rag_filtra_por_edificio_y_fecha():
    e = g.recuperar_entradas(BIT, ["CDS-208"])
    assert e and all(x["building_id"] == "CDS-208" for x in e)
    e = g.recuperar_entradas(BIT, ["CDS-208"], desde="2025-03-15", hasta="2025-03-16")
    assert [x["fecha_inicio"] for x in e] == ["2025-03-12"]
    assert g.recuperar_entradas(BIT, ["CDS-208"], desde="2025-12-01") == []


def test_offline_es_determinista_y_etiquetado():
    a = g.generar_diagnostico_gerencial(SUMMARY, BIT, env={})
    b = g.generar_diagnostico_gerencial(SUMMARY, BIT, env={})
    assert a == b and a["modo"] == "offline" and a["etiqueta"] == "modo offline (plantilla)" and a["modelo"] is None
    assert "CDS-208" in a["texto"]


def _falso_anthropic(monkeypatch, falla=False):
    llamadas = {}

    class Msgs:
        def create(self, **kw):
            llamadas.update(kw)
            if falla:
                raise RuntimeError("sin red")
            return types.SimpleNamespace(content=[types.SimpleNamespace(type="text", text="- texto del LLM falso")])

    class Cli:
        def __init__(self, api_key):
            llamadas["api_key"] = api_key
            self.messages = Msgs()

    monkeypatch.setitem(sys.modules, "anthropic", types.SimpleNamespace(Anthropic=Cli))
    return llamadas


def test_modo_anthropic_usa_modelo_y_contexto_rag(monkeypatch):
    # CLIENTE FALSO: verifica cableado (modelo, contexto, etiqueta), NO la calidad ni la API real
    ll = _falso_anthropic(monkeypatch)
    r = g.generar_diagnostico_gerencial(SUMMARY, BIT, env={"ANTHROPIC_API_KEY": "k"})
    assert ll["model"] == "claude-haiku-5-5" and "Fuga de refrigerante" in ll["messages"][0]["content"]
    assert r["modo"] == "anthropic" and r["etiqueta"] == "Anthropic API (claude-haiku-5-5)" and "LLM falso" in r["texto"]


def test_fallo_del_llm_cae_a_plantilla_con_aviso(monkeypatch, caplog):
    _falso_anthropic(monkeypatch, falla=True)
    with caplog.at_level(logging.WARNING):
        r = g.generar_diagnostico_gerencial(SUMMARY, BIT, env={"ANTHROPIC_API_KEY": "k"})
    assert r["modo"] == "offline" and r["etiqueta"] == "modo offline (plantilla)" and "sin red" in r["error"]
    assert "FALLO LLM" in caplog.text
