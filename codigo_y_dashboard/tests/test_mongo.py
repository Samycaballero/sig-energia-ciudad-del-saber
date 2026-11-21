import json
import logging

import mongomock
import pytest

import mongo_store

ALERTAS = [{"fecha": "2025-03-12", "building_id": "CDS-208", "tipo_alerta": "ANOMALIA_CONSUMO_PICO",
            "severidad": "ALTA", "desviacion_pct": 32.0, "accion_recomendada": "x", "causa_bitacora": "y"}]
IOT = [{"timestamp": "2025-01-01T14:00:00-05:00", "building_id": "CDS-208", "ambiente": {"temp_exterior_c": 33.0}},
       {"timestamp": "2025-01-02T14:00:00-05:00", "building_id": "CDS-208", "ambiente": {"temp_exterior_c": 32.0}}]


def test_inserta_alertas_y_telemetria_en_mongomock():
    cli = mongomock.MongoClient()
    r = mongo_store.guardar_alertas_y_telemetria(ALERTAS, IOT, client=cli, db_name="t")
    assert r["modo"] == "mongo" and r["alertas"] == 1 and r["telemetria_iot"] == 2
    assert cli["t"]["alertas"].count_documents({"building_id": "CDS-208"}) == 1
    doc = cli["t"]["telemetria_iot"].find_one({"timestamp": IOT[0]["timestamp"]})
    assert doc["ambiente"]["temp_exterior_c"] == 33.0 and doc["ts"].year == 2025


def test_es_idempotente():
    cli = mongomock.MongoClient()
    for _ in range(2):
        mongo_store.guardar_alertas_y_telemetria(ALERTAS, IOT, client=cli, db_name="t")
    assert cli["t"]["alertas"].count_documents({}) == 1
    assert cli["t"]["telemetria_iot"].count_documents({}) == 2


def test_sin_mongo_usa_fallback_explicito(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv("MONGO_URI", "mongodb://127.0.0.1:1")
    monkeypatch.setenv("MONGO_TIMEOUT_MS", "300")
    with caplog.at_level(logging.WARNING):
        r = mongo_store.guardar_alertas_y_telemetria(ALERTAS, IOT, fallback_dir=tmp_path)
    assert r["modo"] == "fallback_json"
    assert "MODO FALLBACK: sin Mongo, guardado a JSON" in caplog.text
    assert json.loads((tmp_path / "mongo_fallback_alertas.json").read_text(encoding="utf-8")) == ALERTAS


def test_modo_estricto_falla_en_vez_de_fallback(tmp_path, monkeypatch):
    monkeypatch.setenv("MONGO_URI", "mongodb://127.0.0.1:1")
    monkeypatch.setenv("MONGO_TIMEOUT_MS", "300")
    monkeypatch.setenv("MONGO_STRICT", "1")
    with pytest.raises(RuntimeError, match="MongoDB no disponible"):
        mongo_store.guardar_alertas_y_telemetria(ALERTAS, IOT, fallback_dir=tmp_path)
    assert not list(tmp_path.glob("*.json"))
