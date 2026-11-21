"""Persistencia NoSQL en MongoDB (pymongo): alertas y telemetria IoT/HVAC.

MONGO_URI (defecto mongodb://localhost:27017), MONGO_DB (defecto cds_energia),
MONGO_TIMEOUT_MS (defecto 3000), MONGO_STRICT=1 -> si no hay Mongo, lanza error en vez de usar fallback.
Sin conexion y sin STRICT: log "MODO FALLBACK: sin Mongo, guardado a JSON" y archivos mongo_fallback_*.json.
"""
import json
import logging
import os
from datetime import datetime

import config

log = logging.getLogger("mongo")
URI_DEFECTO = "mongodb://localhost:27017"


def _claves():
    return {"alertas": ("building_id", "fecha", "tipo_alerta"), "telemetria_iot": ("building_id", "timestamp")}


def _docs_telemetria(iot_docs):
    out = []
    for d in iot_docs:
        d = dict(d)
        d["ts"] = datetime.fromisoformat(d["timestamp"])  # tipo fecha nativo para consultas por rango
        out.append(d)
    return out


def _upsert(col, docs, claves):
    # replace_one por documento (no bulk_write): portable a mongomock; shortcut: 4.4k docs/dia, usar bulk si crece
    for d in docs:
        col.replace_one({k: d[k] for k in claves}, d, upsert=True)
    return len(docs)


def conectar(uri=None, timeout_ms=None):
    from pymongo import MongoClient
    uri = uri or os.environ.get("MONGO_URI") or URI_DEFECTO
    timeout_ms = int(timeout_ms or os.environ.get("MONGO_TIMEOUT_MS") or 3000)
    cli = MongoClient(uri, serverSelectionTimeoutMS=timeout_ms)
    cli.admin.command("ping")  # falla aqui si no hay servidor
    return cli


def guardar_alertas_y_telemetria(alertas, iot_docs, client=None, db_name=None, fallback_dir=None):
    """Upsert idempotente. Devuelve {'modo': 'mongo'|'fallback_json', ...}."""
    from pymongo.errors import PyMongoError
    db_name = db_name or os.environ.get("MONGO_DB") or "cds_energia"
    try:
        cli = client or conectar()
        db = cli[db_name]
        k = _claves()
        n_a = _upsert(db["alertas"], [dict(a) for a in alertas], k["alertas"])
        n_t = _upsert(db["telemetria_iot"], _docs_telemetria(iot_docs), k["telemetria_iot"])
        log.info("MongoDB: %d alertas y %d documentos IoT upserted en %s", n_a, n_t, db_name)
        return {"modo": "mongo", "db": db_name, "alertas": n_a, "telemetria_iot": n_t}
    except PyMongoError as exc:
        if os.environ.get("MONGO_STRICT") == "1":
            raise RuntimeError(f"MongoDB no disponible y MONGO_STRICT=1: {exc}") from exc
        log.warning("MODO FALLBACK: sin Mongo, guardado a JSON (%s)", type(exc).__name__)
        d = fallback_dir or config.gold_dir()
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "mongo_fallback_alertas.json"), "w", encoding="utf-8") as f:
            json.dump(alertas, f, ensure_ascii=False, indent=2)
        with open(os.path.join(d, "mongo_fallback_telemetria_iot.json"), "w", encoding="utf-8") as f:
            json.dump(iot_docs, f, ensure_ascii=False)
        return {"modo": "fallback_json", "alertas": len(alertas), "telemetria_iot": len(iot_docs)}
