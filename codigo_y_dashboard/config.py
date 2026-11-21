"""Rutas y supuestos del proyecto. Todo relativo a este archivo; sobreescribible por entorno.

SIG_DATA_DIR : carpeta con raw/silver/gold (por defecto ./data)
SIG_OUT_DIR  : carpeta donde se escriben graficos/ y dashboard_interactivo.html (por defecto ./)
"""
import json
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


def data_dir() -> Path:
    return Path(os.environ.get("SIG_DATA_DIR") or BASE_DIR / "data")


def raw_dir() -> Path:
    return data_dir() / "raw"


def silver_dir() -> Path:
    return data_dir() / "silver"


def gold_dir() -> Path:
    return data_dir() / "gold"


def out_dir() -> Path:
    return Path(os.environ.get("SIG_OUT_DIR") or BASE_DIR)


def cargar_supuestos() -> dict:
    """Devuelve supuestos.json (incluye fuente/justificacion de cada valor)."""
    with open(BASE_DIR / "supuestos.json", encoding="utf-8") as f:
        return json.load(f)


def valor(supuestos: dict, seccion: str, clave: str) -> float:
    return supuestos[seccion][clave]["valor"]
