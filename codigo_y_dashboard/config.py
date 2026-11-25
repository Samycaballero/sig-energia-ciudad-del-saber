"""Rutas y supuestos del proyecto. Todo relativo a este archivo; sobreescribible por entorno.

SIG_DATA_DIR : carpeta con raw/silver/gold (por defecto ./data)
SIG_OUT_DIR  : carpeta donde se escriben graficos/ y dashboard_interactivo.html (por defecto ./)
SIG_FUENTE   : sintetico (defecto; el DAG regenera data/raw) o real (no genera: solo valida data/raw)
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


def cargar_fuentes() -> dict:
    """Devuelve fuentes_oficiales.json (anclas oficiales con URL y estado)."""
    with open(BASE_DIR / "fuentes_oficiales.json", encoding="utf-8") as f:
        return json.load(f)


def ancla(fuentes: dict, ancla_id: str) -> dict:
    return next(x for x in fuentes["fuentes"] if x["id"] == ancla_id)


def cargar_parametros(path=None) -> dict:
    """Devuelve parametros_generador.json (rangos min/max, ancla y justificacion de cada parametro del generador)."""
    with open(path or BASE_DIR / "parametros_generador.json", encoding="utf-8") as f:
        return json.load(f)


def recargo_fp_fraccion(fp: float, umbral: float, por_centesima: float) -> float:
    """Fraccion de recargo: `por_centesima` por cada 0.01 de FP bajo el umbral (continuo); 0 si fp >= umbral.
    Simplificacion: el pliego lo aplica por tramos y solo a comercializacion/distribucion (ver fuentes_oficiales.json)."""
    return max(0.0, (umbral - fp) / 0.01 * por_centesima)
