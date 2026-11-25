"""--fuente real: NO ejecuta el generador; valida el esquema de data/raw (copia temporal de datos sinteticos tratada como 'reales')."""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import pipeline_etl as etl

ROOT = Path(__file__).resolve().parent.parent
CSV = "telemetria_medidores_ami_2025.csv"


def _como_reales(datos):
    """Deja la copia de data/raw como 'datos reales': sin _metodologia.json (el generador no la dejo ahi)."""
    raw = datos / "data" / "raw"
    (raw / "_metodologia.json").unlink()
    return raw


def _cli(datos, *args, script="pipeline_etl.py"):
    env = {k: v for k, v in os.environ.items() if not k.startswith(("ANTHROPIC", "AWS_", "S3_", "BEDROCK", "GEMINI", "GOOGLE_API", "OLLAMA", "GENAI_"))
           and k not in ("SIG_LLM", "SIG_FUENTE")}
    env.update(SIG_DATA_DIR=str(datos / "data"), SIG_OUT_DIR=str(datos / "out"), MONGO_URI="mongodb://127.0.0.1:1",
               MONGO_TIMEOUT_MS="300", PYTHONIOENCODING="utf-8")
    return subprocess.run([sys.executable, str(ROOT / script), *args], capture_output=True, text=True, env=env, encoding="utf-8")


def test_esquema_valido_pasa(datos):
    res = etl.validar_esquema_raw(_como_reales(datos))
    assert res["edificios"] == 12 and res["docs_iot"] == 4380 and res["facturas_xml"] == 144 and res["filas_ami"] > 4380


def test_columna_faltante_en_csv_da_mensaje_claro(datos):
    raw = _como_reales(datos)
    txt = (raw / CSV).read_text(encoding="utf-8").replace("factor_potencia", "fp", 1)
    (raw / CSV).write_text(txt, encoding="utf-8")
    with pytest.raises(etl.EsquemaInvalido, match=r"telemetria_medidores_ami_2025\.csv: faltan columnas \['factor_potencia'\]"):
        etl.validar_esquema_raw(raw)


def test_archivo_faltante_y_xml_incompleto(datos):
    raw = _como_reales(datos)
    (raw / "bitacora_mantenimiento_hvac_2025.txt").unlink()
    xml = raw / "facturacion_mtd_distribuidora_2025.xml"
    xml.write_text(xml.read_text(encoding="utf-8").replace("cargo_fijo_usd", "otro_campo"), encoding="utf-8")
    with pytest.raises(etl.EsquemaInvalido) as e:
        etl.validar_esquema_raw(raw)
    assert "bitacora_mantenimiento_hvac_2025.txt: archivo no encontrado" in str(e.value) and "cargo_fijo_usd" in str(e.value)


def test_cli_real_valido_no_ejecuta_el_generador(datos):
    raw = _como_reales(datos)
    antes = {p.name: p.stat().st_mtime_ns for p in raw.iterdir()}
    p = _cli(datos, "--fuente", "real")
    assert p.returncode == 0, p.stderr
    assert "fuente: real" in p.stdout and "Fuente REAL" in p.stderr and "Fuente SINTETICA" not in p.stderr
    assert {x.name: x.stat().st_mtime_ns for x in raw.iterdir()} == antes and not (raw / "_metodologia.json").exists()
    s = json.loads((datos / "data" / "gold" / "kpi_summary.json").read_text(encoding="utf-8"))
    assert s["procedencia_datos"]["modo"] == "real" and s["procedencia_datos"]["semilla"] is None
    assert "datos provistos por el usuario" in s["aviso_datos"].lower()


def test_cli_real_invalido_sale_con_error(datos):
    raw = _como_reales(datos)
    (raw / CSV).write_text((raw / CSV).read_text(encoding="utf-8").replace("voltaje_promedio_v", "volt", 1), encoding="utf-8")
    p = _cli(datos, "--fuente", "real")
    assert p.returncode != 0
    assert "EsquemaInvalido" in p.stderr and "voltaje_promedio_v" in p.stderr
    assert not (datos / "data" / "gold" / "kpi_summary.json").exists()  # no se genero nada con datos invalidos


def test_cli_real_sin_datos_falla_y_no_genera(datos):
    raw = _como_reales(datos)
    for p in raw.iterdir():
        p.unlink()
    p = _cli(datos, "--fuente", "real")
    assert p.returncode != 0 and "archivo no encontrado" in p.stderr
    assert list(raw.iterdir()) == []  # el generador NO se ejecuto


def test_sintetico_no_pisa_datos_reales_sin_metodologia(datos):
    _como_reales(datos)
    p = _cli(datos, "--fuente", "sintetico")  # el generador se niega a sobrescribir datos que no son suyos
    assert p.returncode != 0 and "no se sobrescriben" in p.stderr
