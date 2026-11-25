import importlib
import json
import os
import subprocess
import sys
import types
from pathlib import Path

import orquestacion_airflow_dag as dag

ROOT = Path(__file__).resolve().parent.parent


def _correr(datos, *args):
    env = {k: v for k, v in os.environ.items() if not k.startswith(("ANTHROPIC", "AWS_", "S3_", "BEDROCK", "GEMINI", "GOOGLE_API", "OLLAMA", "GENAI_"))
           and k != "SIG_LLM"}
    env.update(SIG_DATA_DIR=str(datos / "data"), SIG_OUT_DIR=str(datos / "out"), MONGO_URI="mongodb://127.0.0.1:1",
               MONGO_TIMEOUT_MS="300", PYTHONIOENCODING="utf-8")
    return subprocess.run([sys.executable, str(ROOT / "orquestacion_airflow_dag.py"), "--run-local", *args],
                          capture_output=True, text=True, env=env, encoding="utf-8")


def test_run_local_punta_a_punta(datos):
    p = _correr(datos)
    assert p.returncode == 0, p.stderr
    for tid in dag.TAREAS:
        assert f"OK {tid}" in p.stderr
    assert "MODO FALLBACK: sin Mongo, guardado a JSON" in p.stderr and "S3 OMITIDO" in p.stderr
    out, gold = datos / "out", datos / "data" / "gold"
    for f in ("cds_energia_sig.db", "kpi_summary.json", "bi_ranking_edificios_2025.csv", "mongo_fallback_alertas.json"):
        assert (gold / f).exists(), f
    assert len(list((out / "graficos").glob("*.png"))) == 6
    html = (out / "dashboard_interactivo.html").read_text(encoding="utf-8")
    assert "modo offline (plantilla)" in html and "filterBldg" in html and "monthFrom" in html and "filterLeed" in html
    assert "Amazon Bedrock RAG" not in html and "PPA" not in html and "Enel" not in html and "INDICASAT" not in html
    # la fuente sintetica regenera data/raw con el generador y deja su metodologia
    assert "Fuente SINTETICA" in p.stderr and (datos / "data" / "raw" / "_metodologia.json").exists()
    s = json.loads((gold / "kpi_summary.json").read_text(encoding="utf-8"))
    assert s["procedencia_datos"]["modo"] == "sintetico_anclado" and s["procedencia_datos"]["plausibilidad"]["dentro_de_rango"]
    assert s["procedencia_datos"]["semilla"] == 202511


def test_run_local_falla_con_codigo_distinto_de_cero(datos):
    # con --fuente real no se regenera nada: si falta un archivo, la primera tarea falla con mensaje claro
    (datos / "data" / "raw" / "_metodologia.json").unlink()
    (datos / "data" / "raw" / "facturacion_mtd_distribuidora_2025.xml").unlink()
    p = _correr(datos, "--fuente", "real")
    assert p.returncode != 0
    assert "TAREA FALLIDA: fuente_datos" in p.stderr and "facturacion_mtd_distribuidora_2025.xml: archivo no encontrado" in p.stderr


def test_run_local_fuente_real_valida_corre_y_no_genera(datos):
    raw = datos / "data" / "raw"
    (raw / "_metodologia.json").unlink()
    antes = {f.name: f.stat().st_mtime_ns for f in raw.iterdir()}
    p = _correr(datos, "--fuente", "real")
    assert p.returncode == 0, p.stderr
    assert "Fuente REAL" in p.stderr and "Fuente SINTETICA" not in p.stderr
    assert {f.name: f.stat().st_mtime_ns for f in raw.iterdir()} == antes  # el generador no corrio
    s = json.loads((datos / "data" / "gold" / "kpi_summary.json").read_text(encoding="utf-8"))
    assert s["procedencia_datos"]["modo"] == "real"


def test_run_local_fuente_real_con_esquema_invalido(datos):
    raw = datos / "data" / "raw"
    (raw / "_metodologia.json").unlink()
    f = raw / "sensores_iot_hvac_clima_2025.json"
    f.write_text(f.read_text(encoding="utf-8").replace("cop_eficiencia", "cop", 1), encoding="utf-8")
    p = _correr(datos, "--fuente", "real")
    assert p.returncode != 0 and "TAREA FALLIDA: fuente_datos" in p.stderr and "hvac.cop_eficiencia" in p.stderr


def test_orden_topologico_respeta_dependencias():
    orden = dag.orden_topologico()
    for tid, (_, deps) in dag.TAREAS.items():
        assert all(orden.index(d) < orden.index(tid) for d in deps)


def test_run_local_se_detiene_en_la_primera_falla():
    llamadas = []

    def mal():
        raise RuntimeError("boom")

    t = {"a": (lambda: llamadas.append("a"), []), "b": (mal, ["a"]), "c": (lambda: llamadas.append("c"), ["b"])}
    assert dag.run_local(t) == 1 and llamadas == ["a"]


def test_dag_airflow_con_modulo_falso_cablea_dependencias(monkeypatch):
    # airflow FALSO: solo prueba que el DAG se arma con las mismas dependencias; no es Airflow real
    deps = []

    class Op:
        def __init__(self, task_id, python_callable):
            self.task_id = task_id

        def __rshift__(self, o):
            deps.append((self.task_id, o.task_id))
            return o

    class DAG:
        def __init__(self, **kw):
            self.kw = kw

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    af = types.ModuleType("airflow")
    af.DAG = DAG
    py = types.ModuleType("airflow.operators.python")
    py.PythonOperator = Op
    monkeypatch.setitem(sys.modules, "airflow", af)
    monkeypatch.setitem(sys.modules, "airflow.operators", types.ModuleType("airflow.operators"))
    monkeypatch.setitem(sys.modules, "airflow.operators.python", py)
    monkeypatch.setitem(sys.modules, "airflow.providers", None)  # fuerza la rama Airflow 2
    m = importlib.reload(dag)
    try:
        assert m.dag.kw["dag_id"] == "dag_cds_energia_sig_diario"
        assert set(deps) == {(d, t) for t, (_, ds) in m.TAREAS.items() for d in ds}
    finally:
        monkeypatch.undo()
        importlib.reload(dag)
