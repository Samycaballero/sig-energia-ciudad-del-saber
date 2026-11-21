"""
DAG de orquestacion (Apache Airflow) del SIG de Gestion Energetica - Fundacion Ciudad del Saber.

Cada tarea ejecuta codigo real (pipeline_etl, mongo_store, s3_publish, generar_dashboard_y_graficos).
Los datos pasan entre tareas por archivos (data/silver, data/gold), no por XCom.

  Con Airflow instalado : este archivo define el DAG `dag_cds_energia_sig_diario` (PythonOperator).
  Sin Airflow           : python orquestacion_airflow_dag.py --run-local
                          ejecuta las mismas tareas en orden de dependencias; sale con codigo != 0 si una falla.
"""
import logging
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # Airflow no siempre agrega la carpeta del DAG

import config  # noqa: E402
import generar_dashboard_y_graficos as dash  # noqa: E402
import mongo_store  # noqa: E402
import pipeline_etl as etl  # noqa: E402
import s3_publish  # noqa: E402

log = logging.getLogger("dag")


# ---------------------------------------------------------------- tareas
def t_extract():
    raw = etl.extract()
    return raw["audit"]


def t_clean():
    _, audit = etl.clean(etl.extract())
    return audit


def t_transform_load_sql():
    raw = etl.extract()
    tf = etl.transform(raw, etl.read_silver())
    db = etl.load_sqlite(raw, tf)
    return {"db": str(db), "filas_fact_consumo": len(tf["hechos"]), "alertas": len(tf["alertas"])}


def t_load_mongo():
    return mongo_store.guardar_alertas_y_telemetria(etl.leer_alertas(), etl.extract()["iot"])


def t_gold_y_diagnostico():
    s = etl.build_gold()
    d = s["diagnostico_gerencial"]
    return {"edificios": len(s["ranking_edificios"]), "diagnostico_modo": d["etiqueta"]}


def t_dashboard():
    return dash.main()


def t_publish_s3():
    return s3_publish.publicar()


# task_id -> (callable, dependencias)
TAREAS = {
    "extract_validar_4_formatos": (t_extract, []),
    "clean_silver": (t_clean, ["extract_validar_4_formatos"]),
    "transform_load_sqlite": (t_transform_load_sql, ["clean_silver"]),
    "load_mongodb": (t_load_mongo, ["transform_load_sqlite"]),
    "gold_kpis_y_diagnostico_ia": (t_gold_y_diagnostico, ["transform_load_sqlite"]),
    "dashboard_y_graficos": (t_dashboard, ["gold_kpis_y_diagnostico_ia"]),
    "publicar_s3_opcional": (t_publish_s3, ["load_mongodb", "dashboard_y_graficos"]),
}


def orden_topologico(tareas=None):
    tareas = tareas or TAREAS
    orden, hechas = [], set()
    while len(orden) < len(tareas):
        listas = [t for t, (_, deps) in tareas.items() if t not in hechas and all(d in hechas for d in deps)]
        if not listas:
            raise ValueError("Dependencias ciclicas en TAREAS")
        orden += listas
        hechas.update(listas)
    return orden


def run_local(tareas=None):
    """Ejecuta las tareas en orden. Devuelve 0 si todas OK, 1 si alguna falla (se detiene)."""
    tareas = tareas or TAREAS
    for tid in orden_topologico(tareas):
        t0 = time.time()
        try:
            res = tareas[tid][0]()
        except Exception:
            log.error("TAREA FALLIDA: %s\n%s", tid, traceback.format_exc())
            return 1
        log.info("OK %-30s %.1fs -> %s", tid, time.time() - t0, res)
    return 0


# ---------------------------------------------------------------- DAG real (solo si Airflow esta instalado)
try:
    from airflow import DAG
    try:
        from airflow.providers.standard.operators.python import PythonOperator  # Airflow 3
    except ImportError:
        from airflow.operators.python import PythonOperator  # Airflow 2
except ImportError:
    dag = None
else:
    with DAG(
        dag_id="dag_cds_energia_sig_diario",
        default_args={"owner": "direccion_tecnologia_cds", "retries": 2, "retry_delay": timedelta(minutes=5)},
        description="Pipeline energetico: extract, clean, SQLite, MongoDB, KPIs+IA, dashboard, S3 opcional",
        schedule="0 2 * * *",
        start_date=datetime(2025, 1, 1, tzinfo=timezone(timedelta(hours=-5))),
        catchup=False,
        tags=["SIG", "Energia"],
    ) as dag:
        _ops = {tid: PythonOperator(task_id=tid, python_callable=fn) for tid, (fn, _) in TAREAS.items()}
        for tid, (_, deps) in TAREAS.items():
            for d in deps:
                _ops[d] >> _ops[tid]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    if "--run-local" in sys.argv:
        sys.exit(run_local())
    print(__doc__)
    sys.exit(2)
