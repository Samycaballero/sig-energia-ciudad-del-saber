import sqlite3

import pytest

import config
import pipeline_etl as etl


@pytest.fixture
def db(datos):
    raw = etl.extract()
    rows, _ = etl.clean(raw)
    return etl.load_sqlite(raw, etl.transform(raw, rows)), raw


def test_esquema_y_vistas(db):
    path, _ = db
    c = sqlite3.connect(path)
    objs = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")}
    assert {"dim_edificio", "dim_tiempo", "fact_consumo_diario", "fact_facturacion_mensual",
            "fact_alertas_automatizadas", "vw_bi_ranking_edificios", "vw_bi_tendencia_mensual",
            "vw_bi_detalle_mensual"} <= objs
    cols = {r[1] for r in c.execute("PRAGMA table_info(fact_alertas_automatizadas)")}
    assert {"accion_recomendada", "causa_bitacora"} <= cols
    assert "certificacion_estado" in {r[1] for r in c.execute("PRAGMA table_info(dim_edificio)")}


def test_conteos_y_integridad(db):
    path, raw = db
    c = sqlite3.connect(path)
    n = lambda q: c.execute(q).fetchone()[0]  # noqa: E731
    assert n("SELECT COUNT(*) FROM dim_edificio") == 12
    assert n("SELECT COUNT(*) FROM fact_consumo_diario") == 12 * 365
    assert n("SELECT COUNT(*) FROM fact_facturacion_mensual") == 144
    assert n("SELECT COUNT(*) FROM dim_tiempo") == 365
    assert n("SELECT COUNT(*) FROM fact_consumo_diario WHERE building_id NOT IN (SELECT building_id FROM dim_edificio)") == 0
    assert n("SELECT COUNT(*) FROM fact_consumo_diario WHERE date_key NOT IN (SELECT date_key FROM dim_tiempo)") == 0
    assert n("SELECT COUNT(*) FROM fact_alertas_automatizadas") == n("SELECT SUM(flag_anomalia) FROM fact_consumo_diario")


def test_not_null_se_respeta(db):
    c = sqlite3.connect(db[0])
    with pytest.raises(sqlite3.IntegrityError):
        c.execute("INSERT INTO dim_edificio (building_id, nombre) VALUES ('X', 'x')")


def test_gold_kpis_coherentes_con_vista(datos):
    raw = etl.extract()
    rows, _ = etl.clean(raw)
    etl.load_sqlite(raw, etl.transform(raw, rows))
    s = etl.build_gold(raw=raw)
    assert (config.gold_dir() / "kpi_summary.json").exists()
    assert s["kpis_globales"]["total_edificios"] == 12
    assert abs(sum(r["kwh_anual"] for r in s["ranking_edificios"]) - s["kpis_globales"]["consumo_total_kwh"]) < 1
    assert len(s["detalle_mensual"]) == 144 and len(s["alertas"]) == s["kpis_globales"]["alertas_anomalias_detectadas"]
    imp = s["impacto_financiero_proyectado"]
    assert imp["inversion_estimada_usd"] == s["supuestos"]["inversion"]["capex_usd"]["valor"]
    assert s["diagnostico_gerencial"]["etiqueta"] == "modo offline (plantilla)"
    k = s["kpis_globales"]
    assert k["costo_total_anual_usd"] == pytest.approx(k["costo_energia_anual_usd"] + k["costo_demanda_anual_usd"] + k["costo_fijo_anual_usd"]
                                                       + k["penalizacion_fp_anual_usd"], abs=2)
    assert k["alertas_anomalias_detectadas"] > 0 and 0 < k["concentracion_top3_pct"] < k["concentracion_top4_pct"] < 100
