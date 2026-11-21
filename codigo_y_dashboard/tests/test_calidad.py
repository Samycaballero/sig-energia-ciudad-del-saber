import pytest

import pipeline_etl as etl

CAT = {"B1": {"base_kwh_dia": 1000.0, "pct_fuera_horario": 0.30}}


def fila(fecha, kwh, bid="B1", lab="700", off="300"):
    return {"fecha_lectura": fecha, "building_id": bid, "medidor_ami_id": "M", "kwh_total": kwh,
            "kwh_horario_laboral": lab, "kwh_fuera_horario": off, "demanda_max_kw": "50",
            "factor_potencia": "0.9", "voltaje_promedio_v": "208", "ocupacion_pct": "50"}


def test_fechas_mixtas_se_normalizan():
    rows, a = etl.clean_ami([fila("2025-01-01", "900"), fila("02/01/2025", "950")], CAT)
    assert [r["fecha"] for r in rows] == ["2025-01-01", "2025-01-02"]
    assert a["fechas_normalizadas"] == 1


def test_fecha_invalida_falla_con_mensaje_claro():
    with pytest.raises(ValueError, match="fecha_lectura invalida"):
        etl.clean_ami([fila("31-31-2025", "900")], CAT)


def test_duplicados_se_eliminan():
    rows, a = etl.clean_ami([fila("2025-01-01", "900"), fila("2025-01-01", "905"), fila("2025-01-02", "950")], CAT)
    assert len(rows) == 2 and a["duplicados_eliminados"] == 1


def test_nulos_se_imputan_con_mediana_y_reparten_horario():
    rows, a = etl.clean_ami([fila("2025-01-01", "800"), fila("2025-01-02", "1000"), fila("2025-01-03", "900"),
                             fila("2025-01-04", "", lab="", off="")], CAT)
    r = rows[-1]
    assert a["nulos_imputados"] == 1
    assert r["kwh_total"] == 900.0                      # mediana de 800/1000/900
    assert r["kwh_fuera_horario"] == 270.0 and r["kwh_horario_laboral"] == 630.0  # 30 % del catalogo


def test_outliers_cero_negativo_y_sobre_3x_base_se_corrigen():
    rows, a = etl.clean_ami([fila("2025-01-01", "900"), fila("2025-01-02", "5000"), fila("2025-01-03", "-5"),
                             fila("2025-01-04", "0"), fila("2025-01-05", "1100")], CAT)
    assert a["outliers_corregidos"] == 3
    assert [r["kwh_total"] for r in rows] == [900.0, 1000.0, 1000.0, 1000.0, 1100.0]  # mediana de validos = 1000


def test_edificio_sin_lecturas_validas_usa_base_del_catalogo():
    rows, _ = etl.clean_ami([fila("2025-01-01", "")], CAT)
    assert rows[0]["kwh_total"] == 1000.0


def test_edificio_desconocido_falla():
    with pytest.raises(ValueError, match="no existe en el catalogo"):
        etl.clean_ami([fila("2025-01-01", "900", bid="XX")], CAT)


def test_clean_sobre_datos_reales_no_deja_nulos(datos):
    raw = etl.extract()
    rows, audit = etl.clean(raw)
    assert audit["silver_ami_rows"] == audit["raw_ami_rows"] - audit["duplicados_eliminados"] == len(rows)
    assert all(r["kwh_total"] > 0 and r["kwh_fuera_horario"] is not None for r in rows)
    assert audit["nulos_imputados"] > 0 and audit["outliers_corregidos"] > 0 and audit["duplicados_eliminados"] > 0
