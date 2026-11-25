# SIG de consumo eléctrico: Fundación Ciudad del Saber (caso de estudio)

Proyecto final de **Sistemas de Información Gerencial** (UTP, FISC).

**Pregunta gerencial:** ¿qué edificios de Ciudad del Saber presentan mayor consumo eléctrico y huella de carbono, y cuáles deben intervenirse primero? Pregunta de seguimiento derivada: ¿qué edificios se desvían de su historial de consumo (alertas z-score) y de la referencia interna? ("línea base" = historial del propio edificio y edificio de referencia interno, el Complejo de Hospedaje; no es una línea base oficial).

> **Datos:** no se obtuvieron datos reales de consumo de la Fundación (no son públicos). Se usan **datos sintéticos anclados en fuentes oficiales con fecha** (tarifas ASEP, factores de emisión de MiAmbiente, huella publicada de 2021, clima, feriados y ventas de EDEMET 2025). Los hallazgos describen el modelo, no el campus real. Si llegan datos reales, el pipeline los acepta con `--fuente real`.

## Contenido

| Ruta | Qué contiene |
|---|---|
| `codigo_y_dashboard/` | Pipeline ETL (bronze/silver/gold), SQLite en modelo estrella, MongoDB (pymongo), DAG ejecutable, IA generativa opcional, dashboard HTML interactivo y 84 pruebas. **Instrucciones completas en su `README.md`.** |
| `docs/metodologia_datos.md` | **Cómo se obtuvieron los datos:** qué se buscó, cómo genera los datos el script, rangos mín/máx y en qué se basan, validaciones y limitaciones. |
| `docs/fuentes_oficiales.md` | Datos reales usados con **periodo, fecha de publicación, fecha de consulta, URL y estado**. |
| `docs/solicitud_datos_ciudad_del_saber.md` | Borrador de la solicitud formal de datos reales (aún no enviada). |
| `presentacion/` | Presentación grupal (sin cédulas). |

## Inicio rápido

```bash
cd codigo_y_dashboard
python -m venv .venv && . .venv/bin/activate   # en Windows: ver nota de rutas largas en su README
pip install -r requirements.txt
python orquestacion_airflow_dag.py --run-local   # pipeline completo (8 tareas)
python -m pytest -q                              # 84 pruebas
```

Abrir `codigo_y_dashboard/dashboard_interactivo.html` en el navegador (funciona sin conexión).

Sin credenciales, MongoDB cae a un fallback JSON explícito y la IA generativa corre en «modo offline (plantilla)». Gemini (nivel gratuito), Ollama, Anthropic, Bedrock y S3 se activan solo con variables de entorno (ver README interno).
