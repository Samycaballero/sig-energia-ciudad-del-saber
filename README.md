# SIG de consumo eléctrico: Fundación Ciudad del Saber (caso de estudio)

Proyecto final de **Sistemas de Información Gerencial** (UTP, FISC). Pregunta gerencial: *¿qué edificios presentan mayor consumo eléctrico?*

> **Los datos son 100 % sintéticos** y se generan con `codigo_y_dashboard/generar_datos_fuente.py` con fines académicos. Los roles del equipo son una simulación académica.

## Contenido

| Carpeta | Qué contiene |
|---|---|
| `codigo_y_dashboard/` | Pipeline ETL (bronze/silver/gold), SQLite en modelo estrella, MongoDB (pymongo), DAG de Airflow ejecutable, IA generativa opcional, dashboard HTML interactivo y 28 pruebas. **Instrucciones completas en su `README.md`.** |
| `presentacion/` | Presentación grupal (sin cédulas). |

## Inicio rápido

```bash
cd codigo_y_dashboard
python -m venv .venv && . .venv/bin/activate   # en Windows: .venv\Scripts\activate
pip install -r requirements.txt
python orquestacion_airflow_dag.py --run-local   # pipeline completo
python -m pytest -q                              # 28 pruebas
```

Abrir `codigo_y_dashboard/dashboard_interactivo.html` en el navegador (funciona sin conexión).

Sin credenciales, MongoDB cae a un fallback JSON explícito y la IA generativa corre en «modo offline (plantilla)». S3 y el LLM real se activan solo con variables de entorno (ver README interno).
