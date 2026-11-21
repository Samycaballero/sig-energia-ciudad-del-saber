# SIG de Gestión y Eficiencia Energética — Fundación Ciudad del Saber (Panamá)

Proyecto Final de **Sistemas de Información Gerencial** — Universidad Tecnológica de Panamá (FISC).

Pregunta gerencial: **¿Qué edificios presentan mayor consumo eléctrico y qué acciones basadas en datos permiten reducir costos, picos de demanda y huella de carbono?**

Equipo: Laura Saucedo (Directora de Tecnología, líder), Samir Caballero (Ing. de Datos / BI), Orianis Castro (Operaciones y Eficiencia Energética), Javier Valdés (Finanzas y Sostenibilidad).

## Qué es real y qué no (leer primero)

| Componente | Estado |
|---|---|
| **Datos de entrada** | **Sintéticos**, generados con `generar_datos_fuente.py` (semilla fija) con fines académicos. Los nombres de edificios provienen de un catálogo del campus; las lecturas, facturas y bitácoras **no son mediciones ni documentos reales**. Incluyen defectos de calidad inyectados a propósito (nulos, duplicados, outliers, fechas mixtas). |
| Pipeline ETL (CSV, JSON, XML, TXT) → SQLite estrella → CSV/KPIs | **Real y ejecutable** (`pipeline_etl.py`). SQLite hace de base SQL del prototipo (el diseño objetivo habla de Amazon RDS PostgreSQL; **no implementado**). |
| Detección de anomalías | Real: z-score diario por edificio ≥ 2,1 (umbral en `supuestos.json`). No hay Isolation Forest ni otros modelos. |
| DAG de orquestación | Cada tarea llama código real. Se prueba con `--run-local` **sin Airflow**. Con Airflow instalado define el DAG con `PythonOperator`; **no se ha ejecutado en un Airflow real** (ver Limitaciones). |
| MongoDB | Real con `pymongo` (`mongo_store.py`) si hay servidor; **sin servidor usa un fallback explícito a JSON** y lo avisa en el log (`MODO FALLBACK: sin Mongo, guardado a JSON`). |
| AWS S3 | Opcional (`s3_publish.py`): solo actúa con `S3_BUCKET` + credenciales; si no, registra `S3 OMITIDO`. |
| IA generativa | Opcional (`genai_diagnostico.py`): LLM real **solo** con `ANTHROPIC_API_KEY` (o Bedrock con `BEDROCK_MODEL_ID` + credenciales). Sin clave: **"modo offline (plantilla)"**, texto determinista; el dashboard y la figura 6 rotulan el modo realmente usado. |
| Alertas "automatizadas" | Se **detectan y registran** en SQLite/Mongo con la causa recuperada de la bitácora y una acción *recomendada*. **No** se crean órdenes CMMS ni se envían notificaciones. |
| Ahorros, CAPEX, tarifas, factores de emisión | **Supuestos** en `supuestos.json`, con tipo de fuente y justificación. Solo se cita literatura donde existe (ver abajo); el resto está marcado "supuesto del equipo". No son resultados medidos. |
| Figura 1 (arquitectura) | Arquitectura **objetivo/propuesta**; su subtítulo indica qué parte está implementada. |

### Supuestos financieros (`supuestos.json`)
Ahorro BMS/horarios 8,8 % de la factura, peak shaving 14 % del cargo por demanda, eliminación 95 % de la penalización por factor de potencia, retrofit HVAC 11 % del cargo de energía de los 3 edificios top, CAPEX 285.000 USD: **todos son "supuesto del equipo"**. Contexto de literatura citable: Mills, E. (2011), *Building commissioning: a golden opportunity for reducing energy costs and greenhouse-gas emissions in the United States*, Energy Efficiency 4(2):145–173, doi:10.1007/s12053-011-9116-8 — mediana de 16 % de ahorro de energía de edificio completo en edificios existentes (13 % obra nueva), payback mediano 1,1 años (4,2 obra nueva). Sirve para ubicar el orden de magnitud (8,8 % es conservador frente a 16 %), **no** para derivar los porcentajes. Tarifas y factores de emisión son ilustrativos: sustituir por el pliego tarifario de ASEP y el factor oficial antes de usarlos como evidencia.

## Estructura
```
codigo_y_dashboard/
├── config.py                       # rutas (relativas al código) y carga de supuestos
├── supuestos.json                  # supuestos con fuente/justificación
├── generar_datos_fuente.py         # genera los 4 archivos sintéticos en data/raw
├── pipeline_etl.py                 # extract / clean / transform / load_sqlite / build_gold
├── mongo_store.py                  # alertas + telemetría IoT en MongoDB (o fallback JSON)
├── s3_publish.py                   # publicación opcional a S3
├── genai_diagnostico.py            # RAG simple sobre la bitácora + LLM o plantilla offline
├── orquestacion_airflow_dag.py     # DAG de Airflow y modo --run-local
├── generar_dashboard_y_graficos.py # 6 figuras 300 DPI + dashboard HTML
├── dashboard_template.html         # plantilla del dashboard (se inyectan los datos)
├── dashboard_interactivo.html      # dashboard autocontenido (abrir en el navegador, sin internet)
├── docker-compose.yml              # MongoDB local
├── requirements.txt, .env.example, tests/
├── data/{raw,silver,gold}/         # Bronze / Silver / Gold (SQLite, CSV BI, kpi_summary.json)
└── graficos/                       # figuras para el informe y la presentación
```

## Instalación
```bash
python -m venv .venv
.venv\Scripts\activate          # Windows   (Linux/macOS: source .venv/bin/activate)
pip install -r requirements.txt
```
Python 3.10+. Airflow no está en `requirements.txt` (opcional; ver abajo).

## Variables de entorno (todas opcionales; el código no lee `.env`, expórtelas en el shell — ver `.env.example`)
| Variable | Efecto |
|---|---|
| `MONGO_URI` (defecto `mongodb://localhost:27017`), `MONGO_DB`, `MONGO_TIMEOUT_MS` | Conexión MongoDB. |
| `MONGO_STRICT=1` | Si no hay Mongo, falla en vez de usar el fallback JSON. |
| `ANTHROPIC_API_KEY`, `ANTHROPIC_MODEL` (defecto `claude-haiku-5-5`) | Activa el modo LLM de Anthropic. |
| `BEDROCK_MODEL_ID` (+ credenciales AWS, `AWS_REGION`) | Activa Amazon Bedrock (si no hay clave de Anthropic). |
| `GENAI_MODE=offline\|anthropic\|bedrock` | Fuerza un modo. |
| `S3_BUCKET`, `S3_PREFIX` (+ credenciales AWS) | Activa la publicación a S3. |
| `SIG_DATA_DIR`, `SIG_OUT_DIR` | Cambian la carpeta de datos y de salidas (los tests las usan). |

## Cómo ejecutar
```bash
python generar_datos_fuente.py                  # (opcional) regenera data/raw sintético
python pipeline_etl.py                          # ETL completo (+ Mongo o fallback JSON)
python orquestacion_airflow_dag.py --run-local  # las 7 tareas del DAG en orden; exit != 0 si una falla
python generar_dashboard_y_graficos.py          # figuras + dashboard (lee data/gold/kpi_summary.json)
python -m pytest -q                             # tests
```
`--run-local` ya ejecuta ETL, Mongo, KPIs+diagnóstico, dashboard y S3 (omitido sin configuración), por lo que basta con ese comando para regenerar todo.

**Modo real con MongoDB:** `docker compose up -d` y luego volver a correr el pipeline; el log mostrará `MongoDB: N alertas y M documentos IoT upserted`. **Modo real con LLM:** `set ANTHROPIC_API_KEY=...` (Windows) o `export ANTHROPIC_API_KEY=...` antes de correr; el diagnóstico y el dashboard mostrarán `Anthropic API (claude-haiku-5-5)`. **Offline (defecto):** el rótulo será `modo offline (plantilla)`.

**Airflow real:** instalar `apache-airflow` en Linux/WSL/Docker siguiendo las constraints oficiales y copiar esta carpeta a la carpeta `dags/`; el archivo define `dag_cds_energia_sig_diario` (02:00, hora de Panamá) con las mismas tareas y dependencias que `--run-local`.

## Dashboard
`dashboard_interactivo.html` es autocontenido (sin CDN). Filtros: edificio, certificación LEED, rango de meses y criterio de orden; KPIs, ranking, tendencia mensual, tabla de detalle y alertas se recalculan con los filtros. El EUI se muestra **anualizado linealmente** (kWh del período × 12/meses ÷ m²). Payback/ROI solo se muestran con campus completo y 12 meses. El panel "Copiloto" tiene dos partes: el diagnóstico generado por el pipeline (rotulado con el modo realmente usado; no cambia con los filtros) y un resumen dinámico calculado en el navegador con plantilla (sin IA).

## Limitaciones de verificación
- Airflow real, LLM real (Anthropic/Bedrock), S3 real y un servidor MongoDB real **no se han probado en este entorno**. Los tests cubren: `--run-local` de punta a punta, MongoDB con `mongomock`, S3 con `moto` (emulación en memoria), clientes LLM falsos para el cableado, y un módulo `airflow` falso que comprueba las dependencias del DAG.
- Los datos son sintéticos: los hallazgos describen el comportamiento del modelo, no el consumo real del campus.
- Cambios de esquema respecto a versiones previas: la columna `diagnostico_genai` ahora es `causa_bitacora` (es una recuperación de la bitácora, no texto generado) y `accion_automatizada` ahora es `accion_recomendada`; `mongodb_alertas_genai.json` fue reemplazado por la colección real o por `data/gold/mongo_fallback_*.json`.
