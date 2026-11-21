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
| MongoDB | Real con `pymongo` (`mongo_store.py`). **Probado contra un `mongod` nativo local** (v9.0.2, `localhost:27017`): `--run-local` cargó 43 alertas y 4380 documentos IoT en `cds_energia` (modo `mongo`, no fallback) y por consulta se comprobaron idempotencia y consultas por rango. Las pruebas automatizadas usan `mongomock`. **Sin servidor usa un fallback explícito a JSON** y lo avisa en el log (`MODO FALLBACK: sin Mongo, guardado a JSON`). No probados: el contenedor `docker compose` y MongoDB Atlas. |
| AWS S3 | Opcional (`s3_publish.py`): solo actúa con `S3_BUCKET` + credenciales; si no, registra `S3 OMITIDO`. |
| IA generativa | Opcional (`genai_diagnostico.py`): LLM **gratuito** (Google Gemini, nivel gratuito, o Ollama local) o de pago (Anthropic, Bedrock). **Probado de forma real solo con Google Gemini** (`gemini-3.5-flash-lite`): la corrida entregada generó el diagnóstico con «Google Gemini (gemini-3.5-flash-lite)» (`kpi_summary.json`, dashboard y figura 6 lo rotulan). Ollama, Anthropic (un intento con clave de ejemplo devolvió 401) y Bedrock **no se han probado contra el servicio real**; la selección de proveedor y la caída a plantilla se verifican con servidores HTTP simulados. Sin proveedor válido (modo por defecto) o si el LLM falla: **"modo offline (plantilla)"**, texto determinista, con aviso en el log. El LLM no es determinista: el texto puede variar entre corridas. |
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

> **Windows y rutas largas:** si el repositorio está en una carpeta con ruta larga (por ejemplo dentro de *Documentos* con nombres extensos), `pip install` puede fallar con `OSError: [Errno 2] No such file or directory` por el límite de 260 caracteres de Windows (lo provoca el paquete `anthropic`). Solución: crear el entorno en una ruta corta fuera del proyecto, por ejemplo `python -m venv C:\venv-sig` y usar `C:\venv-sig\Scripts\python.exe` en lugar de activar `.venv`.

## Variables de entorno (todas opcionales; el código no lee `.env`, expórtelas en el shell — ver `.env.example`)
| Variable | Efecto |
|---|---|
| `MONGO_URI` (defecto `mongodb://localhost:27017`), `MONGO_DB`, `MONGO_TIMEOUT_MS` | Conexión MongoDB. |
| `MONGO_STRICT=1` | Si no hay Mongo, falla en vez de usar el fallback JSON. |
| `ANTHROPIC_API_KEY`, `ANTHROPIC_MODEL` (defecto `claude-haiku-5-5`) | Activa el modo LLM de Anthropic. |
| `BEDROCK_MODEL_ID` (+ credenciales AWS, `AWS_REGION`) | Activa Amazon Bedrock (si no hay otro proveedor). |
| `GEMINI_API_KEY` (o `GOOGLE_API_KEY`), `GEMINI_MODEL` | Google Gemini, nivel gratuito (clave sin tarjeta en https://aistudio.google.com/apikey). Sin `GEMINI_MODEL` se descubre un modelo flash-lite/flash. |
| `OLLAMA_MODEL` (p. ej. `llama3.2:3b`), `OLLAMA_HOST` (defecto `http://localhost:11434`) | Ollama local, sin clave; solo se activa si `OLLAMA_MODEL` está definido. |
| `SIG_LLM=offline\|gemini\|ollama\|anthropic\|bedrock` | Fuerza un proveedor (`GENAI_MODE` se acepta como alias). Orden automático: Anthropic > Gemini > Ollama > Bedrock > plantilla. |
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

**Modo real con MongoDB:** tener un `mongod` en `localhost:27017` (o `docker compose up -d`, que no se ha probado) y volver a correr el pipeline; el log mostrará `MongoDB: N alertas y M documentos IoT upserted`. **LLM gratis (Gemini):** `set GEMINI_API_KEY=<su-clave>` (Windows; PowerShell: `$env:GEMINI_API_KEY="<su-clave>"`) o `export GEMINI_API_KEY=<su-clave>` (clave en https://aistudio.google.com/apikey; no la guarde en el repositorio), opcionalmente `GEMINI_MODEL`, y correr `python orquestacion_airflow_dag.py --run-local`; el rótulo será `Google Gemini (<modelo>)`. **LLM gratis local (Ollama):** `ollama pull llama3.2:3b`, `export OLLAMA_MODEL=llama3.2:3b` y correr; el rótulo será `Ollama local (<modelo>)`. `SIG_LLM` fuerza un proveedor. **Privacidad:** el nivel gratuito de la API de Gemini limita la cuota y Google puede usar el contenido enviado para mejorar sus productos (https://aistudio.google.com/docs/terms y https://aistudio.google.com/docs/pricing); aquí los datos son sintéticos (riesgo nulo), pero con datos reales habría que usar el nivel de pago u Ollama local. **Sin proveedor válido (defecto) o si falla:** el rótulo será `modo offline (plantilla)`.

**Airflow real:** instalar `apache-airflow` en Linux/WSL/Docker siguiendo las constraints oficiales y copiar esta carpeta a la carpeta `dags/`; el archivo define `dag_cds_energia_sig_diario` (02:00, hora de Panamá) con las mismas tareas y dependencias que `--run-local`.

## Dashboard
`dashboard_interactivo.html` es autocontenido (sin CDN). Filtros: edificio, certificación LEED, rango de meses y criterio de orden; KPIs, ranking, tendencia mensual, tabla de detalle y alertas se recalculan con los filtros. El EUI se muestra **anualizado linealmente** (kWh del período × 12/meses ÷ m²). Payback/ROI solo se muestran con campus completo y 12 meses. El panel "Copiloto" tiene dos partes: el diagnóstico generado por el pipeline (rotulado con el modo realmente usado; no cambia con los filtros) y un resumen dinámico calculado en el navegador con plantilla (sin IA).

## Limitaciones de verificación
- **Probado con un servidor real:** MongoDB. `python orquestacion_airflow_dag.py --run-local` contra un `mongod` nativo local (v9.0.2, `localhost:27017`) registró `MongoDB: 43 alertas y 4380 documentos IoT upserted en cds_energia` y la tarea `load_mongodb` devolvió modo `mongo` (no fallback); además se verificaron por consulta con `pymongo` la idempotencia y las consultas por rango.
- **Probado con un LLM real:** solo Google Gemini (nivel gratuito, `gemini-3.5-flash-lite`): la corrida entregada generó el diagnóstico con él. La salida contiene al menos una imprecisión menor frente a los datos (dice que 3 de los 11 días anómalos del Ed. 184 no tienen causa en la bitácora; son 4), por lo que requiere validación humana.
- **No se han probado en este entorno:** Airflow real, Ollama, Anthropic (un intento con clave de ejemplo dio 401) y Bedrock reales, S3 real, el contenedor `docker compose` de MongoDB y MongoDB Atlas. Los tests (47, todos pasan; 19 de ellos de la selección de proveedor y la caída a plantilla con servidores HTTP falsos) cubren: `--run-local` de punta a punta (con fallback de Mongo), MongoDB con `mongomock`, S3 con `moto` (emulación en memoria), clientes LLM falsos para el cableado, y un módulo `airflow` falso que comprueba las dependencias del DAG.
- Los datos son sintéticos: los hallazgos describen el comportamiento del modelo, no el consumo real del campus.
- Cambios de esquema respecto a versiones previas: la columna `diagnostico_genai` ahora es `causa_bitacora` (es una recuperación de la bitácora, no texto generado) y `accion_automatizada` ahora es `accion_recomendada`; `mongodb_alertas_genai.json` fue reemplazado por la colección real o por `data/gold/mongo_fallback_*.json`.
