# SIG de Gestión y Eficiencia Energética — Fundación Ciudad del Saber (Panamá)

Proyecto Final de **Sistemas de Información Gerencial** — Universidad Tecnológica de Panamá (FISC).

Pregunta gerencial: **¿Qué edificios de Ciudad del Saber presentan mayor consumo eléctrico y huella de carbono, y cuáles deben intervenirse primero?** Pregunta de seguimiento derivada: ¿qué edificios se desvían de su historial de consumo (alertas z-score) y de la referencia interna? ("línea base" = historial del propio edificio y edificio de referencia interno, el Complejo de Hospedaje; no es una línea base oficial).

Equipo: Laura Saucedo (Directora de Tecnología, líder), Samir Caballero (Ing. de Datos / BI), Orianis Castro (Operaciones y Eficiencia Energética), Javier Valdés (Finanzas y Sostenibilidad).

## Qué es real y qué no (leer primero)

| Componente | Estado |
|---|---|
| **Datos de entrada** | **Sintéticos anclados** (`generar_datos_fuente.py`, semilla fija). Se anclan, **con fecha**, en: tarifa EDEMET MTD (energía, demanda, cargo fijo), regla de factor de potencia, factor de emisión del SIN 2024, escala (tope de la huella 2021 de la Fundación), áreas reales de 3 edificios (tesis 2018), clima histórico de Albrook, feriados 2025 y estacionalidad mensual de EDEMET 2025. **Todo lo demás es supuesto del equipo**: EUI por tipo de edificio, áreas de los demás edificios, % fuera de horario, factor de potencia por edificio, ruido, defectos inyectados, porcentajes de ahorro y CAPEX. Solo 6 nombres de edificios son reales (los verificados); el resto es «ilustrativo». Las lecturas, facturas y bitácoras **no son mediciones ni documentos reales**. |
| Distribuidora | **EDEMET es «probable, no confirmado»**: inferencia geográfica (Clayton queda al oeste del límite EDEMET/ENSA según ASEP); no hay factura ni documento que nombre a Clayton. |
| Tarifa y año | La tarifa es la del pliego de jul-2026 a jun-2030 aplicada a consumo simulado de 2025 (valoración de referencia, anacronismo declarado); el factor de emisión 2024 se usa para 2025 (no hay factor 2025). |
| Pipeline ETL (CSV, JSON, XML, TXT) → SQLite estrella → CSV/KPIs | **Real y ejecutable** (`pipeline_etl.py`). SQLite hace de base SQL del prototipo (el diseño objetivo habla de Amazon RDS PostgreSQL; **no implementado**). |
| Detección de anomalías | Real: z-score diario por edificio ≥ 2,1 (umbral en `supuestos.json`). No hay Isolation Forest ni otros modelos. Los días marcados incluyen variaciones naturales del perfil semanal además de las fallas inyectadas. |
| DAG de orquestación | Cada tarea llama código real. Se prueba con `--run-local` **sin Airflow**. Con Airflow instalado define el DAG con `PythonOperator`; **no se ha ejecutado en un Airflow real**. |
| MongoDB | Real con `pymongo` (`mongo_store.py`). En una versión anterior (v1) de los datos se probó contra un `mongod` nativo local; **con los datos de esta versión se probó solo con `mongomock` y con el fallback JSON** (la corrida entregada usó un `MONGO_URI` inválido a propósito para no tocar ninguna base local: `MODO FALLBACK: sin Mongo, guardado a JSON`). No probados: el contenedor `docker compose` y MongoDB Atlas. |
| AWS S3 | Opcional (`s3_publish.py`): solo actúa con `S3_BUCKET` + credenciales; si no, registra `S3 OMITIDO`. |
| IA generativa | Opcional (`genai_diagnostico.py`): LLM gratuito (Google Gemini, Ollama local) o de pago (Anthropic, Bedrock). **La corrida entregada usa "modo offline (plantilla)"** (sin variables de LLM): texto determinista. Gemini se probó de forma real solo sobre los datos v1; Ollama, Anthropic y Bedrock **no se han probado contra el servicio real** (selección de proveedor y caída a plantilla se verifican con servidores HTTP simulados). Si se usa un LLM, el texto no es determinista y requiere validación humana. |
| Alertas "automatizadas" | Se **detectan y registran** en SQLite/Mongo con la causa recuperada de la bitácora y una acción *recomendada*. **No** se crean órdenes CMMS ni se envían notificaciones. |
| Ahorros, CAPEX | **Supuestos del equipo** en `supuestos.json`; no son resultados medidos. |
| Certificaciones LEED | Solo **reportadas por una fuente, no verificadas en USGBC**: Plaza Ciudad del Saber (LEED Gold, ACR 2013), Complejo de Hospedaje (LEED Platinum, Revista Panorama 2023), Ed. 128 (nivel sin confirmar). Los demás edificios: «Sin dato». |
| Figura 1 (arquitectura) | Arquitectura **objetivo/propuesta**; su subtítulo indica qué parte está implementada. |

## Cómo se generan los datos (método y respaldo)

`generar_datos_fuente.py` **no tiene números escritos en el código**: lee `parametros_generador.json`, donde cada parámetro tiene **min, max, unidad, ancla** (id de `fuentes_oficiales.json` o «supuesto del equipo»), **justificación** y, si la ancla es oficial, sus **tres fechas** (`periodo_del_dato`, `fecha_publicacion`, `fecha_consulta`); la semilla (202511) también está ahí.

1. **Perfil diario por edificio** = índice mensual EDEMET 2025 × clima × perfil de día laborable/no laborable × falla operativa × ruido.
   - **Clima (R3):** la temperatura diaria se genera alrededor de la **media mensual histórica de Albrook** (estación 142-002, serie de 66 años de ETESA/IMHPA citada en el EIA de MiAmbiente publicado el 2023-08-08; fuente secundaria): 26,2–28,0 °C, ruido σ = 0,9 °C y tope superior de 34 °C. Es **climatología histórica, no el clima real de 2025**. La humedad tiene dato real solo en ene, may, ago, sep, oct, nov y dic (85,0–89,2 %); feb, mar, abr, jun y jul **no tienen dato** y usan un valor supuesto de 87,5 % dentro del rango 85–90 %. La estación seca se toma como ≈ 11-dic a 26-abr y la lluviosa como ≈ 26-abr a 11-dic (el pipeline etiqueta cada día así).
   - **Feriados 2025 (R4):** 16 fechas (1-ene, 9-ene, 3/4/5-mar, 17/18-abr, 1-may, 3/4/5/10/28-nov, 8-dic, 20-dic, 25-dic; el 5-mar con reserva; 24 y 31-dic **no** son feriados; el duelo del 22 al 24-abr fue día ordinario). Un día no laborable (fin de semana o feriado) usa el perfil de fin de semana de cada tipo de edificio, la ocupación y horas HVAC de fin de semana y +10 puntos de consumo fuera de horario. Resultado: **104 fines de semana + 15 feriados en día hábil (el 20-dic cae en sábado) = 119 días no laborables y 246 laborables**.
   - **Estacionalidad EDEMET 2025 (R5):** las ventas mensuales de EDEMET (3.903,01 GWh en el año; relación máx/mín 1,11 en EDEMET y 1,07 en el total de distribuidoras: estacionalidad plana) se convierten en un índice (ventas del mes ÷ días del mes, normalizado a promedio 1) que multiplica la carga diaria. Como contraste real (no se usa en el cálculo): el campus Víctor Levi Sasso de la UTP en 2024 sumó 10.772.377 kWh con un patrón académico (consumo más alto entre abr y oct, con caídas en nov: 787.453 kWh y dic: 549.547 kWh).
2. **Escala anual**: el perfil se escala para que el consumo anual sea exactamente **EUI objetivo × área**. El EUI objetivo de cada edificio está dentro del rango de su tipo (laboratorio 300–420, eventos/convenciones 200–300, académico 190–290, oficinas 150–260, comercial 220–330, tecnología 170–270, recreación/eventos 180–280, hospedaje 90–160 kWh/m²/año): **supuestos del equipo**. Como orientación **no confirmada** de la literatura (clima cálido-húmedo) se cita 47–628 kWh/m²/año (*Energies* 2021, 14(21), 7013; no verificado): solo sirve para comprobar que los rangos no sean absurdos.
3. **Áreas (R2):** Ed. 128 = 3.511 m², Ed. 108 = 1.027 m² y Ed. 202 = 704 m², de la tesis de R. Osorio (TalTech, 2018, «District Cooling Potential in Panama City», Apéndice 1, que usa el Plan Maestro de CdS). La tesis solo usa **números** de edificio: que 128 sea la Casa de las Naciones Unidas, 108 la Casa Club y 202 Clayton Bowling es una **suposición del equipo**, y las áreas son de 2018 (pueden haber cambiado). No se reutilizan las áreas de 104, 127 ni 124. Ed. 205 Senacyt, Plaza CdS, Complejo de Hospedaje y los edificios «ilustrativo» tienen áreas **ilustrativas** (supuesto del equipo) elegidas para que el área total del catálogo sea de 12.000–16.000 m² (hoy 14.342 m²). El catálogo guarda `area_fuente` y `area_fecha`.
4. **Facturación** (XML): toda la energía se factura a la tarifa EDEMET MTD (energía, demanda máxima del mes, cargo fijo por cliente-mes, recargo por factor de potencia) y las emisiones con un único factor. No existe contrato renovable ni otra tarifa. **Anacronismo declarado (R7):** el pliego es de jul-2026 a jun-2030 y los datos son de 2025, por lo que el consumo 2025 se valora a la tarifa vigente de 2026 (valoración de referencia). Igualmente, el factor de emisión 2024 (0,1501) se aplica a 2025 porque es el más reciente publicado (no hay factor 2025).
5. **Defectos de calidad** en el CSV (para mostrar la limpieza): conteos exactos `round(probabilidad × filas)` de nulos (1,2 %), lecturas negativas (0,6 %), picos espurios ×9,5 (0,4 %), fechas `dd/mm/yyyy` (2,5 %) y duplicados (1,1 %), en posiciones elegidas por la semilla.
6. **Verificación**: el generador comprueba que todo valor usado o generado quede dentro de `[min, max]` (si no, falla) y escribe `data/raw/_metodologia.json` con semilla, rangos efectivos y observados, defectos inyectados, días no laborables, fecha y versión. **Misma semilla + mismo `parametros_generador.json` → mismos datos** (los tests lo verifican). Cambiar la semilla: `python generar_datos_fuente.py --semilla 7`.
7. El generador **no sobrescribe** una carpeta `data/raw` que no tenga `_metodologia.json` (podrían ser datos reales) salvo con `--forzar`.

### Anclas oficiales (`fuentes_oficiales.json`) y sus fechas
Todas las entradas llevan **periodo del dato, fecha de publicación y fecha de consulta (2026-11-24)**. La tabla completa (dato, valor, periodo, publicación, consulta, URL, estado) se genera en **`data/gold/tabla_fechas_datos.csv`** y también va en `kpi_summary.json` (`procedencia_datos.fechas_datos`). Resumen:

| Dato | Valor | Periodo | Publicación | Estado |
|---|---|---|---|---|
| Energía MTD EDEMET | **0,15570 USD/kWh** (B/. a la par con USD; sensibilidad 0,150–0,160; cota ENSA 0,15672 para 1-sep a 31-dic-2026) | pliego jul-2026 a jun-2030 | Res. AN No. 21781-Elec, 2026-06-18 (publicación del archivo no confirmada) | confirmado con reservas (archivo en la carpeta de consulta pública cp_008-26; vigencia semestral exacta no confirmada) |
| Demanda y cargo fijo MTD EDEMET | **20,23 USD/kW-mes** (ENSA 15,23; rango 15–20,5) y **14,32 USD/cliente-mes** | ídem | ídem | confirmado con reservas |
| Factor de potencia | umbral **0,90**; recargo **2 %** por cada 0,01 por debajo | ídem (pp. 3–4) | ídem | confirmado con reservas. **Simplificación**: la regla real aplica solo a comercialización y distribución; aquí se aplica sobre el cargo por demanda, de forma continua |
| Factor de emisión del SIN 2024 | **0,1501 tCO₂/MWh = 0,1501 kg/kWh** (no se usan 0,4438/0,4932, margen combinado MDL) | 2024 | 2025-08-21 (Gaceta 30348; Res. MIPRE-2025-0030765) | confirmado; se usa para 2025 (el más reciente) |
| Factores del SIN 2021 / 2022 / 2023 | 0,13 (Tabla 19, doc. MiAmbiente, archivo sept-2024) / 0,1079 / 0,1971 (SNE, mayo 2024: 2.403.211 tCO₂ ÷ 12.190.859 MWh) | 2021 / 2022 / 2023 | 2024-09 / no indicada / 2024-05 | 2021 confirmado; 2022 y 2023 confirmados con reservas (fuente primaria no detallada) |
| Huella de la Fundación | **450 tCO₂e** (https://ciudaddelsaber.org/impacto/sostenibilidad; sin alcances ni año de publicación) | 2021 | no indicada | confirmado con reservas |
| **Ancla de escala** (tope) | **3.462 MWh/año** = 450 ÷ 0,13 (rango 2.283–4.170 MWh con factores 2023–2022) | 2021 | cálculo del equipo | **deducción**: es un techo si toda la huella fuera electricidad (alcances desconocidos), no una cifra publicada |
| Áreas Ed. 128 / 108 / 202 | 3.511 / 1.027 / 704 m² | 2018 | 2018 (tesis TalTech) | confirmado con reservas; correspondencia número→nombre = suposición |
| Clima Albrook | temperatura media mensual 26,2–28,0 °C; humedad (7 meses) 85,0–89,2 % | serie histórica de 66 años (climatología) | 2023-08-08 (EIA MiAmbiente, doc. 91714; fuente secundaria) | confirmado con reservas. Respaldo Weather Spark (Tmáx media 30,0–31,7 °C, Tmín 24,4–25,0 °C; sin fecha de publicación): no confirmado, no se usa |
| Feriados 2025 | 16 fechas | 2025 | 2025-01-31 (calendario Global Bank/Red ACH); EY, Telemetro y La Estrella como apoyo | confirmado con reservas (base legal Código de Trabajo arts. 46, 47, 49 citada vía prensa; texto consolidado no abierto) |
| Ventas EDEMET 2025 | 12 valores mensuales, 3.903,01 GWh | 2025 | PDF creados entre 2025-03-10 y 2026-01-20 (ASEP) | confirmado con reservas (las ventas de ENSA feb-abr son idénticas en la fuente: probable copia) |
| UTP (contraste) | campus Víctor Levi Sasso 10.772.377 kWh; 15.384.602 kWh en 15 sedes | 2024 / 2025 | 2025 / no indicada | confirmado; **no es ancla de escala** |
| Precio ocasional 2025 | 66,33 USD/MWh | 2025, 2.º semestre | no indicada | confirmado; sin uso en cálculos |
| Zona de servicio | EDEMET cubre el oeste de la ciudad de Panamá (límite con ENSA: Corredor Sur, Vía Ernesto T. Lefevre, Av. 12 de Octubre, Av. Simón Bolívar, Av. Ricardo J. Alfaro) | 2025, 2.º semestre | PDF creado 2026-09-14 (ASEP) | evidencia geográfica |

**Distribuidora (R6):** Clayton queda al oeste de ese límite, por lo que se **infiere EDEMET** (estado **«probable, no confirmado»**: ningún documento nombra a Clayton). Se recomienda pedir una factura/NIS a la Fundación.

Marco normativo citado por el equipo, **no usado en ningún cálculo ni verificado por el código**: Ley 69 de 2012, Decreto 398 de 2013, Res. SNE 3142 de 2016, Res. JTIA 002 de 2023. Las URL y cifras provienen de la investigación previa del equipo; el código no las contrasta con internet.

### Plausibilidad de escala (comprobación automática)
El pipeline compara el consumo anual simulado del campus con el **tope de 3.462 MWh** (huella 2021 de la Fundación ÷ factor 2021) y **exige una razón entre 0,5× y 1,0×** (modo sintético; en modo real solo informa). Es una comparación de **orden de magnitud, no una validación** (el tope es una deducción con alcances desconocidos). Resultado actual: 3.077,4 MWh = **0,889×** del tope. La UTP solo es **contraste**: la simulación equivale a 0,286× del campus Víctor Levi Sasso 2024 (sin razón exigida). Como referencia adicional, las emisiones simuladas (461,9 tCO₂e con 0,1501) quedan cerca de las 450 tCO₂e de 2021, pero es una coincidencia de orden de magnitud, no una validación (alcances y año distintos). `kpi_summary.json` incluye el bloque `procedencia_datos` (modo `sintetico_anclado`, semilla, anclas con sus fechas, plausibilidad, tabla de fechas, supuestos del equipo y simplificaciones).

### Supuestos financieros (`supuestos.json`)
Ahorro BMS/horarios 8,8 % de la factura, peak shaving 14 % del cargo por demanda, eliminación 95 % de la penalización por factor de potencia, retrofit HVAC 11 % del cargo de energía de los 3 edificios top, CAPEX 285.000 USD: **todos «supuesto del equipo»**. Contexto citable (no deriva los porcentajes): Mills, E. (2011), *Building commissioning…*, Energy Efficiency 4(2):145–173, doi:10.1007/s12053-011-9116-8 — mediana de 16 % de ahorro de energía de edificio completo en edificios existentes, payback mediano 1,1 años. Tarifas y factor de emisión: `tipo_fuente: "oficial"` con URL. El CO₂ evitado es una aproximación proporcional (emisiones × ahorro/factura).

### Cifras de la corrida entregada (`data/gold/kpi_summary.json`, semilla 202511)
Consumo 3.077,4 MWh (12 edificios, 14.342 m²) · EUI promedio 214,6 y máximo 350,0 kWh/m² · factura 671.002 USD (energía 479.146; demanda 186.231 = 27,8 %; cargo fijo 2.062; recargo FP 3.562) · emisiones 461,9 tCO₂e · 60 días con alerta · 35,2 % del consumo fuera de horario · top-3 46,8 % y top-4 56,2 % · ahorro proyectado 113.171 USD/año (16,9 %), payback 30,2 meses (2,52 años), ROI a 3 años 19,1 %, CO₂ evitado 77,9 tCO₂/año. **El CAPEX (285.000 USD) no se reescaló al campus recalibrado: sigue siendo un supuesto del equipo**, por eso el payback sube respecto a versiones previas. Son resultados **del modelo sintético**, no del campus real.

## Datos reales (`--fuente real`)
Con `--fuente real` (en `pipeline_etl.py` y en `orquestacion_airflow_dag.py --run-local`) **no se ejecuta el generador**: se valida que `data/raw/` contenga los archivos con el esquema esperado y, si algo falta o no cumple, se imprime qué archivo/columna falla y el proceso sale con código ≠ 0 (no se escribe silver ni gold). `procedencia_datos.modo` pasa a `real` (la plausibilidad solo informa).

```bash
python pipeline_etl.py --fuente real
python orquestacion_airflow_dag.py --run-local --fuente real     # o SIG_FUENTE=real
```

**Esquema esperado en `data/raw/`** (los 4 formatos + catálogo; se prueba con una copia temporal de los datos sintéticos tratada como reales):

| Archivo | Contenido requerido (tipo, unidad) |
|---|---|
| `catalogo_edificios_cds.json` | Lista JSON de edificios con: `building_id` (texto), `nombre` (texto), `categoria` (texto), `area_m2` (número, m²), `anio_rehab` (entero), `cert_leed` (texto), `base_kwh_dia` (número, kWh/día típico; el ETL descarta lecturas > 3× este valor), `pct_fuera_horario` (0–1), `chiller_antiguedad_anios` (número). Opcionales: `certificacion_estado`, `nombre_estado`. |
| `telemetria_medidores_ami_2025.csv` | Columnas: `fecha_lectura` (`yyyy-mm-dd` o `dd/mm/yyyy`), `building_id`, `medidor_ami_id`, `kwh_total`, `kwh_horario_laboral`, `kwh_fuera_horario` (kWh/día; pueden venir vacíos), `demanda_max_kw` (kW), `factor_potencia` (0–1), `voltaje_promedio_v` (V), `ocupacion_pct` (%). Cada `building_id` debe existir en el catálogo. |
| `sensores_iot_hvac_clima_2025.json` | Lista de documentos: `timestamp` (ISO 8601), `building_id`, `ambiente.temp_exterior_c` (°C), `ambiente.humedad_relativa_pct` (%), `hvac.estado_chiller` (texto), `hvac.cop_eficiencia` (número). Se admiten campos extra. |
| `facturacion_mtd_distribuidora_2025.xml` | Elementos `<FacturaMensual id_factura building_id mes>` con los hijos numéricos `kwh_facturados`, `demanda_maxima_kw`, `factor_potencia_mensual`, `cargo_energia_usd`, `cargo_demanda_usd`, `cargo_fijo_usd`, `penalizacion_fp_usd`, `total_factura_usd` (USD) y `emisiones_alcance2_kg_co2` (kg CO₂). |
| `bitacora_mantenimiento_hvac_2025.txt` | Líneas `[yyyy-mm-dd] EDIFICIO=ID \| SEVERIDAD=ALTA\|MEDIA\|BAJA \| REPORTE_TECNICO: texto` (rango opcional `[ini a fin]`). Al menos una línea válida. |

El ETL espera 12 meses por edificio y un año de datos diarios; con otras dimensiones el dashboard se adapta, pero la proyección financiera se calcula sobre lo que haya en la factura.

## Estructura
```
codigo_y_dashboard/
├── config.py                       # rutas (relativas al código), supuestos, fuentes y parámetros
├── fuentes_oficiales.json          # anclas oficiales (valor, unidad, 3 fechas, entidad, URL, estado) + normativa
├── parametros_generador.json       # rangos min/max, ancla y justificación de cada parámetro del generador + semilla
├── supuestos.json                  # tarifas/emisiones (oficial) y ahorros/CAPEX (supuesto del equipo)
├── generar_datos_fuente.py         # genera los 4 archivos sintéticos + catálogo + _metodologia.json en data/raw
├── pipeline_etl.py                 # fuente (sintético|real) / extract / clean / transform / load_sqlite / build_gold
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

> **Windows y rutas largas:** si el repositorio está en una carpeta con ruta larga, `pip install` puede fallar con `OSError: [Errno 2] No such file or directory` por el límite de 260 caracteres (lo provoca el paquete `anthropic`). Solución: crear el entorno en una ruta corta fuera del proyecto, por ejemplo `python -m venv C:\venv-sig` y usar `C:\venv-sig\Scripts\python.exe`.

## Variables de entorno (todas opcionales; el código no lee `.env`, expórtelas en el shell — ver `.env.example`)
| Variable | Efecto |
|---|---|
| `MONGO_URI` (defecto `mongodb://localhost:27017`), `MONGO_DB`, `MONGO_TIMEOUT_MS` | Conexión MongoDB. |
| `MONGO_STRICT=1` | Si no hay Mongo, falla en vez de usar el fallback JSON. |
| `ANTHROPIC_API_KEY`, `ANTHROPIC_MODEL` (defecto `claude-haiku-5-5`) | Activa el modo LLM de Anthropic. |
| `BEDROCK_MODEL_ID` (+ credenciales AWS, `AWS_REGION`) | Activa Amazon Bedrock (si no hay otro proveedor). |
| `GEMINI_API_KEY` (o `GOOGLE_API_KEY`), `GEMINI_MODEL` | Google Gemini, nivel gratuito (clave sin tarjeta en https://aistudio.google.com/apikey). |
| `OLLAMA_MODEL` (p. ej. `llama3.2:3b`), `OLLAMA_HOST` | Ollama local, sin clave; solo se activa si `OLLAMA_MODEL` está definido. |
| `SIG_LLM=offline\|gemini\|ollama\|anthropic\|bedrock` | Fuerza un proveedor (`GENAI_MODE` es alias). Orden automático: Anthropic > Gemini > Ollama > Bedrock > plantilla. |
| `SIG_FUENTE=sintetico\|real` | Fuente de datos (equivale a `--fuente`). Defecto: `sintetico`. |
| `S3_BUCKET`, `S3_PREFIX` (+ credenciales AWS) | Activa la publicación a S3. |
| `SIG_DATA_DIR`, `SIG_OUT_DIR` | Cambian la carpeta de datos y de salidas (los tests las usan). |

## Cómo ejecutar
```bash
python generar_datos_fuente.py                  # (opcional) regenera data/raw sintético (+ _metodologia.json)
python pipeline_etl.py                          # genera datos + ETL completo (+ Mongo o fallback JSON)
python orquestacion_airflow_dag.py --run-local  # las 8 tareas del DAG en orden; exit != 0 si una falla
python generar_dashboard_y_graficos.py          # figuras + dashboard (lee data/gold/kpi_summary.json)
python -m pytest -q                             # tests
```
`--run-local` ejecuta fuente de datos (generación o validación), ETL, Mongo, KPIs+diagnóstico, dashboard y S3 (omitido sin configuración), por lo que basta con ese comando para regenerar todo. **Cuidado:** para no tocar un MongoDB local existente, defina antes `MONGO_URI` con un valor inválido (p. ej. `mongodb://localhost:1/?serverSelectionTimeoutMS=500`) y se usará el fallback JSON; las pruebas automatizadas ya lo hacen.

**Modo real con MongoDB:** tener un `mongod` en `localhost:27017` (o `docker compose up -d`, no probado); el log mostrará `MongoDB: N alertas y M documentos IoT upserted`. **LLM gratis (Gemini):** `set GEMINI_API_KEY=<su-clave>` (PowerShell: `$env:GEMINI_API_KEY="<su-clave>"`) o `export GEMINI_API_KEY=<su-clave>`; no la guarde en el repositorio. **Privacidad:** el nivel gratuito de Gemini puede usar el contenido enviado para mejorar productos de Google (https://aistudio.google.com/docs/terms); aquí los datos son sintéticos, pero con datos reales habría que usar el nivel de pago u Ollama local. **Sin proveedor válido (defecto):** `modo offline (plantilla)`.

**Airflow real:** instalar `apache-airflow` en Linux/WSL/Docker siguiendo las constraints oficiales y copiar esta carpeta a `dags/`; el archivo define `dag_cds_energia_sig_diario` (02:00, hora de Panamá) con las mismas tareas y dependencias que `--run-local`. Con `SIG_FUENTE=sintetico` (defecto) cada ejecución regenera `data/raw`; con datos reales defina `SIG_FUENTE=real`.

## Dashboard
`dashboard_interactivo.html` es autocontenido (sin CDN). Filtros: edificio, certificación LEED reportada, rango de meses y criterio de orden; KPIs, ranking, tendencia mensual, tabla de detalle y alertas se recalculan con los filtros. El EUI se muestra **anualizado linealmente** (kWh del período × 12/meses ÷ m²). Payback/ROI solo se muestran con campus completo y 12 meses. El pie de página muestra la procedencia de los datos, las anclas y la plausibilidad. El panel "Copiloto" tiene dos partes: el diagnóstico generado por el pipeline (rotulado con el modo realmente usado; no cambia con los filtros) y un resumen dinámico calculado en el navegador con plantilla (sin IA).

## Limitaciones de verificación
- **No verificado por el código:** las cifras y URL de las anclas oficiales (provienen de la investigación previa del equipo; no se consultan en la ejecución); la vigencia semestral exacta del pliego; que EDEMET sea la distribuidora de Clayton (probable, no confirmado); la correspondencia número→nombre de los edificios 128, 108 y 202 (suposición); que el tope de 3.462 MWh sea realmente electricidad (alcances de la huella 2021 desconocidos); la fuente primaria de los factores 2022 y 2023; los nombres de los feriados y su base legal (vía prensa); las certificaciones LEED; los nombres reales de edificios.
- **Climatología, no clima 2025:** la temperatura y la humedad son medias históricas de Albrook (fuente secundaria), no mediciones de 2025; la estacionalidad de consumo viene de las ventas de EDEMET (un agregado de ciudad, no del campus).
- **Probado con un servidor real:** MongoDB, solo sobre la versión v1 de los datos (corrida anterior contra un `mongod` local). Con estos datos solo `mongomock` y el fallback JSON.
- **Probado con un LLM real:** solo Google Gemini sobre los datos v1. La corrida entregada de esta versión usa la plantilla offline.
- **No se han probado en este entorno:** Airflow real, Ollama, Anthropic y Bedrock reales, S3 real, el contenedor `docker compose` de MongoDB y MongoDB Atlas. Los tests (ver `python -m pytest -q`) cubren: generador (rangos, semilla, defectos), anclas (tarifas, factor de emisión, plausibilidad, procedencia), `--fuente real` válido e inválido, `--run-local` de punta a punta, MongoDB con `mongomock`, S3 con `moto`, clientes LLM falsos y un módulo `airflow` falso que comprueba las dependencias del DAG.
- Los datos son sintéticos: los hallazgos describen el comportamiento del modelo, no el consumo real del campus.
- Cambios de esquema respecto a la v1: se eliminó el contrato renovable (las columnas de kWh por origen renovable/red, el porcentaje renovable por edificio y el KPI de energía renovable); la factura incorpora `cargo_fijo_usd`; `dim_edificio` incorpora `certificacion_estado` y `nombre_estado`; el catálogo cambia de nombres/IDs (se añaden CDS-108 y CDS-202); los KPI de EUI de referencia y de EUI máximo ahora se llaman `eui_referencia_hospedaje` y `eui_maximo`.
