# Cómo se obtuvieron los datos iniciales

## 1. Resumen
- **Datos reales de consumo eléctrico de Ciudad del Saber: no se obtuvieron.** La búsqueda en fuentes públicas no encontró kWh, EUI ni facturas de la Fundación ni de sus residentes (es una búsqueda de ausencia). La única cifra real propia de la Fundación es su huella publicada: **450 tCO2e en 2021** (sin alcances ni metodología).
- Se preparó una **solicitud formal de datos** ([solicitud_datos_ciudad_del_saber.md](solicitud_datos_ciudad_del_saber.md)); no se registra envío ni respuesta.
- Mientras tanto se usan **datos sintéticos anclados en fuentes oficiales con fecha** ([fuentes_oficiales.md](fuentes_oficiales.md)). Si llegan datos reales, el pipeline los acepta con `--fuente real` sin cambiar el resto.

## 2. Cómo genera los datos el script
`generar_datos_fuente.py` (v3.0) lee **69 parámetros** de `parametros_generador.json`. Cada parámetro tiene mínimo, máximo, unidad, ancla (fuente real o *supuesto del equipo*) y una justificación. Semilla fija **202511**: mismos datos en cada corrida.

El perfil diario de cada edificio es el producto de: índice mensual real de EDEMET 2025 × temperatura media mensual real de Albrook × día laborable (calendario real de feriados 2025: 119 días no laborables, 246 laborables) × falla operativa inyectada × ruido. Salida: 4 formatos (CSV de medidores, JSON de sensores, XML de facturación, TXT de bitácora).

Defectos inyectados a propósito para ejercitar la limpieza: 53 nulos, 48 duplicados, 26 outliers negativos, 18 picos espurios y 110 fechas en formato mixto.

## 3. En qué se basan los rangos
- **Real y con fecha:** tarifas EDEMET MTD (ASEP), regla de factor de potencia, factor de emisión SIN 2021–2024 (MiAmbiente), huella 2021, áreas de 3 edificios (tesis TalTech 2018), clima de Albrook, feriados 2025, ventas EDEMET 2025.
- **Supuesto del equipo (sin fuente oficial):** EUI por tipo de edificio, % de consumo fuera de horario, factor de potencia base de cada edificio, COP/HVAC, tasas de defectos, porcentajes de ahorro y CAPEX. Están marcados como tales en `parametros_generador.json` y en `supuestos.json`.

## 4. Validaciones automáticas (pruebas)
- Todo valor generado queda dentro de su [mín, máx].
- Reproducibilidad: misma semilla → mismos bytes.
- **Plausibilidad:** consumo simulado 3,077.4 MWh = **0.889×** del tope de 3,462 MWh deducido de la huella de 2021 (450 tCO2e ÷ 0,13 tCO2e/MWh, factor SIN 2021). Es un **techo** porque no se publican los alcances. Rango aceptado: 0.5×–1.0×.
- Contraste (no es validación): el campus de la UTP consumió 10,772,377 kWh en 2024; el campus simulado equivale a 0.286× de ese valor.

## 5. Limitaciones que hay que decir
- Consumo de 2025 valorado con la tarifa vigente de 2026; factor de emisión 2024 aplicado a 2025; clima = climatología histórica, no el de 2025.
- Distribuidora = EDEMET es **probable** (inferencia geográfica de ASEP), no confirmada con factura.
- Edificios con nombre real (Ed. 128, 108, 202, 205, Plaza, Hospedaje) tienen **consumo ficticio**; la correspondencia número→nombre de las áreas de 2018 es una suposición.
- Los hallazgos describen el modelo, no el campus real. Cifras del modelo: consumo 3,077.40 MWh, costo 671,001.86 USD, emisiones 461.92 tCO2e.
