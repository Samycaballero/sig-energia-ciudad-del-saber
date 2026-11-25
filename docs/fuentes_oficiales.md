# Datos reales utilizados y sus fechas

Generado desde `codigo_y_dashboard/data/gold/tabla_fechas_datos.csv`. Todas las fuentes fueron consultadas el **2026-11-24**.
Estado: *confirmado* = se abrió la fuente; *con reservas* = hay una limitación indicada; *deducción* / *no confirmado* = no es un dato publicado directamente.

| Dato | Valor | Periodo del dato | Publicación | Estado | Fuente |
|---|---|---|---|---|---|
| tarifa_energia_edemet_mtd | 0.1557 | 2026-07 a 2030-06 (pliego vigente; los datos simulados son de 2025) | 2026-06-18 (fecha de la Res. AN No. 21781-Elec; publicacion del archivo no confirmada) | confirmado con reservas | https://asep.gob.pa/wp-content/uploads/electricidad/consultas_publicas/2026/cp_008-26/EDEMET/pliego_tarifario_edemet_jul26-Jun30.pdf |
| tarifa_demanda_edemet_mtd | 20.23 | 2026-07 a 2030-06 (pliego vigente; los datos simulados son de 2025) | 2026-06-18 (fecha de la Res. AN No. 21781-Elec; publicacion del archivo no confirmada) | confirmado con reservas | https://asep.gob.pa/wp-content/uploads/electricidad/consultas_publicas/2026/cp_008-26/EDEMET/pliego_tarifario_edemet_jul26-Jun30.pdf |
| cargo_fijo_edemet_mtd | 14.32 | 2026-07 a 2030-06 (pliego vigente; los datos simulados son de 2025) | 2026-06-18 (fecha de la Res. AN No. 21781-Elec; publicacion del archivo no confirmada) | confirmado con reservas | https://asep.gob.pa/wp-content/uploads/electricidad/consultas_publicas/2026/cp_008-26/EDEMET/pliego_tarifario_edemet_jul26-Jun30.pdf |
| factor_potencia_regla | {"umbral_fp_atraso": 0.9, "recargo_pct_por_cada_0_01_debajo": 2.0} | 2026-07 a 2030-06 (pliego vigente; los datos simulados son de 2025) | 2026-06-18 (fecha de la Res. AN No. 21781-Elec; publicacion del archivo no confirmada) | confirmado con reservas | https://asep.gob.pa/wp-content/uploads/electricidad/consultas_publicas/2026/cp_008-26/EDEMET/pliego_tarifario_edemet_jul26-Jun30.pdf |
| factor_emision_sin_2024 | 0.1501 | 2024 | 2025-08-21 (Gaceta Oficial 30348; Res. MIPRE-2025-0030765 del 2025-08-20) | confirmado | https://transparencia-climatica.miambiente.gob.pa/wp-content/uploads/2025/11/2024_FACTOR-DE-EMISION-DEL-SIN-PANAMA-2024.pdf |
| factor_emision_sin_2021 | 0.13 | 2021 | 2024-09 (archivo del documento MiAmbiente) | confirmado | https://transparencia-climatica.miambiente.gob.pa/wp-content/uploads/2024/09/Documento-Factores-de-Emision-y-Potenciales-de-Calentamiento-Global.pdf |
| factor_emision_sin_2022 | 0.1079 | 2022 | no indicada | confirmado con reservas | no indicada |
| factor_emision_sin_2023 | 0.1971 | 2023 | 2024-05 (SNE, mayo 2024) | confirmado con reservas | no indicada |
| huella_fundacion_2021 | 450 | 2021 | no indicada | confirmado con reservas | https://ciudaddelsaber.org/impacto/sostenibilidad |
| tope_escala_fundacion_2021 | 3462000 | 2021 | calculo del equipo (2026-11-24) | deduccion | no indicada |
| areas_cds_tesis_2018 | {"128": 3511, "108": 1027, "202": 704} | 2018 (Plan Maestro de CdS) | 2018 | confirmado con reservas | https://digikogu.taltech.ee/en/Download/b8ddd1a2-3dfd-45fd-9a9f-129e3e4bec4f |
| clima_albrook_etesa_eia_2023 | {"temperatura_media_mensual_c": [26.2, 26.8, 27.5, 28.0, 27.6, 27.1, 27.0, 27.3, 26.7, ... | serie historica de 66 anios (climatologia) | 2023-08-08 | confirmado con reservas | https://documentoesia.miambiente.gob.pa/91714.pdf |
| clima_weatherspark_albrook | {"tmax_c": [30.0, 31.7], "tmin_c": [24.4, 25.0]} | climatologia (sin periodo indicado) | no indicada | no confirmado | no indicada |
| feriados_2025_panama | ["2025-01-01", "2025-01-09", "2025-03-03", "2025-03-04", "2025-03-05", "2025-04-17", "2... | 2025 | 2025-01-31 (calendario Global Bank / Red ACH) | confirmado con reservas | https://globalbank.com.pa/sites/default/files/media/documentos/2025-02/calendario-de-dias-no-habiles-ano-2025.pdf |
| edemet_ventas_mensuales_2025 | [308.76, 322.05, 335.88, 343.05, 333.97, 328.31, 322.74, 333.84, 328.09, 322.08, 310.92... | 2025 (ene-dic) | PDF creados entre 2025-03-10 y 2026-01-20 | confirmado con reservas | https://asep.gob.pa/wp-content/uploads/electricidad/estadisticas_mensuales/2025/{MES}_2025.pdf |
| utp_campus_vls_2024 | [809352, 782108, 921891, 986923, 1012082, 964135, 974931, 1005527, 977275, 1001153, 787... | 2024 | 2025 | confirmado | https://utp.ac.pa/sites/default/files/documentos/2025/pdf/utp-emisiones-alcance2-tco2e-consumo-electrico-2024.pdf |
| utp_gei_2025_total | 15384602 | 2025 (Informe GEI 2025, tabla 1) | no indicada | confirmado | https://utp.ac.pa/sites/default/files/documentos/2026/pdf/01.informe_ghg_2025.pdf |
| precio_ocasional_2025 | 66.33 | 2025, 2o semestre | no indicada | confirmado | https://asep.gob.pa/wp-content/uploads/electricidad/estadisticas/2025/segundo_semestre/mercado.pdf |
| demanda_asep_2s_2025 | limite con ENSA: Corredor Sur, Via Ernesto T. Lefevre, Av. 12 de Octubre, Av. Simon Bol... | 2025, 2o semestre | PDF creado 2026-09-14 | confirmado con reservas | https://asep.gob.pa/wp-content/uploads/electricidad/estadisticas/2025/segundo_semestre/demanda.pdf |
| eui_literatura_climas_calidos | {"min": 47, "max": 628} | no indicado | 2021 | no confirmado | Energies 2021, 14(21), 7013 |
| mills_2011 | mediana 16 % edificios existentes (13 % obra nueva) | estudio de 2011 | 2011 | referencia de contexto | Mills, E. (2011). Energy Efficiency 4(2), 145-173. https://doi.org/10.1007/s12053-011-9116-8 |
| distribuidora_clayton_edemet | La distribuidora que atiende Clayton (Ciudad del Saber) es EDEMET | 2025, 2o semestre | PDF creado 2026-09-14 | probable, no confirmado | no indicada |
| correspondencia_numero_nombre_edificios | 128 = Casa de las Naciones Unidas, 108 = Casa Club, 202 = Clayton Bowling | 2018 | 2018 | suposicion del equipo | no indicada |
