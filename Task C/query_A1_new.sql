-- Sækjum gögn úr energy_flow (skilgreint sem view í query_A3_new)
SELECT
    name AS power_plant_source,
    2025 AS year,
    month,
    'Framleiðsla' AS measurement_type,
    CASE WHEN total_production_kwh IS NULL THEN 0 ELSE total_production_kwh END AS total_kwh
FROM public.energy_flow

UNION ALL

SELECT
    name AS power_plant_source,
    2025 AS year,
    month,
    'Innmötun' AS measurement_type,
    CASE WHEN total_substation_pwr_kwh IS NULL THEN 0 ELSE total_substation_pwr_kwh END AS total_kwh
FROM public.energy_flow

UNION ALL

SELECT
    name AS power_plant_source,
    2025 AS year,
    month,
    'Úttekt' AS measurement_type,
    CASE WHEN delivered_pwr IS NULL THEN 0 ELSE delivered_pwr END AS total_kwh
FROM public.energy_flow

ORDER BY power_plant_source, year, month, total_kwh DESC;
