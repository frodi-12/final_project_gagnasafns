-- Nýtum energy_flow view beint til að sýna þrjár tegundir mælinga árið 2025
SELECT
    name AS power_plant_source,
    year,
    month,
    'Framleiðsla' AS measurement_type,
    CASE WHEN total_production_kwh IS NULL THEN 0 ELSE total_production_kwh END AS total_kwh
FROM public.energy_flow
WHERE year = 2025

UNION ALL

SELECT
    name AS power_plant_source,
    year,
    month,
    'Innmötun' AS measurement_type,
    CASE WHEN total_substation_pwr_kwh IS NULL THEN 0 ELSE total_substation_pwr_kwh END AS total_kwh
FROM public.energy_flow
WHERE year = 2025

UNION ALL

SELECT
    name AS power_plant_source,
    year,
    month,
    'Úttekt' AS measurement_type,
    CASE WHEN delivered_pwr IS NULL THEN 0 ELSE delivered_pwr END AS total_kwh
FROM public.energy_flow
WHERE year = 2025

ORDER BY power_plant_source, year, month, total_kwh DESC;
