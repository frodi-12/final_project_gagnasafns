-- Task C5 - Query 1: Monthly energy flow per power plant using the reusable energy_flow view.
SELECT
    plant_name AS power_plant_source,
    year,
    month,
    'Framleiðsla' AS measurement_type,
    COALESCE(total_production_kwh, 0) AS total_kwh
FROM public.energy_flow
WHERE year = 2025

UNION ALL

SELECT
    plant_name AS power_plant_source,
    year,
    month,
    'Innmötun' AS measurement_type,
    COALESCE(total_substation_pwr_kwh, 0) AS total_kwh
FROM public.energy_flow
WHERE year = 2025

UNION ALL

SELECT
    plant_name AS power_plant_source,
    year,
    month,
    'Úttekt' AS measurement_type,
    COALESCE(delivered_pwr, 0) AS total_kwh
FROM public.energy_flow
WHERE year = 2025

ORDER BY power_plant_source, year, month, total_kwh DESC;
