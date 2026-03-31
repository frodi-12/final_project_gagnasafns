-- Einföld samantekt á úttektum með energy_flow view (sjá query_A3_new)
SELECT
    name AS power_plant_source,
    2025 AS year,
    month,
    'Úttekt' AS measurement_type,
    CASE
        WHEN delivered_pwr IS NULL THEN 0
        ELSE delivered_pwr
    END AS total_kwh
FROM public.energy_flow
ORDER BY power_plant_source, month;
