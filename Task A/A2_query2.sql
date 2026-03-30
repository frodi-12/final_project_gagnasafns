
--query 2
SELECT
    eining_heiti AS power_plant_source,
    EXTRACT(YEAR FROM timi)::int AS year,
    EXTRACT(MONTH FROM timi)::int AS month,
    notandi_heiti AS customer_name,
    SUM(gildi_kwh) AS total_kwh
FROM raforka_legacy.orku_maelingar 
WHERE timi >= '2025-01-01'
  AND timi <  '2026-01-01'
  AND tegund_maelingar = 'Úttekt'
GROUP BY
    eining_heiti,
    EXTRACT(MONTH FROM timi),
    EXTRACT(YEAR FROM timi),
    notandi_heiti
ORDER BY
    power_plant_source,
    year,
    month,
    customer_name;