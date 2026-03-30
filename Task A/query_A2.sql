
--query 2
SELECT
    om.eining_heiti AS power_plant_source,
    EXTRACT(YEAR FROM om.timi)::int AS year,
    EXTRACT(MONTH FROM om.timi)::int AS month,
    om.notandi_heiti AS customer_name,
    SUM(om.gildi_kwh) AS total_kwh
FROM raforka_legacy.orku_maelingar om
WHERE om.timi >= '2025-01-01'
  AND om.timi <  '2026-01-01'
  AND om.tegund_maelingar = 'Úttekt'
GROUP BY om.eining_heiti, EXTRACT(MONTH FROM om.timi), EXTRACT(YEAR FROM om.timi), om.notandi_heiti
ORDER BY power_plant_source, year, month, customer_name;
