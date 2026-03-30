-- Query 1
SELECT
    om.eining_heiti AS power_plant_source,
    EXTRACT(YEAR FROM om.timi) AS year,
    EXTRACT(MONTH FROM om.timi) AS month,
    om.tegund_maelingar AS measurement_type,
    SUM(om.gildi_kwh) AS total_kwh
FROM raforka_legacy.orku_maelingar om
JOIN raforka_legacy.orku_einingar oe ON om.eining_heiti = oe.heiti
WHERE
    oe.tegund = 'virkjun'  
    AND om.timi >= '2025-01-01'
    AND om.timi < '2026-01-01'
GROUP BY om.eining_heiti, year, month, om.tegund_maelingar
ORDER BY om.eining_heiti, month ASC, total_kwh DESC;
