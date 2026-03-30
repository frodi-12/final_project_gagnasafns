-- Query 1
SELECT
    OM.eining_heiti AS power_plant_source,
    EXTRACT(YEAR FROM OM.timi) AS year,
    EXTRACT(MONTH FROM OM.timi) AS month,
    OM.tegund_maelingar AS measurement_type,
    SUM(OM.gildi_kwh) AS total_kwh
FROM raforka_legacy.orku_maelingar OM
JOIN raforka_legacy.orku_einingar OE ON OM.eining_heiti = OE.heiti
WHERE
    OE.tegund = 'virkjun'  
    AND OM.timi >= '2025-01-01'
    AND OM.timi < '2026-01-01'
GROUP BY OM.eining_heiti, year, month, OM.tegund_maelingar
ORDER BY OM.eining_heiti, month ASC, total_kwh DESC;
