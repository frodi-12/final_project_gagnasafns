-- Task A2

-- Query 1
SELECT
    om.eining_heiti AS power_plant_source,
    EXTRACT(YEAR FROM om.timi)::int AS year,
    EXTRACT(MONTH FROM om.timi)::int AS month,
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

-- Query 2
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

DROP VIEW IF EXISTS monthly_plant_energy;

-- Query 3
CREATE VIEW monthly_plant_energy AS
SELECT
    om.eining_heiti AS power_plant_source,
    EXTRACT(YEAR FROM om.timi) AS year,
    EXTRACT(MONTH FROM om.timi) AS month,
    SUM(CASE WHEN om.tegund_maelingar ILIKE 'Framleiðsla' THEN gildi_kwh END) AS framleit_pwr,
    SUM(CASE WHEN om.tegund_maelingar ILIKE 'Innmötun' THEN gildi_kwh END) AS innmotun_pwr,
    SUM(CASE WHEN om.tegund_maelingar ILIKE 'Úttekt' THEN gildi_kwh END) AS uttekt_pwr
FROM raforka_legacy.orku_maelingar om
WHERE EXTRACT(YEAR FROM om.timi) = 2025
GROUP BY om.eining_heiti, year, month;

SELECT
    power_plant_source,
    AVG((framleit_pwr - innmotun_pwr) / framleit_pwr) AS plant_to_sub_loss_ratio,
    AVG((framleit_pwr - uttekt_pwr) / framleit_pwr) AS total_system_loss_ratio
FROM monthly_plant_energy
GROUP BY power_plant_source
ORDER BY power_plant_source;