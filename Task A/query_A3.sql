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