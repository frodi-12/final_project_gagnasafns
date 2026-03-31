
drop view if exists monthly_plant_energy;
CREATE VIEW monthly_plant_energy AS
SELECT
    om.eining_heiti AS power_plant_source,
    EXTRACT(YEAR FROM om.timi) AS year,
    EXTRACT(MONTH FROM om.timi) AS month,
    SUM(CASE WHEN om.tegund_maelingar ILIKE 'Framleiðsla' THEN gildi_kwh END) AS total_framleidsla,
    SUM(CASE WHEN om.tegund_maelingar ILIKE 'Innmötun' THEN gildi_kwh END) AS total_innmotun,
    SUM(CASE WHEN om.tegund_maelingar ILIKE 'Úttekt' THEN gildi_kwh END) AS total_uttekt
FROM raforka_legacy.orku_maelingar om
WHERE EXTRACT(YEAR FROM om.timi) = 2025
GROUP BY om.eining_heiti, year, month;

SELECT
    power_plant_source,
    AVG((total_framleidsla - total_innmotun) / total_framleidsla) AS plant_to_sub_loss_ratio,
    AVG((total_framleidsla - total_uttekt) / total_framleidsla) AS total_system_loss_ratio
FROM monthly_plant_energy
GROUP BY power_plant_source
ORDER BY power_plant_source;