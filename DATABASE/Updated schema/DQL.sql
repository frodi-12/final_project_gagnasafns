-- Task C5

-- Views

CREATE VIEW public.energy_delivered AS
SELECT su.pwr_plant_id, EXTRACT(MONTH FROM su.time) AS month, SUM(su.pwr_measurement_kwh) AS delivered_pwr
FROM public.sub_user_measurements su
GROUP BY su.pwr_plant_id, EXTRACT(MONTH FROM su.time)


CREATE VIEW public.pwr_plant_production AS
SELECT 
    p.name,
    psm.plant_id AS plant_id,
    EXTRACT(MONTH FROM psm.time) AS month,
    SUM(psm.pwr_measurement_kwh) FILTER (WHERE psm.type = 'Framleiðsla') AS total_production_kwh,
    SUM(psm.pwr_measurement_kwh) FILTER (WHERE psm.type = 'Innmötun') AS total_substation_pwr_kwh
FROM public.plant_sub_measurements psm
JOIN public.energy_unit p ON p.id = psm.plant_id
GROUP BY psm.plant_id, p.name, EXTRACT(MONTH FROM psm.time)

CREATE VIEW energy_flow AS
SELECT ppp.name, ppp.month, ppp.total_production_kwh, ppp.total_substation_pwr_kwh, ed.delivered_pwr
FROM public.pwr_plant_production ppp
JOIN public.energy_unit eu ON eu.name = ppp.name
JOIN public.energy_delivered ed ON ed.pwr_plant_id = eu.id AND ed.month = ppp.month
ORDER BY ppp.name, ppp.month


-- Query 1

-- Sækjum gögn úr energy_flow (skilgreint sem view í query_A3_new)
SELECT
    name AS power_plant_source,
    2025 AS year,
    month,
    'Framleiðsla' AS measurement_type,
    CASE WHEN total_production_kwh IS NULL THEN 0 ELSE total_production_kwh END AS total_kwh
FROM public.energy_flow

UNION ALL

SELECT
    name AS power_plant_source,
    2025 AS year,
    month,
    'Innmötun' AS measurement_type,
    CASE WHEN total_substation_pwr_kwh IS NULL THEN 0 ELSE total_substation_pwr_kwh END AS total_kwh
FROM public.energy_flow

UNION ALL

SELECT
    name AS power_plant_source,
    2025 AS year,
    month,
    'Úttekt' AS measurement_type,
    CASE WHEN delivered_pwr IS NULL THEN 0 ELSE delivered_pwr END AS total_kwh
FROM public.energy_flow

ORDER BY power_plant_source, year, month, total_kwh DESC;

-- Query 2

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


-- Query 3

SELECT name,
    (SUM(total_production_kwh)-SUM(total_substation_pwr_kwh))/SUM(total_production_kwh) AS plant_to_sub_loss_ratio,
    (SUM(total_production_kwh)-SUM(delivered_pwr))/SUM(total_production_kwh) AS total_system_loss_ratio
FROM public.energy_flow
GROUP BY name



