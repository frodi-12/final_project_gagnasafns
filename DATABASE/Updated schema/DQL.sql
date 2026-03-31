-- Task C5

-- Views

DROP VIEW IF EXISTS energy_flow CASCADE;
DROP VIEW IF EXISTS public.pwr_plant_production CASCADE;
DROP VIEW IF EXISTS public.energy_delivered CASCADE;
DROP VIEW IF EXISTS public.monthly_company_usage_view CASCADE;

CREATE VIEW public.energy_delivered AS
SELECT
    psc.plant_id AS pwr_plant_id,
    EXTRACT(YEAR FROM su.time)::int AS year,
    EXTRACT(MONTH FROM su.time)::int AS month,
    SUM(su.pwr_measurement_kwh) AS delivered_pwr
FROM public.sub_user_measurements su
JOIN public.plant_substation_connection psc
    ON su.substation_id = psc.substation_id
GROUP BY psc.plant_id, EXTRACT(YEAR FROM su.time)::int, EXTRACT(MONTH FROM su.time)::int;


CREATE VIEW public.pwr_plant_production AS
SELECT 
    p.name,
    psm.plant_id AS plant_id,
    EXTRACT(YEAR FROM psm.time)::int AS year,
    EXTRACT(MONTH FROM psm.time)::int AS month,
    SUM(psm.pwr_measurement_kwh) FILTER (WHERE psm.type = 'Framleiðsla') AS total_production_kwh,
    SUM(psm.pwr_measurement_kwh) FILTER (WHERE psm.type = 'Innmötun') AS total_substation_pwr_kwh
FROM public.plant_sub_measurements psm
JOIN public.energy_unit p ON p.id = psm.plant_id
GROUP BY psm.plant_id, p.name, EXTRACT(YEAR FROM psm.time)::int, EXTRACT(MONTH FROM psm.time)::int;

CREATE VIEW energy_flow AS
SELECT
    ppp.name,
    ppp.year,
    ppp.month,
    ppp.total_production_kwh,
    ppp.total_substation_pwr_kwh,
    CASE WHEN ed.delivered_pwr IS NULL THEN 0 ELSE ed.delivered_pwr END AS delivered_pwr
FROM public.pwr_plant_production ppp
LEFT JOIN public.energy_unit eu ON eu.name = ppp.name
LEFT JOIN public.energy_delivered ed
       ON ed.pwr_plant_id = eu.id
      AND ed.year = ppp.year
      AND ed.month = ppp.month
ORDER BY ppp.name, ppp.year, ppp.month;

CREATE VIEW public.monthly_company_usage_view AS
SELECT
    plants.name AS power_plant_source,
    ui.name AS customer_name,
    EXTRACT(YEAR FROM sumu.time)::int AS year,
    EXTRACT(MONTH FROM sumu.time)::int AS month,
    SUM(sumu.pwr_measurement_kwh) AS total_kwh
FROM public.sub_user_measurements AS sumu
JOIN public.energy_user AS eu ON sumu.energy_user_id = eu.id
JOIN public.user_info AS ui ON eu.kennitala = ui.kennitala
JOIN public.plant_substation_connection AS psc
    ON sumu.substation_id = psc.substation_id
JOIN public.energy_unit AS plants ON plants.id = psc.plant_id
GROUP BY
    plants.name,
    ui.name,
    EXTRACT(YEAR FROM sumu.time)::int,
    EXTRACT(MONTH FROM sumu.time)::int;


-- Query 1

-- Sækjum gögn úr energy_flow (skilgreint sem view í query_A3_new)
SELECT
    name AS power_plant_source,
    year,
    month,
    'Framleiðsla' AS measurement_type,
    CASE WHEN total_production_kwh IS NULL THEN 0 ELSE total_production_kwh END AS total_kwh
FROM public.energy_flow

UNION ALL

SELECT
    name AS power_plant_source,
    year,
    month,
    'Innmötun' AS measurement_type,
    CASE WHEN total_substation_pwr_kwh IS NULL THEN 0 ELSE total_substation_pwr_kwh END AS total_kwh
FROM public.energy_flow

UNION ALL

SELECT
    name AS power_plant_source,
    year,
    month,
    'Úttekt' AS measurement_type,
    CASE WHEN delivered_pwr IS NULL THEN 0 ELSE delivered_pwr END AS total_kwh
FROM public.energy_flow

ORDER BY power_plant_source, year, month, total_kwh DESC;

-- Query 2

-- Einföld samantekt á úttektum með energy_flow view (sjá query_A3_new)
SELECT
    name AS power_plant_source,
    year,
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



