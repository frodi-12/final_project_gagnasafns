-- Task C5

-- Views
DROP VIEW IF EXISTS public.energy_delivered CASCADE;
DROP VIEW IF EXISTS public.pwr_plant_production CASCADE;
DROP VIEW IF EXISTS public.energy_flow CASCADE;

CREATE VIEW public.energy_delivered AS
SELECT
    su.pwr_plant_id,
    EXTRACT(YEAR FROM su.time) AS year,
    EXTRACT(MONTH FROM su.time) AS month,
    SUM(su.pwr_measurement_kwh) AS delivered_pwr
FROM public.sub_user_measurements su
GROUP BY
    su.pwr_plant_id,
    EXTRACT(YEAR FROM su.time),
    EXTRACT(MONTH FROM su.time);

CREATE VIEW public.pwr_plant_production AS
SELECT 
    p.name,
    psm.plant_id,
    EXTRACT(YEAR FROM psm.time) AS year,
    EXTRACT(MONTH FROM psm.time) AS month,
    SUM(psm.pwr_measurement_kwh) FILTER (WHERE psm.type = 'Framleiðsla') AS total_production_kwh,
    SUM(psm.pwr_measurement_kwh) FILTER (WHERE psm.type = 'Innmötun') AS total_substation_pwr_kwh
FROM public.plant_sub_measurements psm
JOIN public.energy_unit p ON p.id = psm.plant_id
GROUP BY
    psm.plant_id,
    p.name,
    EXTRACT(YEAR FROM psm.time),
    EXTRACT(MONTH FROM psm.time);

CREATE VIEW public.energy_flow AS
SELECT
    ppp.name,
    ppp.plant_id,
    ppp.year,
    ppp.month,
    ppp.total_production_kwh,
    ppp.total_substation_pwr_kwh,
    ed.delivered_pwr
FROM public.pwr_plant_production ppp
LEFT JOIN public.energy_delivered ed
    ON ed.pwr_plant_id = ppp.plant_id
   AND ed.year = ppp.year
   AND ed.month = ppp.month;

SELECT
    name AS power_plant_source,
    year,
    month,
    'Framleiðsla' AS measurement_type,
    COALESCE(total_production_kwh, 0) AS total_kwh
FROM public.energy_flow

UNION ALL

SELECT
    name AS power_plant_source,
    year,
    month,
    'Innmötun' AS measurement_type,
    COALESCE(total_substation_pwr_kwh, 0) AS total_kwh
FROM public.energy_flow

UNION ALL

SELECT
    name AS power_plant_source,
    year,
    month,
    'Úttekt' AS measurement_type,
    COALESCE(delivered_pwr, 0) AS total_kwh
FROM public.energy_flow

ORDER BY power_plant_source, year, month, measurement_type;

-- Q2

SELECT
    name AS power_plant_source,
    year,
    month,
    'Úttekt' AS measurement_type,
    COALESCE(delivered_pwr, 0) AS total_kwh
FROM public.energy_flow
ORDER BY power_plant_source, year, month;





-- Q3

SELECT
    name,
    (SUM(total_production_kwh) - SUM(total_substation_pwr_kwh))
        / NULLIF(SUM(total_production_kwh), 0) AS plant_to_sub_loss_ratio,
    (SUM(total_production_kwh) - SUM(COALESCE(delivered_pwr, 0)))
        / NULLIF(SUM(total_production_kwh), 0) AS total_system_loss_ratio
FROM public.energy_flow
GROUP BY name;