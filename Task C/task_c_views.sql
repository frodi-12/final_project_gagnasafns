-- General-purpose views for the normalized schema (Task C5 helper objects)

DROP VIEW IF EXISTS public.monthly_company_usage_view CASCADE;
DROP VIEW IF EXISTS public.energy_flow CASCADE;
DROP VIEW IF EXISTS public.substation_plant_map CASCADE;

CREATE OR REPLACE VIEW public.substation_plant_map AS
WITH RECURSIVE sub_map AS (
    SELECT
        psc.substation_id,
        psc.plant_id
    FROM public.plant_substation_connection AS psc
    UNION ALL
    SELECT
        ssc.receiving_station_id AS substation_id,
        sub_map.plant_id
    FROM public.substation_substation_connection AS ssc
    JOIN sub_map ON ssc.sending_station_id = sub_map.substation_id
)
SELECT DISTINCT substation_id, plant_id
FROM sub_map;

CREATE OR REPLACE VIEW public.energy_flow AS
WITH pwr_plant_monthly_totals AS (
    SELECT
        eu.id AS plant_id,
        eu.name AS plant_name,
        EXTRACT(YEAR FROM psm.time)::int AS year,
        EXTRACT(MONTH FROM psm.time)::int AS month,
        SUM(CASE WHEN psm.type ILIKE 'Framlei%' THEN psm.pwr_measurement_kwh ELSE 0 END) AS total_production_kwh,
        SUM(CASE WHEN psm.type ILIKE 'Innm%' THEN psm.pwr_measurement_kwh ELSE 0 END) AS total_substation_pwr_kwh
    FROM public.plant_sub_measurements AS psm
    JOIN public.energy_unit AS eu ON eu.id = psm.plant_id
    GROUP BY eu.id, eu.name, year, month
),
energy_delivered AS (
    SELECT
        spm.plant_id,
        EXTRACT(YEAR FROM sumu.time)::int AS year,
        EXTRACT(MONTH FROM sumu.time)::int AS month,
        SUM(sumu.pwr_measurement_kwh) AS delivered_pwr
    FROM public.sub_user_measurements AS sumu
    JOIN public.substation_plant_map AS spm
      ON sumu.substation_id = spm.substation_id
    GROUP BY spm.plant_id, year, month
)
SELECT
    ppt.plant_id,
    ppt.plant_name,
    ppt.year,
    ppt.month,
    ppt.total_production_kwh,
    ppt.total_substation_pwr_kwh,
    COALESCE(ed.delivered_pwr, 0) AS delivered_pwr
FROM pwr_plant_monthly_totals AS ppt
LEFT JOIN energy_delivered AS ed
  ON ed.plant_id = ppt.plant_id
 AND ed.year = ppt.year
 AND ed.month = ppt.month;

CREATE OR REPLACE VIEW public.monthly_company_usage_view AS
SELECT
    plants.name AS power_plant_source,
    ui.name AS customer_name,
    EXTRACT(YEAR FROM sumu.time)::int AS year,
    EXTRACT(MONTH FROM sumu.time)::int AS month,
    SUM(sumu.pwr_measurement_kwh) AS total_kwh
FROM public.sub_user_measurements AS sumu
JOIN public.substation_plant_map AS spm
  ON sumu.substation_id = spm.substation_id
JOIN public.energy_unit AS plants ON plants.id = spm.plant_id
JOIN public.energy_user AS eu ON eu.id = sumu.energy_user_id
JOIN public.user_info AS ui ON ui.kennitala = eu.kennitala
GROUP BY plants.name, ui.name, year, month;
