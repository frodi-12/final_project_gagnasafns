CREATE OR REPLACE VIEW monthly_loss_view AS
SELECT 
    eu.name AS power_plant_source,
    EXTRACT(YEAR FROM psm.time) AS year,
    EXTRACT(MONTH FROM psm.time) AS month,
    SUM(CASE WHEN psm.type ILIKE 'Framleiðsla' THEN psm.pwr_measurement_kwh END) AS framleit_pwr,
    SUM(CASE WHEN psm.type ILIKE 'Innmötun' THEN psm.pwr_measurement_kwh END) AS innmotun_pwr,
    SUM(sum2.pwr_measurement_kwh) AS uttekt_pwr
FROM plant_sub_measurements psm
JOIN pwr_plant pp ON psm.plant_ID = pp.ID
JOIN energy_unit eu ON pp.ID = eu.ID
LEFT JOIN substation_substation_connection ssc ON ssc.sending_station_ID = psm.substation_ID
LEFT JOIN sub_user_measurements sum2 ON sum2.substation_ID = ssc.receiving_station_ID
    AND EXTRACT(MONTH FROM psm.time) = EXTRACT(MONTH FROM sum2.time)
    AND EXTRACT(YEAR FROM psm.time) = EXTRACT(YEAR FROM sum2.time)
GROUP BY eu.name, EXTRACT(YEAR FROM psm.time), EXTRACT(MONTH FROM psm.time);

SELECT
    power_plant_source,
    AVG((framleit_pwr - innmotun_pwr) / framleit_pwr) AS plant_to_sub_loss_ratio,
    AVG((framleit_pwr - uttekt_pwr) / framleit_pwr) AS total_system_loss_ratio
FROM monthly_loss_view
GROUP BY power_plant_source;

SELECT 
    psm.plant_ID AS power_plant_source,
    (SUM(psm.generated_pwr) - SUM(psm.received_pwr)) / SUM(psm.generated_pwr) AS plant_to_substation_loss_ratio,
    (SUM(psm.generated_pwr) - (SELECT SUM(sum.received_pwr) FROM public.sub_user_measurements sum)) / SUM(psm.generated_pwr) AS total_system_loss_ratio
FROM public.plant_sub_measurements psm
GROUP BY psm.plant_ID;

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

SELECT * 
FROM public.energy_flow



