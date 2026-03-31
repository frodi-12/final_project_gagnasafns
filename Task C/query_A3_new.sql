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
SELECT name,
    (SUM(total_production_kwh)-SUM(total_substation_pwr_kwh))/SUM(total_production_kwh) AS plant_to_sub_loss_ratio,
    (SUM(total_production_kwh)-SUM(delivered_pwr))/SUM(total_production_kwh) AS total_system_loss_ratio
FROM public.energy_flow
GROUP BY name