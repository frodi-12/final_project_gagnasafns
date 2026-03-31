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
SELECT ppp.name AS power_plant_source, ppp.month, ppp.total_production_kwh, ppp.total_substation_pwr_kwh, ed.delivered_pwr
FROM public.pwr_plant_production ppp
JOIN public.energy_unit eu ON eu.name = ppp.name
JOIN public.energy_delivered ed ON ed.pwr_plant_id = eu.id AND ed.month = ppp.month
ORDER BY ppp.name, ppp.month


--query a3
SELECT power_plant_source,
    AVG((total_production_kwh - total_substation_pwr_kwh)/ total_production_kwh) AS plant_to_sub_loss_ratio,
    AVG((total_production_kwh - delivered_pwr)/total_production_kwh) AS total_system_loss_ratio
FROM public.energy_flow
GROUP BY power_plant_source;

--query a2
SELECT eunit.name, EXTRACT(year FROM sume.time) AS year, EXTRACT(MONTH FROM sume.time) AS month, ui.owner AS customer_name, SUM(sume.pwr_measurement_kwh) AS total_kwh
FROM public.sub_user_measurements sume
JOIN public.energy_user eu ON eu.id = sume.energy_user_id
JOIN public.user_info ui ON ui.kennitala = eu.kennitala
JOIN public.energy_unit eunit ON eunit.id = sume.pwr_plant_id
GROUP BY eunit.name, ui.owner, EXTRACT(MONTH FROM sume.time), EXTRACT(year FROM sume.time)
ORDER BY eunit.name, EXTRACT(MONTH FROM sume.time), ui.owner ASC

--query a1

SELECT eu.name, EXTRACT(YEAR FROM psm.time) AS year, EXTRACT(MONTH FROM psm.time) AS month, psm."type" AS measurement_type, SUM(psm.pwr_measurement_kwh) AS total_kwh
FROM public.plant_sub_measurements psm
JOIN public.energy_unit eu ON eu.id = psm.plant_id
GROUP BY eu.name, EXTRACT(YEAR FROM psm.time), EXTRACT(MONTH FROM psm.time), psm.type
UNION
SELECT eu.name, EXTRACT(YEAR FROM sume.time) AS year, EXTRACT(MONTH FROM sume.time) AS month, 'Úttekt' AS measurement_type, SUM(sume.pwr_measurement_kwh) AS total_kwh
FROM public.sub_user_measurements sume
JOIN public.energy_unit eu ON eu.id = sume.pwr_plant_id
GROUP BY eu.name, EXTRACT(YEAR FROM sume.time), EXTRACT(MONTH FROM sume.time)
ORDER BY name, year, month, total_kwh DESC




