SELECT 
    psm.plant_ID AS power_plant_source,
    (SUM(psm.generated_pwr) - SUM(psm.received_pwr)) / SUM(psm.generated_pwr) AS plant_to_substation_loss_ratio,
    (SUM(psm.generated_pwr) - (SELECT SUM(sumo.received_pwr) FROM public.sub_user_measurements sumo)) / SUM(psm.generated_pwr) AS total_system_loss_ratio
FROM public.plant_sub_measurements psm
GROUP BY psm.plant_ID;