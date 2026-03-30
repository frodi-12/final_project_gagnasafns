SELECT 
    x.name AS power_plant_source,
    AVG((x.framleit_pwr - x.innmotun_pwr) / x.framleit_pwr) AS plant_to_sub_loss_ratio,
    AVG((x.framleit_pwr - x.uttekt_pwr) / x.framleit_pwr) AS total_system_loss_ratio
FROM (
    SELECT 
        eu.name,
        EXTRACT(YEAR FROM psm.time) AS year,
        EXTRACT(MONTH FROM psm.time) AS month,
        SUM(CASE WHEN psm.type ILIKE 'Framleiðsla' THEN psm.pwr_measurement_kwh END) AS framleit_pwr,
        SUM(CASE WHEN psm.type ILIKE 'Innmötun' THEN psm.pwr_measurement_kwh END) AS innmotun_pwr,
        SUM(psm.pwr_measurement_kwh) AS uttekt_pwr
    FROM plant_sub_measurements psm
    JOIN pwr_plant pp ON psm.plant_ID = pp.ID
    JOIN energy_unit eu ON pp.ID = eu.ID
    GROUP BY eu.name, year, month
) x
GROUP BY x.name;