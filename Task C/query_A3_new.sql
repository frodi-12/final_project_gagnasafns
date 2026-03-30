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
LEFT JOIN sub_user_measurements sum2 
    ON sum2.substation_ID = ssc.receiving_station_ID
    AND EXTRACT(MONTH FROM psm.time) = EXTRACT(MONTH FROM sum2.time)
    AND EXTRACT(YEAR FROM psm.time) = EXTRACT(YEAR FROM sum2.time)
GROUP BY eu.name, EXTRACT(YEAR FROM psm.time), EXTRACT(MONTH FROM psm.time);

SELECT
    power_plant_source,
    AVG((framleit_pwr - innmotun_pwr) / framleit_pwr) AS plant_to_sub_loss_ratio,
    AVG((framleit_pwr - uttekt_pwr) / framleit_pwr) AS total_system_loss_ratio
FROM monthly_loss_view
GROUP BY power_plant_source;



SELECT current_schema();
SELECT table_schema, table_name 
FROM information_schema.tables 
WHERE table_schema NOT IN ('pg_catalog', 'information_schema');