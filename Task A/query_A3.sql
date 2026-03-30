
SELECT x.eining_heiti AS power_plant_source, (x.framleit_pwr-x.innmotun_pwr)/x.framleit_pwr AS Plant_to_sub_loss_ratio, (x.framleit_pwr-x.uttekt_pwr)/x.framleit_pwr AS total_system_loss_ratio
FROM (
SELECT 
        om.eining_heiti,
        SUM(CASE WHEN om.tegund_maelingar ILIKE 'Framleiðsla' THEN gildi_kwh END) AS framleit_pwr,
        SUM(CASE WHEN om.tegund_maelingar ILIKE 'Innmötun' THEN gildi_kwh END) AS innmotun_pwr,
        SUM(CASE WHEN om.tegund_maelingar ILIKE 'Úttekt' THEN gildi_kwh END) AS uttekt_pwr
    FROM raforka_legacy.orku_maelingar om
    GROUP BY om.eining_heiti
) AS x;