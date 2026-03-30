-- Notum SELECT til að sameina þrjár tegundir mælinga
SELECT
    allt.power_plant_source,
    allt.year,
    allt.month,
   t_type,
    allt.total_kwh allt.measuremen
FROM (
    -- Framleiðsla, söfnun á generated_pwr
    SELECT
        eu.name AS power_plant_source,
        EXTRACT(YEAR FROM psm.time) AS year,
        EXTRACT(MONTH FROM psm.time) AS month,
        'Framleiðsla' AS measurement_type,
        SUM(psm.generated_pwr) AS total_kwh
    FROM public.plant_sub_measurements AS psm
    JOIN public.pwr_plant AS pp ON psm.plant_id = pp.id
    JOIN public.energy_unit AS eu ON eu.id = pp.id
    WHERE psm.time >= DATE '2025-01-01'
      AND psm.time < DATE '2026-01-01'
    GROUP BY eu.name, year, month

    UNION ALL

    -- Innmötun, fengið frá received_pwr
    SELECT
        eu.name AS power_plant_source,
        EXTRACT(YEAR FROM psm.time) AS year,
        EXTRACT(MONTH FROM psm.time) AS month,
        'Innmötun' AS measurement_type,
        SUM(psm.received_pwr) AS total_kwh
    FROM public.plant_sub_measurements AS psm
    JOIN public.pwr_plant AS pp ON psm.plant_id = pp.id
    JOIN public.energy_unit AS eu ON eu.id = pp.id
    WHERE psm.time >= DATE '2025-01-01'
      AND psm.time < DATE '2026-01-01'
    GROUP BY eu.name, year, month

    UNION ALL

    -- Úttekt, tengjum notendamælingar við virkjun.
    SELECT
        eu.name AS power_plant_source,
        EXTRACT(YEAR FROM sub_um.time) AS year,
        EXTRACT(MONTH FROM sub_um.time) AS month,
        'Úttekt' AS measurement_type,
        SUM(sub_um.received_pwr) AS total_kwh
    FROM public.sub_user_measurements AS sub_um
    JOIN public.plant_substation_connection AS psc ON sub_um.substation_id = psc.substation_id
    JOIN public.pwr_plant AS pp ON psc.plant_id = pp.id
    JOIN public.energy_unit AS eu ON eu.id = pp.id
    WHERE sub_um.time >= DATE '2025-01-01'
      AND sub_um.time < DATE '2026-01-01'
    GROUP BY eu.name, year, month
) AS allt -- Gefum nöf allt fyrir öll gögnin
ORDER BY allt.power_plant_source, allt.year, allt.month, allt.total_kwh DESC;
