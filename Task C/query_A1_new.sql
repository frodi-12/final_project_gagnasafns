-- Samanburður á framleiðslu, innmötun og úttekt árið 2025
WITH plant_measurements AS (
    -- Sækjum mælingar frá plöntum (framleiðsla og innmötun)
    SELECT
        eu.name AS power_plant_source,
        EXTRACT(YEAR FROM psm.time)::int AS year,
        EXTRACT(MONTH FROM psm.time)::int AS month,
        CASE
            WHEN psm.type ILIKE 'Framlei%' THEN 'Framleiðsla'
            WHEN psm.type ILIKE 'Innm%' THEN 'Innmötun'
        END AS measurement_type,
        SUM(psm.pwr_measurement_kwh) AS total_kwh
    FROM public.plant_sub_measurements AS psm
    JOIN public.pwr_plant AS pp ON psm.plant_id = pp.id
    JOIN public.energy_unit AS eu ON eu.id = pp.id
    WHERE psm.time >= DATE '2025-01-01'
      AND psm.time < DATE '2026-01-01'
      AND psm.type ILIKE ANY (ARRAY['Framlei%', 'Innm%'])
    GROUP BY eu.name, year, month, measurement_type
),
delivery_measurements AS (
    -- Sækjum úttektir frá notendum og speglum til virkjunar
    SELECT
        eu.name AS power_plant_source,
        EXTRACT(YEAR FROM sub_um.time)::int AS year,
        EXTRACT(MONTH FROM sub_um.time)::int AS month,
        'Úttekt' AS measurement_type,
        SUM(sub_um.pwr_measurement_kwh) AS total_kwh
    FROM public.sub_user_measurements AS sub_um
    JOIN public.plant_substation_connection AS psc ON sub_um.substation_id = psc.substation_id
    JOIN public.pwr_plant AS pp ON psc.plant_id = pp.id
    JOIN public.energy_unit AS eu ON eu.id = pp.id
    WHERE sub_um.time >= DATE '2025-01-01'
      AND sub_um.time < DATE '2026-01-01'
    GROUP BY eu.name, year, month
)
-- Sýnum allar mælingar saman í einföldu yfirliti
SELECT * FROM plant_measurements
UNION ALL
SELECT * FROM delivery_measurements
ORDER BY power_plant_source, year, month, total_kwh DESC;
