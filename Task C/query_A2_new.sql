SELECT
    eu.id AS customer_id,
    ui.name AS customer_name,
    EXTRACT(YEAR FROM sum.time)::int AS year,
    EXTRACT(MONTH FROM sum.time)::int AS month,
    SUM(sum.received_pwr) AS total_kwh
FROM public.sub_user_measurements sum
JOIN public.energy_user eu 
    ON sum.energy_user_id = eu.id
JOIN public.user_info ui 
    ON eu.kennitala = ui.kennitala
WHERE sum.time >= '2025-01-01'
  AND sum.time <  '2026-01-01'
GROUP BY
    eu.id,
    ui.name,
    year,
    month
ORDER BY
    year,
    month,
    customer_name;