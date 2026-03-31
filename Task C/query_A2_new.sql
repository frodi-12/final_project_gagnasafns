-- Task C5 - Query 2: Monthly withdrawal per plant and customer using the reusable view.
SELECT
    power_plant_source,
    customer_name,
    year,
    month,
    total_kwh
FROM public.monthly_company_usage_view
WHERE year = 2025
ORDER BY power_plant_source, year, month, customer_name;
