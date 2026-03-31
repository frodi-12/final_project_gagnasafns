-- Einföld samantekt á úttektum fyrir hvern viðskiptavin (sjá monthly_company_usage_view)
SELECT
    power_plant_source,
    customer_name,
    year,
    month,
    total_kwh
FROM public.monthly_company_usage_view
WHERE year = 2025
ORDER BY power_plant_source, year, month, customer_name;
