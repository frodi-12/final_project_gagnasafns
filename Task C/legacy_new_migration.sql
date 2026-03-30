
CREATE OR REPLACE FUNCTION public.cords_to_km(
    lat1 DOUBLE PRECISION,
    lon1 DOUBLE PRECISION,
    lat2 DOUBLE PRECISION,
    lon2 DOUBLE PRECISION
)
RETURNS DOUBLE PRECISION AS
$$
DECLARE
    earth_radius CONSTANT DOUBLE PRECISION := 6371; -- km
BEGIN
    RETURN earth_radius * acos(
        cos(radians(lat1)) * cos(radians(lat2)) *
        cos(radians(lon2) - radians(lon1)) +
        sin(radians(lat1)) * sin(radians(lat2))
    );
END;
$$
LANGUAGE plpgsql IMMUTABLE;

INSERT INTO public.energy_unit (id, name, owner, construction_date, x_cords, y_cords)
SELECT id, heiti, eigandi, MAKE_DATE(ar_uppsett, manudir_uppsett, dagur_uppsett), "X_HNIT", "Y_HNIT"
FROM raforka_legacy.orku_einingar;

INSERT INTO public.pwr_plant (id, type)
SELECT id, tegund_stod
FROM raforka_legacy.orku_einingar
WHERE tegund ILIKE 'virkjun'

INSERT INTO public.substation (id)
SELECT id
FROM raforka_legacy.orku_einingar
WHERE tegund ILIKE 'stod'

INSERT INTO public.energy_user (id, kennitala, x_cords, y_cords)
SELECT id, CAST(kennitala AS VARCHAR), "X_HNIT", "Y_HNIT"
FROM raforka_legacy.notendur_skraning

INSERT INTO public.user_info (kennitala, founding_year, name, owner)
SELECT kennitala, ar_stofnad, heiti, eigandi
FROM raforka_legacy.notendur_skraning

INSERT INTO public.plant_substation_connection (plant_id, substation_id, distance)
SELECT plant.id, sub.id, public.cords_to_km(plant."X_HNIT", plant."Y_HNIT", sub."X_HNIT", sub."Y_HNIT")
FROM raforka_legacy.orku_einingar plant
JOIN raforka_legacy.orku_einingar sub ON sub.heiti = plant.tengd_stod
WHERE plant.tegund ILIKE 'virkjun'

INSERT INTO public.substation_user_connection (substation_id, energy_user_id, distance)
SELECT DISTINCT o.id, n.id, public.cords_to_km(o."X_HNIT", o."Y_HNIT", n."X_HNIT", n."Y_HNIT")
FROM raforka_legacy.orku_maelingar m
JOIN raforka_legacy.notendur_skraning n ON n.eigandi = m.notandi_heiti
JOIN raforka_legacy.orku_einingar o ON o.heiti = m.sendandi_maelingar
WHERE m.notandi_heiti IS NOT NULL

INSERT INTO public.substation_substation_connection (sending_station_id, receiving_station_id, distance)
SELECT x.sub1_id, x.sub2_id, public.cords_to_km(s1."X_HNIT", s1."Y_HNIT", s2."X_HNIT", s2."Y_HNIT")
FROM (
    VALUES
        (1,2),
        (2,3)
) AS x(sub1_id,sub2_id)
JOIN raforka_legacy.orku_einingar s1 ON s1.id = x.sub1_id
JOIN raforka_legacy.orku_einingar s2 ON s2.id = x.sub2_id

INSERT INTO public.plant_sub_measurements (id, plant_id, substation_id, time, pwr_measurement_kwh, type)
SELECT om.id, 
    p.id, 
    (SELECT substation_id
    FROM public.plant_substation_connection s
    WHERE s.plant_id = p.id), 
    om.timi, 
    om.gildi_kwh, 
    om.tegund_maelingar
FROM raforka_legacy.orku_maelingar om
JOIN raforka_legacy.orku_einingar p ON p.heiti = om.eining_heiti
WHERE om.tegund_maelingar ILIKE ANY (ARRAY['Framleiðsla','Innmötun'])

INSERT INTO public.sub_user_measurements
SELECT om.id, s.substation_id, s.energy_user_id, om.timi, om.gildi_kwh
FROM raforka_legacy.orku_maelingar om
JOIN raforka_legacy.notendur_skraning u ON u.eigandi = om.notandi_heiti
JOIN public.substation_user_connection s ON s.energy_user_id = u.id

SELECT * FROM public.substation_user_connection

SELECT * FROM raforka_legacy.orku_maelingar
limit 50

SELECT * FROM raforka_legacy.orku_einingar

SELECT * FROM raforka_legacy.notendur_skraning


