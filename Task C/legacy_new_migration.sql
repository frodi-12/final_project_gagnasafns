
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

INSERT INTO public.plant_substation_connection (plant_id, substation_id)
SELECT plant.id, sub.id 
FROM raforka_legacy.orku_einingar plant
JOIN raforka_legacy.orku_einingar sub ON sub.heiti = plant.tengd_stod
WHERE plant.tegund ILIKE 'virkjun'

SELECT * FROM raforka_legacy.orku_einingar
