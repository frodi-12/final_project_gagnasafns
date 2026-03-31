
CREATE INDEX idx_psm_time
ON public.plant_sub_measurements(time);

CREATE INDEX idx_psm_plant_time
ON public.plant_sub_measurements(plant_ID, time);

CREATE INDEX idx_psm_substation_time
ON public.plant_sub_measurements(substation_ID, time);

CREATE INDEX idx_psm_type
ON public.plant_sub_measurements(type);

CREATE INDEX idx_sum_time
ON public.sub_user_measurements(time);

CREATE INDEX idx_sum_substation_time
ON public.sub_user_measurements(substation_ID, time);

CREATE INDEX idx_sum_user_time
ON public.sub_user_measurements(energy_user_ID, time);

CREATE INDEX idx_ssm_time
ON public.sub_sub_measurements(time);

CREATE INDEX idx_ssm_sending_time
ON public.sub_sub_measurements(sending_station_ID, time);

CREATE INDEX idx_ssm_receiving_time
ON public.sub_sub_measurements(receiving_station_ID, time);

CREATE INDEX idx_psc_plant
ON public.plant_substation_connection(plant_ID);

CREATE INDEX idx_psc_substation
ON public.plant_substation_connection(substation_ID);

CREATE INDEX idx_ssc_send
ON public.substation_substation_connection(sending_station_ID);

CREATE INDEX idx_ssc_receive
ON public.substation_substation_connection(receiving_station_ID);

CREATE INDEX idx_suc_substation
ON public.substation_user_connection(substation_ID);

CREATE INDEX idx_suc_user
ON public.substation_user_connection(energy_user_ID);

CREATE INDEX idx_energy_unit_name
ON public.energy_unit(name);

CREATE INDEX idx_energy_user_kennitala
ON public.energy_user(kennitala);

CREATE INDEX idx_psm_plant_time_type
ON public.plant_sub_measurements(plant_ID, time, type);