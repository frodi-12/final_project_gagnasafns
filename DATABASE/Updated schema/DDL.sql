-- Task C3

DROP TABLE IF EXISTS public.user_info CASCADE;

DROP TABLE IF EXISTS public.sub_user_measurements CASCADE;

DROP TABLE IF EXISTS public.sub_sub_measurements CASCADE;

DROP TABLE IF EXISTS public.plant_sub_measurements CASCADE;

DROP TABLE IF EXISTS public.plant_substation_connection CASCADE;

DROP TABLE IF EXISTS public.substation_substation_connection CASCADE;

DROP TABLE IF EXISTS public.substation_user_connection CASCADE;

DROP TABLE IF EXISTS public.energy_user CASCADE;

DROP TABLE IF EXISTS public.substation CASCADE;

DROP TABLE IF EXISTS public.pwr_plant CASCADE;

DROP TABLE IF EXISTS public.energy_unit CASCADE;

CREATE TABLE public.energy_unit(
    ID INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    owner VARCHAR NOT NULL,
    construction_date DATE NOT NULL,
    x_cords FLOAT NOT NULL,
    y_cords FLOAT NOT NULL
);

CREATE TABLE public.pwr_plant(
    ID INTEGER PRIMARY KEY,
    type VARCHAR NOT NULL,
    FOREIGN KEY (ID) REFERENCES public.energy_unit(ID)
);

CREATE TABLE public.substation(
    ID INTEGER PRIMARY KEY,
    FOREIGN KEY (ID) REFERENCES public.energy_unit(ID)
);

CREATE TABLE public.plant_substation_connection(
    plant_ID INTEGER,
    substation_ID INTEGER,
    distance FLOAT NOT NULL,
    FOREIGN KEY (plant_ID) REFERENCES public.pwr_plant(ID),
    FOREIGN KEY (substation_ID) REFERENCES public.substation(ID),
    PRIMARY KEY (plant_ID, substation_ID)
);

CREATE TABLE public.substation_substation_connection(
    sending_station_ID INTEGER,
    receiving_station_ID INTEGER,
    distance FLOAT NOT NULL,
    FOREIGN KEY (sending_station_ID) REFERENCES public.substation(ID),
    FOREIGN KEY (receiving_station_ID) REFERENCES public.substation(ID),
    PRIMARY KEY (sending_station_ID, receiving_station_ID)
);

CREATE TABLE public.energy_user(
    ID INTEGER PRIMARY KEY,
    kennitala VARCHAR UNIQUE NOT NULL,
    x_cords FLOAT NOT NULL,
    y_cords FLOAT NOT NULL
);

CREATE TABLE public.user_info(
    kennitala VARCHAR PRIMARY KEY,
    founding_year INTEGER NOT NULL,
    name VARCHAR NOT NULL,
    owner VARCHAR NOT NULL,
    FOREIGN KEY (kennitala) REFERENCES public.energy_user(kennitala)
);

CREATE TABLE public.substation_user_connection(
    substation_ID INTEGER,
    energy_user_ID INTEGER,
    distance FLOAT NOT NULL,
    FOREIGN KEY (substation_ID) REFERENCES public.substation(ID),
    FOREIGN KEY (energy_user_ID) REFERENCES public.energy_user(ID),
    PRIMARY KEY (substation_ID, energy_user_ID)
);

CREATE TABLE public.plant_sub_measurements(
    ID INTEGER PRIMARY KEY,
    plant_ID INTEGER,
    substation_ID INTEGER,
    time TIMESTAMP NOT NULL,
    pwr_measurement_kwh FLOAT NOT NULL,
    type VARCHAR NOT NULL,
    FOREIGN KEY (plant_ID,substation_ID) REFERENCES public.plant_substation_connection(plant_ID, substation_ID)
);

CREATE TABLE public.sub_user_measurements(
    ID INTEGER PRIMARY KEY,
    substation_ID INTEGER,
    energy_user_ID INTEGER,
    time TIMESTAMP NOT NULL,
    pwr_measurement_kwh FLOAT NOT NULL,
    pwr_plant_id INTEGER NOT NULL,
    FOREIGN KEY (substation_ID, energy_user_ID) REFERENCES public.substation_user_connection(substation_ID, energy_user_ID)
);

CREATE TABLE public.sub_sub_measurements(
    sending_station_ID INTEGER,
    receiving_station_ID INTEGER,
    time TIMESTAMP NOT NULL,
    sent_pwr FLOAT NOT NULL,
    received_pwr FLOAT NOT NULL,
    FOREIGN KEY (sending_station_ID, receiving_station_ID) REFERENCES public.substation_substation_connection(sending_station_ID, receiving_station_ID)
);

-- Task D1


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