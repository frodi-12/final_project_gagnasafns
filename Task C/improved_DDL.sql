
CREATE TABLE energy_unit(
    ID INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    owner VARCHAR NOT NULL,
    construction_date DATE NOT NULL,
    x_cords FLOAT NOT NULL,
    y_cords FLOAT NOT NULL
);

CREATE TABLE pwr_plant(
    ID INTEGER PRIMARY KEY,
    type VARCHAR NOT NULL,
    FOREIGN KEY (ID) REFERENCES energy_unit(ID)
);

CREATE TABLE substation(
    ID INTEGER PRIMARY KEY,
    FOREIGN KEY (ID) REFERENCES energy_unit(ID)
);

CREATE TABLE plant_substation_transport(
    plant_ID INTEGER,
    substation_ID INTEGER,
    time TIMESTAMP NOT NULL,
    generated_pwr_kwh FLOAT NOT NULL,
    received_pwr_kwh FLOAT NOT NULL,
    FOREIGN KEY (plant_ID) REFERENCES pwr_plant(ID),
    FOREIGN KEY (substation_ID) REFERENCES substation(ID),
    PRIMARY KEY (plant_ID, substation_ID, time)
);

CREATE TABLE substation_substation_transport(
    sending_station_ID INTEGER,
    receiving_station_ID INTEGER,
    time TIMESTAMP NOT NULL,
    sent_pwr_kwh FLOAT NOT NULL,
    received_pwr_kwh FLOAT NOT NULL,
    FOREIGN KEY (sending_station_ID) REFERENCES substation(ID),
    FOREIGN KEY (receiving_station_ID) REFERENCES substation(ID),
    PRIMARY KEY (sending_station_ID, receiving_station_ID, time)
);

CREATE TABLE energy_user(
    kennitala INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    owner VARCHAR NOT NULL,
    year_founded DATE NOT NULL,
    x_cords FLOAT NOT NULL,
    y_cords FLOAT NOT NULL
);

CREATE TABLE substation_user_transport(
    substation_ID INTEGER,
    energy_user_kennitala INTEGER,
    received_pwr_kwh FLOAT NOT NULL,
    sent_pwr_kwh FLOAT NOT NULL,
    time TIMESTAMP NOT NULL,
    FOREIGN KEY (substation_ID) REFERENCES substation(ID),
    FOREIGN KEY (energy_user_kennitala) REFERENCES energy_user(kennitala),
    PRIMARY KEY (substation_ID, energy_user_kennitala, time)
);