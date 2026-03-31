


WITH injection AS (
    -- hourly injection into substations from plants
    SELECT
        time,
        substation_id,
        SUM(pwr_measurement_kwh) AS injection_kwh
    FROM public.plant_sub_measurements
    WHERE type = 'Innmötun'
    GROUP BY time, substation_id
),

withdrawal AS (
    -- hourly withdrawal from substations to users
    SELECT
        time,
        substation_id,
        SUM(pwr_measurement_kwh) AS withdrawal_kwh
    FROM public.sub_user_measurements
    GROUP BY time, substation_id
),

system_balance AS (
    -- total injection and withdrawal per hour
    SELECT
        i.time,
        SUM(i.injection_kwh) AS total_injection,
        AVG(w.withdrawal_kwh) AS total_withdrawal
    FROM injection i
    JOIN withdrawal w
        ON i.time = w.time
    GROUP BY i.time
),

system_loss AS (
    -- hourly system loss
    SELECT
        time,
        total_injection - total_withdrawal AS loss_kwh
    FROM system_balance
),

total_distance AS (
    -- total cable distance in the network
    SELECT SUM(distance) AS total_distance
    FROM public.substation_substation_connection
),

loss_distribution AS (
    -- loss per connection per hour
    SELECT
        sl.time,
        ssc.sending_station_ID,
        ssc.receiving_station_ID,
        ssc.distance,
        (sl.loss_kwh / td.total_distance) * ssc.distance AS cable_loss_kwh
    FROM system_loss sl
    CROSS JOIN total_distance td
    JOIN public.substation_substation_connection ssc
        ON TRUE
),

flow_estimate AS (
    -- estimate flow based on injection at the sending station
    SELECT
        ld.time,
        ld.sending_station_ID,
        ld.receiving_station_ID,
        ld.cable_loss_kwh,
        COALESCE(i.injection_kwh,0) AS injection_kwh,
        COALESCE(i.injection_kwh,0) - ld.cable_loss_kwh AS flow_kwh
    FROM loss_distribution ld
    LEFT JOIN injection i
        ON i.substation_id = ld.sending_station_ID
        AND i.time = ld.time
)
INSERT INTO public.sub_sub_measurements
(
    sending_station_ID,
    receiving_station_ID,
    time,
    pwr_measurement_kwh,
    pwr_loss_kwh
)
SELECT
    sending_station_ID,
    receiving_station_ID,
    time,
    flow_kwh,
    cable_loss_kwh
FROM flow_estimate;

