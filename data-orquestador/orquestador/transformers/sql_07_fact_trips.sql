DROP TABLE IF EXISTS clean.fact_trips;

CREATE TABLE clean.fact_trips AS
SELECT
    ROW_NUMBER() OVER (
        ORDER BY s.tpep_pickup_datetime, s.tpep_dropoff_datetime, s.pu_location_id, s.do_location_id
    ) AS trip_key,
    dv.vendor_key,
    dpt.payment_type_key,
    dpl.pickup_location_key,
    ddl.dropoff_location_key,
    s.rate_code_id,
    s.tpep_pickup_datetime,
    s.tpep_dropoff_datetime,
    s.trip_duration_minutes,
    s.passenger_count,
    s.trip_distance,
    s.fare_amount,
    s.extra,
    s.mta_tax,
    s.tip_amount,
    s.tolls_amount,
    s.improvement_surcharge,
    s.total_amount,
    s.congestion_surcharge,
    s.airport_fee,
    s.source_year,
    s.source_month,
    s.source_file
FROM clean.stg_trips_valid s
LEFT JOIN clean.dim_vendor dv
    ON s.vendor_id = dv.vendor_id
LEFT JOIN clean.dim_payment_type dpt
    ON s.payment_type = dpt.payment_type
LEFT JOIN clean.dim_pickup_location dpl
    ON s.pu_location_id = dpl.pu_location_id
LEFT JOIN clean.dim_dropoff_location ddl
    ON s.do_location_id = ddl.do_location_id;