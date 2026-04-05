CREATE UNLOGGED TABLE clean.stg_trips_valid AS
SELECT
    vendor_id,
    rate_code_id,
    pu_location_id,
    do_location_id,
    payment_type,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    source_year,
    source_month,
    source_file,
    EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0 AS trip_duration_minutes
FROM raw.ny_taxi_trips
WHERE tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND tpep_dropoff_datetime >= tpep_pickup_datetime
  AND COALESCE(trip_distance, 0) >= 0
  AND COALESCE(fare_amount, 0) >= 0
  AND COALESCE(total_amount, 0) >= 0
  AND COALESCE(passenger_count, 0) >= 0
  AND pu_location_id IS NOT NULL
  AND do_location_id IS NOT NULL;

SELECT COUNT(*) AS total_rows
FROM clean.stg_trips_valid;
