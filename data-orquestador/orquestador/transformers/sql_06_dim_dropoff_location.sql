DROP TABLE IF EXISTS clean.dim_dropoff_location;

CREATE TABLE clean.dim_dropoff_location AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY do_location_id) AS dropoff_location_key,
    do_location_id
FROM clean.stg_trips_valid
WHERE do_location_id IS NOT NULL;