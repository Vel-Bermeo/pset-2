DROP TABLE IF EXISTS clean.dim_pickup_location;

CREATE TABLE clean.dim_pickup_location AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY pu_location_id) AS pickup_location_key,
    pu_location_id
FROM clean.stg_trips_valid
WHERE pu_location_id IS NOT NULL;
