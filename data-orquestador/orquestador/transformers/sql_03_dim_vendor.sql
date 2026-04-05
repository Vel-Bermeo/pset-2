CREATE TABLE clean.dim_vendor AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY vendor_id) AS vendor_key,
    CAST(vendor_id AS INT) AS vendor_id
FROM clean.stg_trips_valid
WHERE vendor_id IS NOT NULL;