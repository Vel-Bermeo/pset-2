-- Docs: https://docs.mage.ai/guides/sql-blocksDROP TABLE IF EXISTS clean.dim_payment_type;

CREATE TABLE clean.dim_payment_type AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY payment_type) AS payment_type_key,
    payment_type
FROM clean.stg_trips_valid
WHERE payment_type IS NOT NULL;