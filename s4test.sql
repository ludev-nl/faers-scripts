-- Set schema search path
SET search_path TO faers_combined, public;

-- STANDARDIZE DEMO_Combined AGE FIELD TO YEARS
ALTER TABLE faers_combined."DEMO_Combined"
ADD COLUMN IF NOT EXISTS age_years_fixed FLOAT;

WITH cte AS (
    SELECT
        "DEMO_ID",
        age,
        age_cod,
        CASE
            WHEN age_cod = 'DEC' THEN ROUND(CAST(age AS NUMERIC) * 12, 2)
            WHEN age_cod IN ('YR', 'YEAR') THEN ROUND(CAST(age AS NUMERIC), 2)
            WHEN age_cod = 'MON' THEN ROUND(CAST(age AS NUMERIC) / 12, 2)
            WHEN age_cod IN ('WK', 'WEEK') THEN ROUND(CAST(age AS NUMERIC) / 52, 2)
            WHEN age_cod IN ('DY', 'DAY') THEN ROUND(CAST(age AS NUMERIC) / 365, 2)
            WHEN age_cod IN ('HR', 'HOUR') THEN ROUND(CAST(age AS NUMERIC) / 8760, 2)
            ELSE NULL
        END AS age_years_fixed
    FROM faers_combined."DEMO_Combined"
    WHERE age ~ '^[0-9]+(\.[0-9]+)?$' -- ISNUMERIC check
)
UPDATE faers_combined."DEMO_Combined"
SET age_years_fixed = cte.age_years_fixed
FROM cte
WHERE faers_combined."DEMO_Combined"."DEMO_ID" = cte."DEMO_ID";

-- Add COUNTRY_CODE column
ALTER TABLE faers_combined."DEMO_Combined"
ADD COLUMN IF NOT EXISTS country_code VARCHAR(2);

-- Update COUNTRY_CODE using CSV-based country mappings
DROP TABLE IF EXISTS faers_combined.country_mappings;
CREATE TABLE faers_combined.country_mappings (
    country_name VARCHAR(255) PRIMARY KEY,
    country_code VARCHAR(2)
);

\copy faers_combined.country_mappings(country_name, country_code) FROM '"\Users\xocas\OneDrive\Desktop\faers-scripts\faers_data\reporter_countries.csv"' WITH (FORMAT CSV, DELIMITER ',', HEADER true, NULL '');

-- Clean up country_code to ensure valid values
UPDATE faers_combined.country_mappings
SET country_code = NULL
WHERE country_code = '';

-- Update DEMO_Combined country_code field
UPDATE faers_combined."DEMO_Combined"
SET country_code = (
    SELECT m.country_code
    FROM faers_combined.country_mappings m
    WHERE faers_combined."DEMO_Combined".reporter_country = m.country_name
)
WHERE country_code IS NULL;

-- If reporter_country is already a 2-character code, retain it
UPDATE faers_combined."DEMO_Combined"
SET country_code = reporter_country
WHERE LENGTH(reporter_country) = 2 AND country_code IS NULL;

-- Add and standardize Gender column
ALTER TABLE faers_combined."DEMO_Combined"
ADD COLUMN IF NOT EXISTS gender VARCHAR(3);

UPDATE faers_combined."DEMO_Combined"
SET gender = gndr_cod;

UPDATE faers_combined."DEMO_Combined"
SET gender = NULL
WHERE gender IN ('UNK', 'NS', 'YR');