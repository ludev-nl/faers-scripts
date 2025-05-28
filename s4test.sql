--STANDARDIZE DEMO_Combined AGE FILED TO YEARS

ALTER TABLE DEMO_Combined
ADD COLUMN IF NOT EXISTS age_years_fixed FLOAT;


WITH cte AS (
  SELECT
    demo_id,
    age,
    age_cod,
    CASE
      WHEN age_cod = 'DEC' THEN ROUND(CAST(age AS FLOAT) * 12, 2)
      WHEN age_cod = 'YR' THEN ROUND(age, 2)
      WHEN age_cod = 'YEAR' THEN ROUND(age, 2)
      WHEN age_cod = 'MON' THEN ROUND(CAST(age AS FLOAT) / 12, 2)
      WHEN age_cod = 'WK' THEN ROUND(CAST(age AS FLOAT) / 52, 2)
      WHEN age_cod = 'WEEK' THEN ROUND(CAST(age AS FLOAT) / 52, 2)
      WHEN age_cod = 'DY' THEN ROUND(CAST(age AS FLOAT) / 365, 2)
      WHEN age_cod = 'DAY' THEN ROUND(CAST(age AS FLOAT) / 365, 2)
      WHEN age_cod = 'HR' THEN ROUND(CAST(age AS FLOAT) / 8760, 2)
      WHEN age_cod = 'HOUR' THEN ROUND(CAST(age AS FLOAT) / 8760, 2)
      ELSE age
    END AS age_years_fixed
  FROM demo_combined
  WHERE age ~ '^[0-9]+(\.[0-9]+)?$' -- ISNUMERIC check
)
UPDATE demo_combined
SET age_years_fixed = cte.age_years_fixed
FROM cte
WHERE demo_combined.demo_id = cte.demo_id;

-- Add COUNTRY_CODE column
ALTER TABLE DEMO_Combined
ADD COLUMN IF NOT EXISTS country_code VARCHAR(2);

-- Update COUNTRY_CODE using JSON-based country mappings
-- Load country mappings from JSON file (e.g., demo_country_mappings.json)
DROP TABLE IF EXISTS country_mappings;
CREATE TABLE IF NOT EXISTS country_mappings (
    country_name TEXT PRIMARY KEY,
    country_code TEXT
);

\copy country_mappings(country_name, country_code) FROM '../faers-data/reporter_countries.csv' CSV HEADER;
--COPY country_mappings(country_name, country_code) FROM 'reporter_countries.json' WITH (FORMAT JSON);

-- Update DEMO_Combined country_code field
UPDATE demo_combined
SET country_code = (
  SELECT m.country_code
  FROM country_mappings m
  WHERE demo_combined.reporter_country = m.country_name
)
WHERE country_code IS NULL;

-- If reporter_country is already a 2-character code, retain it
UPDATE demo_combined
SET country_code = reporter_country
WHERE LENGTH(reporter_country) = 2 AND country_code IS NULL;

-- Add and standardize Gender column
ALTER TABLE DEMO_Combined
ADD COLUMN IF NOT EXISTS gender VARCHAR(3);

UPDATE demo_combined
SET gender = sex;

UPDATE demo_combined
SET gender = NULL
WHERE gender IN ('UNK', 'NS', 'YR');
