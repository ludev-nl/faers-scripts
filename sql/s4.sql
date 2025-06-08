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
DO $$
BEGIN
    -- Check if file exists
    IF NOT EXISTS (
        SELECT FROM pg_stat_file('/data/faers/FAERS_MAK/2.LoadDataToDatabase/reporter_countries.csv')
    ) THEN
        RAISE NOTICE 'File /data/faers/FAERS_MAK/2.LoadDataToDatabase/reporter_countries.csv does not exist, skipping country_mappings creation';
        RETURN;
    END IF;

    DROP TABLE IF EXISTS faers_combined.country_mappings;
    CREATE TABLE faers_combined.country_mappings (
        country_name VARCHAR(255) PRIMARY KEY,
        country_code VARCHAR(2)
    );

    \copy faers_combined.country_mappings(country_name, country_code) FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/reporter_countries.csv' WITH (FORMAT CSV, DELIMITER ',', HEADER true, NULL '');

    -- Clean up country_code
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
END $$;

-- Add and standardize Gender column
ALTER TABLE faers_combined."DEMO_Combined"
ADD COLUMN IF NOT EXISTS gender VARCHAR(3);

UPDATE faers_combined."DEMO_Combined"
SET gender = gndr_cod;

UPDATE faers_combined."DEMO_Combined"
SET gender = NULL
WHERE gender IN ('UNK', 'NS', 'YR');

-- Create ALIGNED_DEMO_DRUG_REAC_INDI_THER table
DO $$
DECLARE
    table_exists BOOLEAN;
BEGIN
    -- Check DEMO_Combined
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_combined') 
        AND relname = 'DEMO_Combined'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_combined.DEMO_Combined does not exist, creating empty ALIGNED_DEMO_DRUG_REAC_INDI_THER';
    END IF;

    -- Check DRUG_Combined
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_combined') 
        AND relname = 'DRUG_Combined'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_combined.DRUG_Combined does not exist, creating empty ALIGNED_DEMO_DRUG_REAC_INDI_THER';
    END IF;

    -- Check REAC_Combined
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_combined') 
        AND relname = 'REAC_Combined'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_combined.REAC_Combined does not exist, creating empty ALIGNED_DEMO_DRUG_REAC_INDI_THER';
    END IF;

    -- Check INDI_Combined
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_combined') 
        AND relname = 'INDI_Combined'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_combined.INDI_Combined does not exist, creating empty ALIGNED_DEMO_DRUG_REAC_INDI_THER';
    END IF;

    -- Check THER_Combined
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_combined') 
        AND relname = 'THER_Combined'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_combined.THER_Combined does not exist, creating empty ALIGNED_DEMO_DRUG_REAC_INDI_THER';
    END IF;
END $$;

-- Create ALIGNED_DEMO_DRUG_REAC_INDI_THER table structure
DROP TABLE IF EXISTS faers_combined."ALIGNED_DEMO_DRUG_REAC_INDI_THER";
CREATE TABLE faers_combined."ALIGNED_DEMO_DRUG_REAC_INDI_THER" (
    "primaryid" BIGINT,
    "caseid" BIGINT,
    age_years_fixed FLOAT,
    country_code VARCHAR(2),
    gender VARCHAR(3),
    "DRUG_ID" INTEGER,
    "drug_seq" BIGINT,
    "role_cod" VARCHAR(2),
    "drugname" TEXT,
    "prod_ai" TEXT,
    "nda_num" VARCHAR(200),
    reaction TEXT,
    reaction_meddra_code TEXT,
    indication TEXT,
    indication_meddra_code TEXT,
    therapy_start_date DATE,
    therapy_end_date DATE,
    reporting_period VARCHAR(10)
);

-- Populate table if data exists
INSERT INTO faers_combined."ALIGNED_DEMO_DRUG_REAC_INDI_THER"
SELECT DISTINCT
    d."primaryid",
    d."caseid",
    d.age_years_fixed,
    d.country_code,
    d.gender,
    dr."DRUG_ID",
    dr."drug_seq",
    dr."role_cod",
    dr."drugname",
    dr."prod_ai",
    dr."nda_num",
    r.pt AS reaction,
    r.meddra_code AS reaction_meddra_code,
    i.indi AS indication,
    i.meddra_code AS indication_meddra_code,
    t.start_dt AS therapy_start_date,
    t.end_dt AS therapy_end_date,
    dr."PERIOD" AS reporting_period
FROM faers_combined."DEMO_Combined" d
INNER JOIN faers_combined."DRUG_Combined" dr
    ON d."primaryid" = dr."primaryid"
INNER JOIN faers_combined."REAC_Combined" r
    ON d."primaryid" = r."primaryid"
INNER JOIN faers_combined."INDI_Combined" i
    ON d."primaryid" = i."primaryid"
INNER JOIN faers_combined."THER_Combined" t
    ON d."primaryid" = t."primaryid" AND dr."drug_seq" = t."drug_seq"
ON CONFLICT DO NOTHING;

-- Create indexes
CREATE INDEX IF NOT EXISTS "idx_aligned_primaryid" ON faers_combined."ALIGNED_DEMO_DRUG_REAC_INDI_THER" ("primaryid");
CREATE INDEX IF NOT EXISTS "idx_aligned_drug_id" ON faers_combined."ALIGNED_DEMO_DRUG_REAC_INDI_THER" ("DRUG_ID");