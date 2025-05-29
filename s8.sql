-- Verify database context
DO $$
BEGIN
    IF current_database() != 'faersdatabase' THEN
        RAISE EXCEPTION 'Must be connected to faersdatabase, current database is %', current_database();
    END IF;
END $$;

-- Ensure faers_b schema exists
CREATE SCHEMA IF NOT EXISTS faers_b AUTHORIZATION postgres;

-- Verify schema exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'faers_b') THEN
        RAISE EXCEPTION 'Schema faers_b failed to create';
    END IF;
END $$;

-- Grant privileges
GRANT ALL ON SCHEMA faers_b TO postgres;

-- Set search path
SET search_path TO faers_b, faers_combined, public;

-- Create function to clear numeric characters
CREATE OR REPLACE FUNCTION faers_b.clear_numeric_characters(input_string TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN REGEXP_REPLACE(COALESCE(input_string, ''), '[0-9]', '', 'g');
END;
$$ LANGUAGE plpgsql;

-- Create DRUG_Mapper_Temp table
DROP TABLE IF EXISTS faers_b."DRUG_Mapper_Temp";
CREATE TABLE faers_b."DRUG_Mapper_Temp" (
    "DRUGNAME" TEXT,
    "prod_ai" TEXT,
    "CLEANED_DRUGNAME" TEXT,
    "CLEANED_PROD_AI" TEXT
);

-- Check if DRUG_Mapper exists and has required columns
DO $$
DECLARE
    table_exists BOOLEAN;
    has_drugname BOOLEAN;
    has_prod_ai BOOLEAN;
BEGIN
    -- Check if table exists
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'DRUG_Mapper'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_b.DRUG_Mapper does not exist, skipping INSERT into DRUG_Mapper_Temp';
        RETURN;
    END IF;

    -- Check for DRUGNAME column
    SELECT EXISTS (
        SELECT FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'DRUG_Mapper' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b'))
        AND attname = 'DRUGNAME'
        AND NOT attisdropped
    ) INTO has_drugname;

    -- Check for prod_ai column
    SELECT EXISTS (
        SELECT FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'DRUG_Mapper' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b'))
        AND attname = 'prod_ai'
        AND NOT attisdropped
    ) INTO has_prod_ai;

    IF NOT has_drugname OR NOT has_prod_ai THEN
        RAISE NOTICE 'Required columns DRUGNAME or prod_ai missing in faers_b.DRUG_Mapper, skipping INSERT into DRUG_Mapper_Temp';
        RETURN;
    END IF;
END $$;

-- Populate DRUG_Mapper_Temp with distinct rows
INSERT INTO faers_b."DRUG_Mapper_Temp" ("DRUGNAME", "prod_ai", "CLEANED_DRUGNAME", "CLEANED_PROD_AI")
SELECT DISTINCT "DRUGNAME", "prod_ai", "DRUGNAME" AS "CLEANED_DRUGNAME", "prod_ai" AS "CLEANED_PROD_AI"
FROM faers_b."DRUG_Mapper"
WHERE "NOTES" IS NULL
ON CONFLICT DO NOTHING;

-- Clean CLEANED_DRUGNAME
-- Remove numeric characters
UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_DRUGNAME" = faers_b.clear_numeric_characters("CLEANED_DRUGNAME");

-- Remove whitespace and control characters
UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_DRUGNAME" = TRIM(REGEXP_REPLACE("CLEANED_DRUGNAME", E'[\n\r\t]', ''));

-- Remove special characters
UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_DRUGNAME" = TRIM(BOTH ' ":.,?/\`~!@#$%^&*-_=+' FROM "CLEANED_DRUGNAME");

-- Remove suffixes
UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_DRUGNAME" = LEFT("CLEANED_DRUGNAME", LENGTH("CLEANED_DRUGNAME") - 4)
WHERE RIGHT("CLEANED_DRUGNAME", 4) = ' NOS';

UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_DRUGNAME" = LEFT("CLEANED_DRUGNAME", LENGTH("CLEANED_DRUGNAME") - 4)
WHERE RIGHT("CLEANED_DRUGNAME", 4) = ' GEL';

UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_DRUGNAME" = LEFT("CLEANED_DRUGNAME", LENGTH("CLEANED_DRUGNAME") - 4)
WHERE RIGHT("CLEANED_DRUGNAME", 4) = ' CAP';

UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_DRUGNAME" = LEFT("CLEANED_DRUGNAME", LENGTH("CLEANED_DRUGNAME") - 5)
WHERE RIGHT("CLEANED_DRUGNAME", 5) = ' JELL';

UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_DRUGNAME" = LEFT("CLEANED_DRUGNAME", LENGTH("CLEANED_DRUGNAME") - 4)
WHERE RIGHT("CLEANED_DRUGNAME", 4) = ' TAB';

UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_DRUGNAME" = LEFT("CLEANED_DRUGNAME", LENGTH("CLEANED_DRUGNAME") - 4)
WHERE RIGHT("CLEANED_DRUGNAME", 4) = ' FOR';

UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_DRUGNAME" = LEFT("CLEANED_DRUGNAME", LENGTH("CLEANED_DRUGNAME") - 2)
WHERE RIGHT("CLEANED_DRUGNAME", 2) = '//';

UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_DRUGNAME" = LEFT("CLEANED_DRUGNAME", LENGTH("CLEANED_DRUGNAME") - 1)
WHERE RIGHT("CLEANED_DRUGNAME", 1) = '/';

-- Clean CLEANED_PROD_AI
-- Remove numeric characters
UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_PROD_AI" = faers_b.clear_numeric_characters("CLEANED_PROD_AI");

-- Remove whitespace and control characters
UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_PROD_AI" = TRIM(REGEXP_REPLACE("CLEANED_PROD_AI", E'[\n\r\t]', ''));

-- Remove special characters
UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_PROD_AI" = TRIM(BOTH ' ":.,?/\`~!@#$%^&*-_=+' FROM "CLEANED_PROD_AI");

-- Remove suffixes
UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_PROD_AI" = LEFT("CLEANED_PROD_AI", LENGTH("CLEANED_PROD_AI") - 4)
WHERE RIGHT("CLEANED_PROD_AI", 4) = ' NOS';

UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_PROD_AI" = LEFT("CLEANED_PROD_AI", LENGTH("CLEANED_PROD_AI") - 4)
WHERE RIGHT("CLEANED_PROD_AI", 4) = ' GEL';

UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_PROD_AI" = LEFT("CLEANED_PROD_AI", LENGTH("CLEANED_PROD_AI") - 4)
WHERE RIGHT("CLEANED_PROD_AI", 4) = ' CAP';

UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_PROD_AI" = LEFT("CLEANED_PROD_AI", LENGTH("CLEANED_PROD_AI") - 5)
WHERE RIGHT("CLEANED_PROD_AI", 5) = ' JELL';

UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_PROD_AI" = LEFT("CLEANED_PROD_AI", LENGTH("CLEANED_PROD_AI") - 4)
WHERE RIGHT("CLEANED_PROD_AI", 4) = ' TAB';

UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_PROD_AI" = LEFT("CLEANED_PROD_AI", LENGTH("CLEANED_PROD_AI") - 4)
WHERE RIGHT("CLEANED_PROD_AI", 4) = ' FOR';

UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_PROD_AI" = LEFT("CLEANED_PROD_AI", LENGTH("CLEANED_PROD_AI") - 2)
WHERE RIGHT("CLEANED_PROD_AI", 2) = '//';

UPDATE faers_b."DRUG_Mapper_Temp"
SET "CLEANED_PROD_AI" = LEFT("CLEANED_PROD_AI", LENGTH("CLEANED_PROD_AI") - 1)
WHERE RIGHT("CLEANED_PROD_AI", 1) = '/';