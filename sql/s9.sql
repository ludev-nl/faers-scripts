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

-- Create placeholder IDD table if missing
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'IDD'
    ) THEN
        CREATE TABLE faers_b."IDD" (
            "DRUGNAME" TEXT,
            "RXAUI" VARCHAR(8)
        );
        RAISE NOTICE 'Created placeholder faers_b.IDD table';
    END IF;
END $$;

-- Add CLEANED_DRUGNAME and CLEANED_PROD_AI columns to DRUG_Mapper if missing
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'DRUG_Mapper' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b'))
        AND attname = 'CLEANED_DRUGNAME'
        AND NOT attisdropped
    ) THEN
        ALTER TABLE faers_b."DRUG_Mapper" ADD COLUMN "CLEANED_DRUGNAME" TEXT;
        RAISE NOTICE 'Added CLEANED_DRUGNAME column to faers_b.DRUG_Mapper';
    END IF;

    IF NOT EXISTS (
        SELECT FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'DRUG_Mapper' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b'))
        AND attname = 'CLEANED_PROD_AI'
        AND NOT attisdropped
    ) THEN
        ALTER TABLE faers_b."DRUG_Mapper" ADD COLUMN "CLEANED_PROD_AI" TEXT;
        RAISE NOTICE 'Added CLEANED_PROD_AI column to faers_b.DRUG_Mapper';
    END IF;
END $$;

-- Check if DRUG_Mapper exists and has required columns
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
    has_drugname BOOLEAN;
    has_prod_ai BOOLEAN;
    has_cleaned_drugname BOOLEAN;
    has_cleaned_prod_ai BOOLEAN;
    has_notes BOOLEAN;
    has_rxaui BOOLEAN;
    has_rxcui BOOLEAN;
    has_sab BOOLEAN;
    has_tty BOOLEAN;
    has_str BOOLEAN;
    has_code BOOLEAN;
BEGIN
    -- Check if DRUG_Mapper exists
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'DRUG_Mapper'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_b.DRUG_Mapper does not exist, skipping updates';
        RETURN;
    END IF;

    -- Check row count
    SELECT COUNT(*) INTO row_count FROM faers_b."DRUG_Mapper";
    IF row_count = 0 THEN
        RAISE NOTICE 'Table faers_b.DRUG_Mapper is empty, updates may have no effect';
    END IF;

    -- Check for required columns
    SELECT EXISTS (
        SELECT FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'DRUG_Mapper' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b'))
        AND attname = 'DRUGNAME'
        AND NOT attisdropped
    ) INTO has_drugname;

    SELECT EXISTS (
        SELECT FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'DRUG_Mapper' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b'))
        AND attname = 'prod_ai'
        AND NOT attisdropped
    ) INTO has_prod_ai;

    SELECT EXISTS (
        SELECT FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'DRUG_Mapper' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b'))
        AND attname = 'CLEANED_DRUGNAME'
        AND NOT attisdropped
    ) INTO has_cleaned_drugname;

    SELECT EXISTS (
        SELECT FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'DRUG_Mapper' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b'))
        AND attname = 'CLEANED_PROD_AI'
        AND NOT attisdropped
    ) INTO has_cleaned_prod_ai;

    SELECT EXISTS (
        SELECT FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'DRUG_Mapper' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b'))
        AND attname = 'NOTES'
        AND NOT attisdropped
    ) INTO has_notes;

    SELECT EXISTS (
        SELECT FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'DRUG_Mapper' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b'))
        AND attname = 'RXAUI'
        AND NOT attisdropped
    ) INTO has_rxaui;

    SELECT EXISTS (
        SELECT FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'DRUG_Mapper' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b'))
        AND attname = 'RXCUI'
        AND NOT attisdropped
    ) INTO has_rxcui;

    SELECT EXISTS (
        SELECT FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'DRUG_Mapper' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b'))
        AND attname = 'SAB'
        AND NOT attisdropped
    ) INTO has_sab;

    SELECT EXISTS (
        SELECT FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'DRUG_Mapper' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b'))
        AND attname = 'TTY'
        AND NOT attisdropped
    ) INTO has_tty;

    SELECT EXISTS (
        SELECT FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'DRUG_Mapper' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b'))
        AND attname = 'STR'
        AND NOT attisdropped
    ) INTO has_str;

    SELECT EXISTS (
        SELECT FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'DRUG_Mapper' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b'))
        AND attname = 'CODE'
        AND NOT attisdropped
    ) INTO has_code;

    IF NOT (has_drugname AND has_prod_ai AND has_cleaned_drugname AND has_cleaned_prod_ai AND has_notes AND has_rxaui AND has_rxcui AND has_sab AND has_tty AND has_str AND has_code) THEN
        RAISE NOTICE 'Required columns missing in faers_b.DRUG_Mapper, skipping updates';
        RETURN;
    END IF;
END $$;

-- Check if DRUG_Mapper_Temp exists
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
    has_drugname BOOLEAN;
    has_prod_ai BOOLEAN;
    has_cleaned_drugname BOOLEAN;
    has_cleaned_prod_ai BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'DRUG_Mapper_Temp'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_b.DRUG_Mapper_Temp does not exist, skipping updates from DRUG_Mapper_Temp';
        RETURN;
    END IF;

    -- Check row count
    SELECT COUNT(*) INTO row_count FROM faers_b."DRUG_Mapper_Temp";
    IF row_count = 0 THEN
        RAISE NOTICE 'Table faers_b.DRUG_Mapper_Temp is empty, updates may have no effect';
    END IF;

    -- Check for required columns
    SELECT EXISTS (
        SELECT FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'DRUG_Mapper_Temp' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b'))
        AND attname = 'DRUGNAME'
        AND NOT attisdropped
    ) INTO has_drugname;

    SELECT EXISTS (
        SELECT FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'DRUG_Mapper_Temp' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b'))
        AND attname = 'prod_ai'
        AND NOT attisdropped
    ) INTO has_prod_ai;

    SELECT EXISTS (
        SELECT FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'DRUG_Mapper_Temp' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b'))
        AND attname = 'CLEANED_DRUGNAME'
        AND NOT attisdropped
    ) INTO has_cleaned_drugname;

    SELECT EXISTS (
        SELECT FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'DRUG_Mapper_Temp' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b'))
        AND attname = 'CLEANED_PROD_AI'
        AND NOT attisdropped
    ) INTO has_cleaned_prod_ai;

    IF NOT (has_drugname AND has_prod_ai AND has_cleaned_drugname AND has_cleaned_prod_ai) THEN
        RAISE NOTICE 'Required columns missing in faers_b.DRUG_Mapper_Temp, skipping updates from DRUG_Mapper_Temp';
        RETURN;
    END IF;
END $$;

-- Update DRUG_Mapper with cleaned data from DRUG_Mapper_Temp
UPDATE faers_b."DRUG_Mapper"
SET "CLEANED_DRUGNAME" = dmt."CLEANED_DRUGNAME"
FROM faers_b."DRUG_Mapper_Temp" dmt
WHERE dmt."DRUGNAME" = faers_b."DRUG_Mapper"."DRUGNAME"
AND faers_b."DRUG_Mapper"."NOTES" IS NULL;

UPDATE faers_b."DRUG_Mapper"
SET "CLEANED_PROD_AI" = dmt."CLEANED_PROD_AI"
FROM faers_b."DRUG_Mapper_Temp" dmt
WHERE dmt."prod_ai" = faers_b."DRUG_Mapper"."prod_ai"
AND faers_b."DRUG_Mapper"."NOTES" IS NULL;

-- Check if RXNCONSO exists
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'RXNCONSO'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_b.RXNCONSO does not exist, skipping RXNCONSO updates';
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b."RXNCONSO";
    IF row_count = 0 THEN
        RAISE NOTICE 'Table faers_b.RXNCONSO is empty, RXNCONSO updates may have no effect';
    END IF;
END $$;

-- Update DRUG_Mapper with RXNCONSO mappings
-- 9.1: Match CLEANED_DRUGNAME with RXNORM (MIN, IN, PIN)
UPDATE faers_b."DRUG_Mapper"
SET 
    "RXAUI" = CAST(rxn."RXAUI" AS BIGINT),
    "RXCUI" = CAST(rxn."RXCUI" AS BIGINT),
    "NOTES" = '9.1',
    "SAB" = rxn."SAB",
    "TTY" = rxn."TTY",
    "STR" = rxn."STR",
    "CODE" = rxn."CODE"
FROM faers_b."RXNCONSO" rxn
WHERE rxn."STR" = faers_b."DRUG_Mapper"."CLEANED_DRUGNAME"
AND faers_b."DRUG_Mapper"."NOTES" IS NULL
AND rxn."SAB" = 'RXNORM'
AND rxn."TTY" IN ('MIN', 'IN', 'PIN');

-- 9.2: Match CLEANED_PROD_AI with RXNORM (MIN, IN, PIN)
UPDATE faers_b."DRUG_Mapper"
SET 
    "RXAUI" = CAST(rxn."RXAUI" AS BIGINT),
    "RXCUI" = CAST(rxn."RXCUI" AS BIGINT),
    "NOTES" = '9.2',
    "SAB" = rxn."SAB",
    "TTY" = rxn."TTY",
    "STR" = rxn."STR",
    "CODE" = rxn."CODE"
FROM faers_b."RXNCONSO" rxn
WHERE rxn."STR" = faers_b."DRUG_Mapper"."CLEANED_PROD_AI"
AND faers_b."DRUG_Mapper"."NOTES" IS NULL
AND rxn."SAB" = 'RXNORM'
AND rxn."TTY" IN ('MIN', 'IN', 'PIN');

-- 9.5: Match CLEANED_DRUGNAME with RXNORM (IN)
UPDATE faers_b."DRUG_Mapper"
SET 
    "RXAUI" = CAST(rxn."RXAUI" AS BIGINT),
    "RXCUI" = CAST(rxn."RXCUI" AS BIGINT),
    "NOTES" = '9.5',
    "SAB" = rxn."SAB",
    "TTY" = rxn."TTY",
    "STR" = rxn."STR",
    "CODE" = rxn."CODE"
FROM faers_b."RXNCONSO" rxn
WHERE rxn."STR" = faers_b."DRUG_Mapper"."CLEANED_DRUGNAME"
AND faers_b."DRUG_Mapper"."NOTES" IS NULL
AND rxn."TTY" = 'IN';

-- 9.6: Match CLEANED_PROD_AI with RXNORM (IN)
UPDATE faers_b."DRUG_Mapper"
SET 
    "RXAUI" = CAST(rxn."RXAUI" AS BIGINT),
    "RXCUI" = CAST(rxn."RXCUI" AS BIGINT),
    "NOTES" = '9.6',
    "SAB" = rxn."SAB",
    "TTY" = rxn."TTY",
    "STR" = rxn."STR",
    "CODE" = rxn."CODE"
FROM faers_b."RXNCONSO" rxn
WHERE rxn."STR" = faers_b."DRUG_Mapper"."CLEANED_PROD_AI"
AND faers_b."DRUG_Mapper"."NOTES" IS NULL
AND rxn."TTY" = 'IN';

-- 9.9: Match CLEANED_DRUGNAME with RXNORM (any TTY)
UPDATE faers_b."DRUG_Mapper"
SET 
    "RXAUI" = CAST(rxn."RXAUI" AS BIGINT),
    "RXCUI" = CAST(rxn."RXCUI" AS BIGINT),
    "NOTES" = '9.9',
    "SAB" = rxn."SAB",
    "TTY" = rxn."TTY",
    "STR" = rxn."STR",
    "CODE" = rxn."CODE"
FROM faers_b."RXNCONSO" rxn
WHERE rxn."STR" = faers_b."DRUG_Mapper"."CLEANED_DRUGNAME"
AND faers_b."DRUG_Mapper"."NOTES" IS NULL;

-- 9.10: Match CLEANED_PROD_AI with RXNORM (any TTY)
UPDATE faers_b."DRUG_Mapper"
SET 
    "RXAUI" = CAST(rxn."RXAUI" AS BIGINT),
    "RXCUI" = CAST(rxn."RXCUI" AS BIGINT),
    "NOTES" = '9.10',
    "SAB" = rxn."SAB",
    "TTY" = rxn."TTY",
    "STR" = rxn."STR",
    "CODE" = rxn."CODE"
FROM faers_b."RXNCONSO" rxn
WHERE rxn."STR" = faers_b."DRUG_Mapper"."CLEANED_PROD_AI"
AND faers_b."DRUG_Mapper"."NOTES" IS NULL;

-- IDD Updates
-- 9.3: Match CLEANED_DRUGNAME via IDD with RXNORM (MIN, IN, PIN)
UPDATE faers_b."DRUG_Mapper"
SET 
    "RXAUI" = CAST(rxn."RXAUI" AS BIGINT),
    "RXCUI" = CAST(rxn."RXCUI" AS BIGINT),
    "NOTES" = '9.3',
    "SAB" = rxn."SAB",
    "TTY" = rxn."TTY",
    "STR" = rxn."STR",
    "CODE" = rxn."CODE"
FROM faers_b."IDD" idd
INNER JOIN faers_b."RXNCONSO" rxn
    ON rxn."RXAUI" = idd."RXAUI"
WHERE idd."DRUGNAME" = faers_b."DRUG_Mapper"."CLEANED_DRUGNAME"
AND faers_b."DRUG_Mapper"."NOTES" IS NULL
AND rxn."SAB" = 'RXNORM'
AND rxn."TTY" IN ('MIN', 'IN', 'PIN');

-- 9.4: Match CLEANED_PROD_AI via IDD with RXNORM (MIN, IN, PIN)
UPDATE faers_b."DRUG_Mapper"
SET 
    "RXAUI" = CAST(rxn."RXAUI" AS BIGINT),
    "RXCUI" = CAST(rxn."RXCUI" AS BIGINT),
    "NOTES" = '9.4',
    "SAB" = rxn."SAB",
    "TTY" = rxn."TTY",
    "STR" = rxn."STR",
    "CODE" = rxn."CODE"
FROM faers_b."IDD" idd
INNER JOIN faers_b."RXNCONSO" rxn
    ON rxn."RXAUI" = idd."RXAUI"
WHERE idd."DRUGNAME" = faers_b."DRUG_Mapper"."CLEANED_PROD_AI"
AND faers_b."DRUG_Mapper"."NOTES" IS NULL
AND rxn."SAB" = 'RXNORM'
AND rxn."TTY" IN ('MIN', 'IN', 'PIN');

-- 9.7: Match CLEANED_DRUGNAME via IDD with RXNORM (IN)
UPDATE faers_b."DRUG_Mapper"
SET 
    "RXAUI" = CAST(rxn."RXAUI" AS BIGINT),
    "RXCUI" = CAST(rxn."RXCUI" AS BIGINT),
    "NOTES" = '9.7',
    "SAB" = rxn."SAB",
    "TTY" = rxn."TTY",
    "STR" = rxn."STR",
    "CODE" = rxn."CODE"
FROM faers_b."IDD" idd
INNER JOIN faers_b."RXNCONSO" rxn
    ON rxn."RXAUI" = idd."RXAUI"
WHERE idd."DRUGNAME" = faers_b."DRUG_Mapper"."CLEANED_DRUGNAME"
AND faers_b."DRUG_Mapper"."NOTES" IS NULL
AND rxn."TTY" = 'IN';

-- 9.8: Match CLEANED_PROD_AI via IDD with RXNORM (IN)
UPDATE faers_b."DRUG_Mapper"
SET 
    "RXAUI" = CAST(rxn."RXAUI" AS BIGINT),
    "RXCUI" = CAST(rxn."RXCUI" AS BIGINT),
    "NOTES" = '9.8',
    "SAB" = rxn."SAB",
    "TTY" = rxn."TTY",
    "STR" = rxn."STR",
    "CODE" = rxn."CODE"
FROM faers_b."IDD" idd
INNER JOIN faers_b."RXNCONSO" rxn
    ON rxn."RXAUI" = idd."RXAUI"
WHERE idd."DRUGNAME" = faers_b."DRUG_Mapper"."CLEANED_PROD_AI"
AND faers_b."DRUG_Mapper"."NOTES" IS NULL
AND rxn."TTY" = 'IN';

-- 9.11: Match CLEANED_DRUGNAME via IDD with RXNORM (any TTY)
UPDATE faers_b."DRUG_Mapper"
SET 
    "RXAUI" = CAST(rxn."RXAUI" AS BIGINT),
    "RXCUI" = CAST(rxn."RXCUI" AS BIGINT),
    "NOTES" = '9.11',
    "SAB" = rxn."SAB",
    "TTY" = rxn."TTY",
    "STR" = rxn."STR",
    "CODE" = rxn."CODE"
FROM faers_b."IDD" idd
INNER JOIN faers_b."RXNCONSO" rxn
    ON rxn."RXAUI" = idd."RXAUI"
WHERE idd."DRUGNAME" = faers_b."DRUG_Mapper"."CLEANED_DRUGNAME"
AND faers_b."DRUG_Mapper"."NOTES" IS NULL;

-- 9.12: Match CLEANED_PROD_AI via IDD with RXNORM (any TTY)
UPDATE faers_b."DRUG_Mapper"
SET 
    "RXAUI" = CAST(rxn."RXAUI" AS BIGINT),
    "RXCUI" = CAST(rxn."RXCUI" AS BIGINT),
    "NOTES" = '9.12',
    "SAB" = rxn."SAB",
    "TTY" = rxn."TTY",
    "STR" = rxn."STR",
    "CODE" = rxn."CODE"
FROM faers_b."IDD" idd
INNER JOIN faers_b."RXNCONSO" rxn
    ON rxn."RXAUI" = idd."RXAUI"
WHERE idd."DRUGNAME" = faers_b."DRUG_Mapper"."CLEANED_PROD_AI"
AND faers_b."DRUG_Mapper"."NOTES" IS NULL;