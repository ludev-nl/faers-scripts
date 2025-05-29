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

-- Create DRUG_RxNorm_Mapping table
DROP TABLE IF EXISTS faers_b."DRUG_RxNorm_Mapping";
CREATE TABLE faers_b."DRUG_RxNorm_Mapping" (
    "MAPPING_ID" SERIAL PRIMARY KEY,
    "DRUG_ID" INTEGER,
    "primaryid" BIGINT,
    "DRUGNAME" TEXT NOT NULL,
    "RXCUI" VARCHAR(8),
    "RXAUI" VARCHAR(8),
    "STR" TEXT,
    "TTY" VARCHAR(20),
    "SAB" VARCHAR(20),
    "MAPPING_DATE" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY ("DRUG_ID") REFERENCES faers_b."DRUG_Mapper" ("DRUG_ID")
);

-- Check if source tables exist and have data
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    -- Check faers_b.DRUG_Mapper
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'DRUG_Mapper'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_b.DRUG_Mapper does not exist, skipping INSERT into DRUG_RxNorm_Mapping';
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b."DRUG_Mapper";
    IF row_count = 0 THEN
        RAISE NOTICE 'Table faers_b.DRUG_Mapper is empty, skipping INSERT into DRUG_RxNorm_Mapping';
        RETURN;
    END IF;

    -- Check faers_b.RXNCONSO
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'RXNCONSO'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_b.RXNCONSO does not exist, skipping INSERT into DRUG_RxNorm_Mapping';
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b."RXNCONSO";
    IF row_count = 0 THEN
        RAISE NOTICE 'Table faers_b.RXNCONSO is empty, skipping INSERT into DRUG_RxNorm_Mapping';
        RETURN;
    END IF;

    -- Check faers_combined.DRUG_Combined
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_combined') 
        AND relname = 'DRUG_Combined'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_combined.DRUG_Combined does not exist, skipping INSERT into DRUG_RxNorm_Mapping';
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_combined."DRUG_Combined";
    IF row_count = 0 THEN
        RAISE NOTICE 'Table faers_combined.DRUG_Combined is empty, skipping INSERT into DRUG_RxNorm_Mapping';
        RETURN;
    END IF;
END $$;

-- Insert data into DRUG_RxNorm_Mapping
INSERT INTO faers_b."DRUG_RxNorm_Mapping" (
    "DRUG_ID", "primaryid", "DRUGNAME", "RXCUI", "RXAUI", "STR", "TTY", "SAB"
)
SELECT 
    dm."DRUG_ID",
    dm."primaryid",
    dm."DRUGNAME",
    rc."RXCUI",
    rc."RXAUI",
    rc."STR",
    rc."TTY",
    rc."SAB"
FROM faers_b."DRUG_Mapper" dm
LEFT JOIN faers_b."RXNCONSO" rc
    ON UPPER(dm."DRUGNAME") = UPPER(rc."STR")
WHERE dm."primaryid" IN (
    SELECT "primaryid" FROM faers_combined."DRUG_Combined"
)
ON CONFLICT DO NOTHING;

-- Create indexes
CREATE INDEX IF NOT EXISTS "idx_drug_rxnorm_drug_id" ON faers_b."DRUG_RxNorm_Mapping" ("DRUG_ID");
CREATE INDEX IF NOT EXISTS "idx_drug_rxnorm_rxcui" ON faers_b."DRUG_RxNorm_Mapping" ("RXCUI");
CREATE INDEX IF NOT EXISTS "idx_drug_rxnorm_primaryid" ON faers_b."DRUG_RxNorm_Mapping" ("primaryid");

-- Optional: Load additional mapping data (example, commented out)
-- \copy faers_b."DRUG_RxNorm_Mapping" ("DRUG_ID", "RXCUI", "RXAUI", "STR", "TTY", "SAB") FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/mapping_data.csv' WITH (FORMAT CSV, DELIMITER ',', NULL '', HEADER TRUE);