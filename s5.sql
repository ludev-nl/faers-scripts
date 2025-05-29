-- Verify database context
DO $$
BEGIN
    IF current_database() != 'faersdatabase' THEN
        RAISE EXCEPTION 'Must be connected to faersdatabase, current database is %', current_database();
    END IF;
END $$;

-- Set schema with explicit authorization
CREATE SCHEMA IF NOT EXISTS faers_b AUTHORIZATION postgres;

-- Verify schema exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'faers_b') THEN
        RAISE EXCEPTION 'Schema faers_b failed to create';
    END IF;
END $$;

-- Grant privileges to ensure access
GRANT ALL ON SCHEMA faers_b TO postgres;

-- Set search path
SET search_path TO faers_b, faers_combined, public;

-- Create DRUG_Mapper table
DROP TABLE IF EXISTS faers_b."DRUG_Mapper";
CREATE TABLE faers_b."DRUG_Mapper" (
    "DRUG_ID" INTEGER PRIMARY KEY,
    "primaryid" BIGINT,
    "caseid" BIGINT,
    "DRUG_SEQ" BIGINT,
    "ROLE_COD" VARCHAR(2),
    "PERIOD" VARCHAR(4),
    "DRUGNAME" TEXT,
    "prod_ai" TEXT,
    "NDA_NUM" VARCHAR(200),
    "NOTES" VARCHAR(100),
    "RXAUI" BIGINT,
    "RXCUI" BIGINT,
    "STR" TEXT,
    "SAB" VARCHAR(20),
    "TTY" VARCHAR(20),
    "CODE" VARCHAR(50),
    "remapping_NOTES" TEXT,
    "remapping_RXAUI" VARCHAR(8),
    "remapping_RXCUI" VARCHAR(8),
    "remapping_STR" TEXT,
    "remapping_SAB" VARCHAR(20),
    "remapping_TTY" VARCHAR(20),
    "remapping_CODE" VARCHAR(50),
    "id" SERIAL UNIQUE
);

-- Check if faers_combined.DRUG_Combined exists and has data
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    -- Check if table exists
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_combined') 
        AND relname = 'DRUG_Combined'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_combined.DRUG_Combined does not exist, skipping INSERT into DRUG_Mapper';
        RETURN;
    END IF;

    -- Check row count
    SELECT COUNT(*) INTO row_count FROM faers_combined."DRUG_Combined";
    IF row_count = 0 THEN
        RAISE NOTICE 'Table faers_combined.DRUG_Combined is empty, skipping INSERT into DRUG_Mapper';
        RETURN;
    END IF;

    -- Check if ALIGNED_DEMO_DRUG_REAC_INDI_THER exists
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_combined') 
        AND relname = 'ALIGNED_DEMO_DRUG_REAC_INDI_THER'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_combined.ALIGNED_DEMO_DRUG_REAC_INDI_THER does not exist, skipping INSERT into DRUG_Mapper';
        RETURN;
    END IF;

    -- Check row count for ALIGNED_DEMO_DRUG_REAC_INDI_THER
    SELECT COUNT(*) INTO row_count FROM faers_combined."ALIGNED_DEMO_DRUG_REAC_INDI_THER";
    IF row_count = 0 THEN
        RAISE NOTICE 'Table faers_combined.ALIGNED_DEMO_DRUG_REAC_INDI_THER is empty, skipping INSERT into DRUG_Mapper';
        RETURN;
    END IF;
END $$;

-- Insert data from faers_combined.DRUG_Combined (only if checks pass)
INSERT INTO faers_b."DRUG_Mapper" (
    "DRUG_ID", "primaryid", "caseid", "DRUG_SEQ", "ROLE_COD", "DRUGNAME", "prod_ai", "NDA_NUM", "PERIOD"
)
SELECT "DRUG_ID", "primaryid", "caseid", "drug_seq", "role_cod", "drugname", "prod_ai", "nda_num", "PERIOD"
FROM faers_combined."DRUG_Combined"
WHERE "primaryid" IN (SELECT "primaryid" FROM faers_combined."ALIGNED_DEMO_DRUG_REAC_INDI_THER")
ON CONFLICT ON CONSTRAINT "DRUG_Mapper_pkey" DO NOTHING;

-- Create RxNorm tables
DROP TABLE IF EXISTS faers_b."RXNATOMARCHIVE";
CREATE TABLE faers_b."RXNATOMARCHIVE" (
    "RXAUI" VARCHAR(8) NOT NULL,
    "AUI" VARCHAR(10),
    "STR" TEXT NOT NULL,
    "ARCHIVE_TIMESTAMP" VARCHAR(280) NOT NULL,
    "CREATED_TIMESTAMP" VARCHAR(280) NOT NULL,
    "UPDATED_TIMESTAMP" VARCHAR(280) NOT NULL,
    "CODE" VARCHAR(50),
    "IS_BRAND" VARCHAR(1),
    "LAT" VARCHAR(3),
    "LAST_RELEASED" VARCHAR(30),
    "SAUI" VARCHAR(50),
    "VSAB" VARCHAR(40),
    "RXCUI" VARCHAR(8),
    "SAB" VARCHAR(20),
    "TTY" VARCHAR(20),
    "MERGED_TO_RXCUI" VARCHAR(8)
);

DROP TABLE IF EXISTS faers_b."RXNCONSO";
CREATE TABLE faers_b."RXNCONSO" (
    "RXCUI" VARCHAR(8) NOT NULL,
    "LAT" VARCHAR(3) NOT NULL DEFAULT 'ENG',
    "TS" VARCHAR(1),
    "LUI" VARCHAR(8),
    "STT" VARCHAR(3),
    "SUI" VARCHAR(8),
    "ISPREF" VARCHAR(1),
    "RXAUI" VARCHAR(8) NOT NULL,
    "SAUI" VARCHAR(50),
    "SCUI" VARCHAR(50),
    "SDUI" VARCHAR(50),
    "SAB" VARCHAR(20) NOT NULL,
    "TTY" VARCHAR(20) NOT NULL,
    "CODE" VARCHAR(50) NOT NULL,
    "STR" TEXT NOT NULL,
    "SRL" VARCHAR(10),
    "SUPPRESS" VARCHAR(1),
    "CVF" VARCHAR(50)
);

DROP TABLE IF EXISTS faers_b."RXNREL";
CREATE TABLE faers_b."RXNREL" (
    "RXCUI1" VARCHAR(8),
    "RXAUI1" VARCHAR(8),
    "STYPE1" VARCHAR(50),
    "REL" VARCHAR(4),
    "RXCUI2" VARCHAR(8),
    "RXAUI2" VARCHAR(8),
    "STYPE2" VARCHAR(50),
    "RELA" VARCHAR(100),
    "RUI" VARCHAR(10),
    "SRUI" VARCHAR(50),
    "SAB" VARCHAR(20) NOT NULL,
    "SL" VARCHAR(1000),
    "DIR" VARCHAR(1),
    "RG" VARCHAR(10),
    "SUPPRESS" VARCHAR(1),
    "CVF" VARCHAR(50)
);

DROP TABLE IF EXISTS faers_b."RXNSAB";
CREATE TABLE faers_b."RXNSAB" (
    "VCUI" VARCHAR(8),
    "RCUI" VARCHAR(8),
    "VSAB" VARCHAR(40),
    "RSAB" VARCHAR(20) NOT NULL,
    "SON" TEXT,
    "SF" VARCHAR(20),
    "SVER" VARCHAR(20),
    "VSTART" VARCHAR(10),
    "VEND" VARCHAR(10),
    "IMETA" VARCHAR(10),
    "RMETA" VARCHAR(10),
    "SLC" VARCHAR(1000),
    "SCC" VARCHAR(1000),
    "SRL" INTEGER,
    "TFR" INTEGER,
    "CFR" INTEGER,
    "CXTY" VARCHAR(50),
    "TTYL" VARCHAR(300),
    "ATNL" VARCHAR(1000),
    "LAT" VARCHAR(3),
    "CENC" VARCHAR(20),
    "CURVER" VARCHAR(1),
    "SABIN" VARCHAR(1),
    "SSN" TEXT,
    "SCIT" VARCHAR(4000)
);

DROP TABLE IF EXISTS faers_b."RXNSAT";
CREATE TABLE faers_b."RXNSAT" (
    "RXCUI" VARCHAR(8),
    "LUI" VARCHAR(8),
    "SUI" VARCHAR(8),
    "RXAUI" VARCHAR(9),
    "STYPE" VARCHAR(50),
    "CODE" VARCHAR(50),
    "ATUI" VARCHAR(11),
    "SATUI" VARCHAR(50),
    "ATN" VARCHAR(1000) NOT NULL,
    "SAB" VARCHAR(20) NOT NULL,
    "ATV" VARCHAR(4000),
    "SUPPRESS" VARCHAR(1),
    "CVF" VARCHAR(50)
);

DROP TABLE IF EXISTS faers_b."RXNSTY";
CREATE TABLE faers_b."RXNSTY" (
    "RXCUI" VARCHAR(8) NOT NULL,
    "TUI" VARCHAR(4),
    "STN" VARCHAR(100),
    "STY" VARCHAR(50),
    "ATUI" VARCHAR(11),
    "CVF" VARCHAR(50)
);

DROP TABLE IF EXISTS faers_b."RXNDOC";
CREATE TABLE faers_b."RXNDOC" (
    "DOCKEY" VARCHAR(50) NOT NULL,
    "VALUE" VARCHAR(1000),
    "TYPE" VARCHAR(50) NOT NULL,
    "EXPL" VARCHAR(1000)
);

DROP TABLE IF EXISTS faers_b."RXNCUICHANGES";
CREATE TABLE faers_b."RXNCUICHANGES" (
    "RXAUI" VARCHAR(8),
    "CODE" VARCHAR(50),
    "SAB" VARCHAR(20),
    "TTY" VARCHAR(20),
    "STR" TEXT,
    "OLD_RXCUI" VARCHAR(8) NOT NULL,
    "NEW_RXCUI" VARCHAR(8) NOT NULL
);

DROP TABLE IF EXISTS faers_b."RXNCUI";
CREATE TABLE faers_b."RXNCUI" (
    "cui1" VARCHAR(8),
    "ver_start" VARCHAR(40),
    "ver_end" VARCHAR(40),
    "cardinality" VARCHAR(8),
    "cui2" VARCHAR(8)
);

-- Load data using \copy
\copy faers_b."RXNATOMARCHIVE" FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNATOMARCHIVE.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);
\copy faers_b."RXNCONSO" FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNCONSO.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);
\copy faers_b."RXNREL" FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNREL.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);
\copy faers_b."RXNSAB" FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNSAB.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);
\copy faers_b."RXNSAT" FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNSAT.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);
\copy faers_b."RXNSTY" FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNSTY.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);
\copy faers_b."RXNDOC" FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNDOC.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);
\copy faers_b."RXNCUICHANGES" FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNCUICHANGES.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);
\copy faers_b."RXNCUI" FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNCUI.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);

-- Create indexes
CREATE INDEX IF NOT EXISTS "DRUGNAME_INDEX" ON faers_b."DRUG_Mapper" ("DRUGNAME");
CREATE INDEX IF NOT EXISTS "RXNCONSO_RXCUI" ON faers_b."RXNCONSO" ("RXCUI");
CREATE INDEX IF NOT EXISTS "RXNCONSO_RXAUI" ON faers_b."RXNCONSO" ("RXAUI");
CREATE INDEX IF NOT EXISTS "RXNCONSO_SAB" ON faers_b."RXNCONSO" ("SAB");
CREATE INDEX IF NOT EXISTS "RXNCONSO_TTY" ON faers_b."RXNCONSO" ("TTY");
CREATE INDEX IF NOT EXISTS "RXNCONSO_CODE" ON faers_b."RXNCONSO" ("CODE");
CREATE INDEX IF NOT EXISTS "RXNSAT_RXCUI" ON faers_b."RXNSAT" ("RXCUI");
CREATE INDEX IF NOT EXISTS "RXNSAT_RXAUI" ON faers_b."RXNSAT" ("RXCUI", "RXAUI");
CREATE INDEX IF NOT EXISTS "RXNREL_RXCUI1" ON faers_b."RXNREL" ("RXCUI1");
CREATE INDEX IF NOT EXISTS "RXNREL_RXCUI2" ON faers_b."RXNREL" ("RXCUI2");
CREATE INDEX IF NOT EXISTS "RXNREL_RXAUI1" ON faers_b."RXNREL" ("RXAUI1");
CREATE INDEX IF NOT EXISTS "RXNREL_RXAUI2" ON faers_b."RXNREL" ("RXAUI2");
CREATE INDEX IF NOT EXISTS "RXNREL_RELA" ON faers_b."RXNREL" ("RELA");
CREATE INDEX IF NOT EXISTS "RXNREL_REL" ON faers_b."RXNREL" ("REL");