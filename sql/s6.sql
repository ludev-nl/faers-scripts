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

-- Create helper function for consistent string cleaning
CREATE OR REPLACE FUNCTION faers_b.clean_string(input TEXT)
RETURNS TEXT AS $$
DECLARE
    output TEXT := input;
BEGIN
    -- Extract content from parentheses if present
    IF POSITION('(' IN output) > 0 AND POSITION(')' IN output) > POSITION('(' IN output) THEN
        output := SUBSTRING(output FROM POSITION('(' IN output) + 1 FOR 
                           POSITION(')' IN output) - POSITION('(' IN output) - 1);
    END IF;

    -- Clean special characters and trim
    output := TRIM(BOTH ' :.,?/`~!@#$%^&*-_=+ ' FROM output);

    -- Replace common patterns
    output := REPLACE(output, ';', ' / ');
    output := REGEXP_REPLACE(output, '\s+', ' ', 'g');

    RETURN COALESCE(output, '');
END;
$$ LANGUAGE plpgsql;

-- Create products_at_fda table
DO $$
BEGIN
    DROP TABLE IF EXISTS faers_b.products_at_fda;
    CREATE TABLE faers_b.products_at_fda (
        applno VARCHAR(10),
        productno VARCHAR(10),
        form TEXT,
        strength TEXT,
        referencedrug INTEGER,
        drugname TEXT,
        activeingredient TEXT,
        referencestandard INTEGER,
        rxaui VARCHAR(8),
        ai_2 TEXT
    );
    RAISE NOTICE 'Created faers_b.products_at_fda table';
END $$;

-- Process activeingredient
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'products_at_fda'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_b.products_at_fda does not exist, skipping ai_2 update';
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.products_at_fda;
    IF row_count = 0 THEN
        RAISE NOTICE 'Table faers_b.products_at_fda is empty, skipping ai_2 update';
        RETURN;
    END IF;

    UPDATE faers_b.products_at_fda
    SET ai_2 = faers_b.clean_string(activeingredient);
END $$;

-- Create IDD table
DO $$
BEGIN
    DROP TABLE IF EXISTS faers_b."IDD";
    CREATE TABLE faers_b."IDD" (
        "DRUGNAME" TEXT,
        "RXAUI" VARCHAR(8),
        "RXCUI" VARCHAR(8),
        "STR" TEXT,
        "SAB" VARCHAR(50),
        "TTY" VARCHAR(10),
        "CODE" VARCHAR(50)
    );
    RAISE NOTICE 'Created faers_b.IDD table';
END $$;

-- Create indexes for performance
DO $$
BEGIN
    IF EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'IDD'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_idd_drugname ON faers_b."IDD" ("DRUGNAME");
        CREATE INDEX IF NOT EXISTS idx_idd_rxaui ON faers_b."IDD" ("RXAUI");
    END IF;

    IF EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'products_at_fda'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_products_at_fda_applno ON faers_b.products_at_fda (applno);
        CREATE INDEX IF NOT EXISTS idx_products_at_fda_rxaui ON faers_b.products_at_fda (rxaui);
    END IF;

    IF EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'drug_mapper'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_drug_mapper_nda_num ON faers_b.drug_mapper (nda_num);
        CREATE INDEX IF NOT EXISTS idx_drug_mapper_notes ON faers_b.drug_mapper (notes);
    END IF;

    IF EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'rxnconso'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_rxnconso_str_sab_tty ON faers_b.rxnconso (str, sab, tty);
    END IF;

    RAISE NOTICE 'Created indexes for performance';
END $$;

-- Update products_at_fda rxaui mappings
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'products_at_fda'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_b.products_at_fda does not exist, skipping rxaui updates';
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.products_at_fda;
    IF row_count = 0 THEN
        RAISE NOTICE 'Table faers_b.products_at_fda is empty, skipping rxaui updates';
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_b.rxnconso does not exist, skipping rxaui updates';
        RETURN;
    END IF;

    -- Strict conditions
    UPDATE faers_b.products_at_fda
    SET rxaui = rxnconso.rxaui
    FROM faers_b.rxnconso
    WHERE products_at_fda.ai_2 = rxnconso.str
      AND rxnconso.sab = 'RXNORM'
      AND rxnconso.tty IN ('IN', 'MIN')
      AND products_at_fda.rxaui IS NULL;

    -- Relaxed conditions
    UPDATE faers_b.products_at_fda
    SET rxaui = rxnconso.rxaui
    FROM faers_b.rxnconso
    WHERE products_at_fda.ai_2 = rxnconso.str
      AND products_at_fda.rxaui IS NULL;
END $$;

-- Mapping drugs to RxNorm
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'drug_mapper'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_b.drug_mapper does not exist, skipping RxNorm mapping';
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper;
    IF row_count = 0 THEN
        RAISE NOTICE 'Table faers_b.drug_mapper is empty, skipping RxNorm mapping';
        RETURN;
    END IF;

    -- Mapping by NDA number
    IF EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'products_at_fda'
    ) AND EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'rxnconso'
    ) THEN
        UPDATE faers_b.drug_mapper
        SET rxaui = c.rxaui,
            rxcui = c.rxcui,
            notes = '1.0',
            sab = c.sab,
            tty = c.tty,
            str = c.str,
            code = c.code
        FROM faers_b.products_at_fda b
        JOIN faers_b.rxnconso c ON b.rxaui = c.rxaui
        WHERE drug_mapper.notes IS NULL
          AND drug_mapper.nda_num ~ '^[0-9]+$'
          AND b.rxaui IS NOT NULL
          AND POSITION('.' IN drug_mapper.nda_num) = 0
          AND LENGTH(drug_mapper.nda_num) < 6
          AND (CASE 
                   WHEN LEFT(drug_mapper.nda_num, 1) = '0' 
                   THEN RIGHT(drug_mapper.nda_num, LENGTH(drug_mapper.nda_num) - 1)
                   ELSE drug_mapper.nda_num 
               END) = b.applno;
    END IF;

    -- Create temp table for cleaned names
    CREATE TEMP TABLE cleaned_drugs (
        id INTEGER,
        drugname TEXT,
        prod_ai TEXT,
        clean_drugname TEXT,
        clean_prodai TEXT
    );

    INSERT INTO cleaned_drugs
    SELECT id, 
           drugname, 
           prod_ai, 
           faers_b.clean_string(drugname) AS clean_drugname, 
           faers_b.clean_string(prod_ai) AS clean_prodai
    FROM faers_b.drug_mapper
    WHERE notes IS NULL;

    CREATE INDEX idx_cleaned_drugs ON cleaned_drugs (id, clean_drugname, clean_prodai);

    -- Mapping by drug name
    IF EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'rxnconso'
    ) THEN
        UPDATE faers_b.drug_mapper
        SET rxaui = r.rxaui,
            rxcui = r.rxcui,
            notes = CASE 
                WHEN r.sab = 'RXNORM' AND r.tty = 'IN' THEN '1.1'
                WHEN r.sab = 'RXNORM' AND r.tty = 'MIN' THEN '1.2'
                WHEN r.sab = 'RXNORM' AND r.tty = 'PIN' THEN '1.2.2'
                WHEN r.sab = 'MTHSPL' THEN '1.3'
                WHEN r.tty = 'IN' THEN '1.4'
                WHEN r.sab = 'RXNORM' THEN '1.5'
                ELSE '1.6'
            END,
            sab = r.sab,
            tty = r.tty,
            str = r.str,
            code = r.code
        FROM cleaned_drugs cd
        JOIN faers_b.rxnconso r ON cd.clean_drugname = r.str
        WHERE drug_mapper.id = cd.id
          AND drug_mapper.notes IS NULL
          AND (
              (r.sab = 'RXNORM' AND r.tty IN ('IN', 'MIN', 'PIN')) OR
              (r.sab = 'MTHSPL') OR
              (r.tty = 'IN') OR
              (r.sab = 'RXNORM')
          );

        -- Mapping by product active ingredient
        UPDATE faers_b.drug_mapper
        SET rxaui = r.rxaui,
            rxcui = r.rxcui,
            notes = CASE 
                WHEN r.sab = 'RXNORM' AND r.tty = 'IN' THEN '2.1'
                WHEN r.sab = 'RXNORM' AND r.tty = 'MIN' THEN '2.2'
                WHEN r.sab = 'RXNORM' AND r.tty = 'PIN' THEN '2.2.2'
                WHEN r.sab = 'MTHSPL' THEN '2.3'
                WHEN r.tty = 'IN' THEN '2.4'
                WHEN r.sab = 'RXNORM' THEN '2.5'
                ELSE '2.6'
            END,
            sab = r.sab,
            tty = r.tty,
            str = r.str,
            code = r.code
        FROM cleaned_drugs cd
        JOIN faers_b.rxnconso r ON cd.clean_prodai = r.str
        WHERE drug_mapper.id = cd.id
          AND drug_mapper.notes IS NULL
          AND (
              (r.sab = 'RXNORM' AND r.tty IN ('IN', 'MIN', 'PIN')) OR
              (r.sab = 'MTHSPL') OR
              (r.tty = 'IN') OR
              (r.sab = 'RXNORM')
          );
    END IF;

    -- Clean up temp table
    DROP TABLE cleaned_drugs;
END $$;

-- IDD mapping
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'IDD'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_b.IDD does not exist, skipping IDD mapping';
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b."IDD";
    IF row_count = 0 THEN
        RAISE NOTICE 'Table faers_b.IDD is empty, skipping IDD mapping';
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'drug_mapper'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_b.drug_mapper does not exist, skipping IDD mapping';
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper;
    IF row_count = 0 THEN
        RAISE NOTICE 'Table faers_b.drug_mapper is empty, skipping IDD mapping';
        RETURN;
    END IF;

    -- Mapping by drug name
    UPDATE faers_b.drug_mapper
    SET notes = CASE 
            WHEN i."SAB" = 'RXNORM' AND i."TTY" IN ('IN', 'MIN', 'PIN') THEN '6.1'
            WHEN i."TTY" = 'IN' THEN '6.4'
            ELSE '6.5'
        END,
        rxaui = CAST(i."RXAUI" AS BIGINT),
        rxcui = CAST(i."RXCUI" AS BIGINT),
        sab = i."SAB",
        tty = i."TTY",
        str = i."STR",
        code = i."CODE"
    FROM faers_b."IDD" i
    WHERE drug_mapper.drugname = i."DRUGNAME"
      AND drug_mapper.notes IS NULL
      AND i."RXAUI" IS NOT NULL;

    -- Mapping by product active ingredient
    UPDATE faers_b.drug_mapper
    SET notes = CASE 
            WHEN i."SAB" = 'RXNORM' AND i."TTY" IN ('IN', 'MIN', 'PIN') THEN '6.2'
            WHEN i."TTY" = 'IN' THEN '6.4'
            ELSE '6.6'
        END,
        rxaui = CAST(i."RXAUI" AS BIGINT),
        rxcui = CAST(i."RXCUI" AS BIGINT),
        sab = i."SAB",
        tty = i."TTY",
        str = i."STR",
        code = i."CODE"
    FROM faers_b."IDD" i
    WHERE drug_mapper.prod_ai = i."DRUGNAME"
      AND drug_mapper.notes IS NULL
      AND i."RXAUI" IS NOT NULL;
END $$;

-- Create manual mapping table
DO $$
BEGIN
    DROP TABLE IF EXISTS faers_b.manual_mapping;
    CREATE TABLE faers_b.manual_mapping (
        drugname TEXT,
        count INTEGER,
        rxaui BIGINT,
        rxcui BIGINT,
        sab VARCHAR(20),
        tty VARCHAR(20),
        str TEXT,
        code VARCHAR(50),
        notes TEXT
    );
    RAISE NOTICE 'Created faers_b.manual_mapping table';
END $$;

-- Insert unmapped drugs with high counts
DO $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'drug_mapper'
    ) INTO table_exists;

    IF NOT table_exists THEN
        RAISE NOTICE 'Table faers_b.drug_mapper does not exist, skipping manual mapping';
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper;
    IF row_count = 0 THEN
        RAISE NOTICE 'Table faers_b.drug_mapper is empty, skipping manual mapping';
        RETURN;
    END IF;

    INSERT INTO faers_b.manual_mapping (count, drugname)
    SELECT COUNT(drugname) AS count, drugname
    FROM faers_b.drug_mapper
    WHERE notes IS NULL
    GROUP BY drugname
    HAVING COUNT(drugname) > 199;
END $$;