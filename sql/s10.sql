<<<<<<<< HEAD:sql/s10.sql
-- s10.sql: Create and set up remapping tables in faers_b schema

-- Statement 1: Verify database context
DO $$
BEGIN
    IF current_database() != 'faersdatabase' THEN
        RAISE EXCEPTION 'Must be connected to faersdatabase, current database is %', current_database();
    END IF;
END $$;

-- Statement 2: Ensure faers_b schema exists
CREATE SCHEMA IF NOT EXISTS faers_b AUTHORIZATION postgres;

-- Statement 3: Verify schema exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'faers_b') THEN
        RAISE EXCEPTION 'Schema faers_b failed to create';
    END IF;
END $$;

-- Statement 4: Grant privileges
GRANT ALL ON SCHEMA faers_b TO postgres;

-- Statement 5: Set search path
SET search_path TO faers_b, faers_combined, public;

-- Statement 6: Create logging table
CREATE TABLE IF NOT EXISTS faers_b.remapping_log (
    log_id SERIAL PRIMARY KEY,
    step VARCHAR(50),
    message TEXT,
    log_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Statement 7: Create drug_mapper table
CREATE TABLE IF NOT EXISTS faers_b.drug_mapper (
    drug_id TEXT,
    primaryid TEXT,
    drug_seq TEXT,
    role_cod TEXT,
    period TEXT,
    drugname TEXT,
    prod_ai TEXT,
    notes TEXT,
    rxaui VARCHAR(8),
    rxcui VARCHAR(8),
    str TEXT,
    sab VARCHAR(20),
    tty VARCHAR(20),
    code VARCHAR(50),
    remapping_rxaui VARCHAR(8),
    remapping_rxcui VARCHAR(8),
    remapping_str TEXT,
    remapping_sab VARCHAR(20),
    remapping_tty VARCHAR(20),
    remapping_code VARCHAR(50),
    remapping_notes TEXT
);

-- Statement 8: Create drug_mapper_2 table
CREATE TABLE IF NOT EXISTS faers_b.drug_mapper_2 (
    drug_id TEXT,
    primaryid TEXT,
    drug_seq TEXT,
    role_cod TEXT,
    period TEXT,
    drugname TEXT,
    prod_ai TEXT,
    notes TEXT,
    rxaui VARCHAR(8),
    rxcui VARCHAR(8),
    str TEXT,
    sab VARCHAR(20),
    tty VARCHAR(20),
    code VARCHAR(50),
    remapping_notes TEXT,
    rela TEXT,
    remapping_rxaui VARCHAR(8),
    remapping_rxcui VARCHAR(8),
    remapping_str TEXT,
    remapping_sab VARCHAR(20),
    remapping_tty VARCHAR(20),
    remapping_code VARCHAR(50)
);

-- Statement 9: Create drug_mapper_3 table
CREATE TABLE IF NOT EXISTS faers_b.drug_mapper_3 (
    drug_id TEXT,
    primaryid TEXT,
    drug_seq TEXT,
    role_cod TEXT,
    period TEXT,
    drugname TEXT,
    prod_ai TEXT,
    notes TEXT,
    rxaui VARCHAR(8),
    rxcui VARCHAR(8),
    str TEXT,
    sab VARCHAR(20),
    tty VARCHAR(20),
    code VARCHAR(50),
    remapping_rxaui VARCHAR(8),
    remapping_rxcui VARCHAR(8),
    remapping_str TEXT,
    remapping_sab VARCHAR(20),
    remapping_tty VARCHAR(20),
    remapping_code VARCHAR(50),
    remapping_notes TEXT
);

-- Statement 10: Create manual_remapper table
CREATE TABLE IF NOT EXISTS faers_b.manual_remapper (
    count INTEGER,
    source_drugname VARCHAR(3000),
    source_rxaui VARCHAR(8),
    source_rxcui VARCHAR(8),
    source_sab VARCHAR(20),
    source_tty VARCHAR(20),
    final_rxaui BIGINT,
    notes VARCHAR(100)
);

-- Statement 11: Create indexes for performance
DO $$
BEGIN
    IF EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'rxnconso'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_rxnconso_rxcui ON faers_b.rxnconso(rxcui) INCLUDE (rxaui, str, sab, tty, code);
        CREATE INDEX IF NOT EXISTS idx_rxnconso_rxaui ON faers_b.rxnconso(rxaui);
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Create Indexes', 'Created indexes on rxnconso');
    ELSE
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Create Indexes', 'Table rxnconso does not exist, skipping index creation');
    END IF;

    IF EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'rxnrel'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_rxnrel_rxcui ON faers_b.rxnrel(rxcui1, rxcui2) INCLUDE (rxaui1, rxaui2, rela);
        CREATE INDEX IF NOT EXISTS idx_rxnrel_rxaui ON faers_b.rxnrel(rxaui1, rxaui2);
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Create Indexes', 'Created indexes on rxnrel');
    ELSE
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Create Indexes', 'Table rxnrel does not exist, skipping index creation');
    END IF;

    IF EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'drug_mapper'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_drug_mapper_remapping ON faers_b.drug_mapper(remapping_rxcui, remapping_rxaui) INCLUDE (drug_id, remapping_notes);
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Create Indexes', 'Created indexes on drug_mapper');
    ELSE
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Create Indexes', 'Table drug_mapper does not exist, skipping index creation');
    END IF;
END $$;

-- Statement 12: Step 1 - Initial RXNORM Update
CREATE OR REPLACE FUNCTION faers_b.step_1_initial_rxnorm_update() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 1', 'Table faers_b.drug_mapper does not exist, skipping Step 1');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 1', 'Table faers_b.drug_mapper is empty, skipping Step 1');
        RETURN;
    END IF;

    UPDATE faers_b.drug_mapper
    SET remapping_rxcui = rxcui,
        remapping_rxaui = rxaui,
        remapping_str = str,
        remapping_sab = sab,
        remapping_tty = tty,
        remapping_code = code,
        remapping_notes = '1'
    FROM faers_b.rxnconso
    WHERE faers_b.drug_mapper.rxcui = faers_b.rxnconso.rxcui
      AND faers_b.rxnconso.sab = 'RXNORM'
      AND faers_b.rxnconso.tty = 'IN'
      AND faers_b.drug_mapper.remapping_notes IS NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 1', 'Initial RXNORM update completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 1', 'Error in Step 1: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 13: Step 2 - Create drug_mapper_2
CREATE OR REPLACE FUNCTION faers_b.step_2_create_drug_mapper_2() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 2', 'Table faers_b.drug_mapper does not exist, skipping Step 2');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 2', 'Table faers_b.drug_mapper is empty, skipping Step 2');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 2', 'Table faers_b.rxnconso does not exist, skipping Step 2');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnrel'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 2', 'Table faers_b.rxnrel does not exist, skipping Step 2');
        RETURN;
    END IF;

    INSERT INTO faers_b.drug_mapper_2
    SELECT c.drug_id, c.primaryid, c.drug_seq, c.role_cod, c.period, c.drugname, c.prod_ai, c.notes,
           c.rxaui, c.rxcui, c.str, c.sab, c.tty, c.code,
           CASE WHEN a.rxaui IS NULL THEN c.remapping_notes ELSE '2' END AS remapping_notes,
           b.rela,
           CASE WHEN a.rxaui IS NULL THEN c.remapping_rxaui ELSE a.rxaui END AS remapping_rxaui,
           CASE WHEN a.rxcui IS NULL THEN c.remapping_rxcui ELSE a.rxcui END AS remapping_rxcui,
           CASE WHEN a.str IS NULL THEN c.remapping_str ELSE a.str END AS remapping_str,
           CASE WHEN a.sab IS NULL THEN c.remapping_sab ELSE a.sab END AS remapping_sab,
           CASE WHEN a.tty IS NULL THEN c.remapping_tty ELSE a.tty END AS remapping_tty,
           CASE WHEN a.code IS NULL THEN c.remapping_code ELSE a.code END AS remapping_code
    FROM faers_b.rxnconso a
    INNER JOIN faers_b.rxnrel b ON a.rxcui = b.rxcui1 AND a.tty = 'IN' AND a.sab = 'RXNORM'
    RIGHT OUTER JOIN faers_b.drug_mapper c ON b.rxcui2 = c.rxcui
    WHERE c.remapping_notes IS NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 2', 'Created drug_mapper_2 successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 2', 'Error in Step 2: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 14: Step 3 - Manual Remapping Update
CREATE OR REPLACE FUNCTION faers_b.step_3_manual_remapping_update() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 3', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 3');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 3', 'Table faers_b.drug_mapper_2 is empty, skipping Step 3');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'hopefully_last_one_5_7_2021'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 3', 'Table faers_b.hopefully_last_one_5_7_2021 does not exist, skipping Step 3');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 3', 'Table faers_b.rxnconso does not exist, skipping Step 3');
        RETURN;
    END IF;

    UPDATE faers_b.drug_mapper_2
    SET remapping_notes = 'MAN_REM /',
        remapping_rxaui = h.last_rxaui,
        remapping_rxcui = r.rxcui,
        remapping_str = r.str,
        remapping_sab = r.sab,
        remapping_tty = r.tty,
        remapping_code = r.code
    FROM faers_b.hopefully_last_one_5_7_2021 h
    INNER JOIN faers_b.rxnconso r ON h.last_rxaui = r.rxaui
    WHERE drug_mapper_2.str = h.str
      AND drug_mapper_2.rxaui = h.rxaui
      AND drug_mapper_2.remapping_notes IS NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 3', 'Manual remapping update completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 3', 'Error in Step 3: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 15: Step 4 - Manual Remapping Insert
CREATE OR REPLACE FUNCTION faers_b.step_4_manual_remapping_insert() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 4', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 4');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 4', 'Table faers_b.drug_mapper_2 is empty, skipping Step 4');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'hopefully_last_one_5_7_2021'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 4', 'Table faers_b.hopefully_last_one_5_7_2021 does not exist, skipping Step 4');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 4', 'Table faers_b.rxnconso does not exist, skipping Step 4');
        RETURN;
    END IF;

    INSERT INTO faers_b.drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui,
     str, sab, tty, code, remapping_notes, rela, remapping_rxaui, remapping_rxcui, remapping_sab,
     remapping_tty, remapping_code, remapping_str)
    SELECT d.drug_id, d.primaryid, d.drug_seq, d.role_cod, d.period, d.drugname, d.prod_ai, d.notes,
           d.rxaui, d.rxcui, d.str, d.sab, d.tty, d.code,
           'MAN2/' || d.remapping_notes,
           d.rela,
           h.last_rxaui AS remapping_rxaui,
           r.rxcui AS remapping_rxcui,
           r.sab AS remapping_sab,
           r.tty AS remapping_tty,
           r.code AS remapping_code,
           r.str AS remapping_str
    FROM faers_b.drug_mapper_2 d
    INNER JOIN faers_b.hopefully_last_one_5_7_2021 h ON h.rxaui = d.rxaui AND h.str = d.str
    INNER JOIN faers_b.rxnconso r ON h.last_rxaui = r.rxaui
    WHERE h.last_rxaui IS NOT NULL
      AND d.remapping_notes LIKE 'MAN_REM /%';

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 4', 'Manual remapping insert completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 4', 'Error in Step 4: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 16: Step 5 - Manual Remapping Delete
CREATE OR REPLACE FUNCTION faers_b.step_5_manual_remapping_delete() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 5', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 5');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 5', 'Table faers_b.drug_mapper_2 is empty, skipping Step 5');
        RETURN;
    END IF;

    DELETE FROM faers_b.drug_mapper_2
    WHERE remapping_notes LIKE 'MAN_REM /%';

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 5', 'Manual remapping delete completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 5', 'Error in Step 5: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 17: Step 6 - VANDF Relationships
CREATE OR REPLACE FUNCTION faers_b.step_6_vandf_relationships() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 6', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 6');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 6', 'Table faers_b.drug_mapper_2 is empty, skipping Step 6');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 6', 'Table faers_b.rxnconso does not exist, skipping Step 6');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnrel'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 6', 'Table faers_b.rxnrel does not exist, skipping Step 6');
        RETURN;
    END IF;

    INSERT INTO faers_b.drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui, str, sab,
     tty, code, rela, remapping_notes, remapping_rxaui, remapping_rxcui, remapping_str, remapping_sab,
     remapping_tty, remapping_code)
    SELECT e.drug_id, e.primaryid, e.drug_seq, e.role_cod, e.period, e.drugname, e.prod_ai, e.notes,
           e.rxaui, e.rxcui, e.str, e.sab, e.tty, e.code, e.rela, '3',
           a.rxaui AS remapping_rxaui,
           a.rxcui AS remapping_rxcui,
           a.str AS remapping_str,
           a.sab AS remapping_sab,
           a.tty AS remapping_tty,
           a.code AS remapping_code
    FROM faers_b.rxnconso a
    INNER JOIN faers_b.rxnrel b ON a.rxcui = b.rxcui1 AND a.tty = 'IN' AND a.sab = 'RXNORM'
    INNER JOIN faers_b.rxnconso c ON b.rxcui2 = c.rxcui
    INNER JOIN faers_b.rxnrel d ON c.rxcui = d.rxcui1 AND d.rela = 'HAS_INGREDIENTS' AND c.sab = 'VANDF' AND c.tty = 'IN'
    INNER JOIN faers_b.drug_mapper_2 e ON d.rxcui2 = e.rxcui
    WHERE e.remapping_notes IS NULL;

    DELETE FROM faers_b.drug_mapper_2
    WHERE drug_id IN (
        SELECT drug_id FROM faers_b.drug_mapper_2 WHERE remapping_notes = '3'
    ) AND remapping_notes IS NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 6', 'VANDF relationships completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 6', 'Error in Step 6: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 18: Step 7 - MMSL to RXNORM Insert
CREATE OR REPLACE FUNCTION faers_b.step_7_mmsl_to_rxnorm_insert() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 7', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 7');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 7', 'Table faers_b.drug_mapper_2 is empty, skipping Step 7');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 7', 'Table faers_b.rxnconso does not exist, skipping Step 7');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnrel'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 7', 'Table faers_b.rxnrel does not exist, skipping Step 7');
        RETURN;
    END IF;

    INSERT INTO faers_b.drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui, str, sab,
     tty, code, remapping_notes, remapping_rxaui, remapping_rxcui, remapping_str, remapping_sab,
     remapping_tty, remapping_code)
    SELECT DISTINCT e.drug_id, e.primaryid, e.drug_seq, e.role_cod, e.period, e.drugname, e.prod_ai, e.notes,
           e.rxaui, e.rxcui, e.str, e.sab, e.tty, e.code, '7',
           c1.rxaui AS remapping_rxaui,
           c1.rxcui AS remapping_rxcui,
           c1.str AS remapping_str,
           c1.sab AS remapping_sab,
           c1.tty AS remapping_tty,
           c1.code AS remapping_code
    FROM faers_b.drug_mapper_2 e
    INNER JOIN faers_b.rxnrel r ON e.rxcui = r.rxcui2
    INNER JOIN faers_b.rxnconso c ON r.rxcui1 = c.rxcui
    INNER JOIN faers_b.rxnrel r1 ON r.rxcui1 = r1.rxcui2
    INNER JOIN faers_b.rxnconso c1 ON r1.rxcui1 = c1.rxcui
    WHERE e.sab = 'MMSL'
      AND c1.sab = 'RXNORM'
      AND c1.tty = 'IN'
      AND c.sab = 'RXNORM'
      AND c.tty = 'SCDC'
      AND c1.rxaui != '11794211'
      AND e.remapping_notes IS NULL;

    DELETE FROM faers_b.drug_mapper_2
    WHERE drug_id IN (
        SELECT drug_id FROM faers_b.drug_mapper_2 WHERE remapping_notes = '7'
    ) AND remapping_notes IS NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 7', 'MMSL to RXNORM insert completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 7', 'Error in Step 7: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 19: Step 8 - RXNORM SCDC to IN Insert
CREATE OR REPLACE FUNCTION faers_b.step_8_rxnorm_scdc_to_in_insert() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 8', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 8');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 8', 'Table faers_b.drug_mapper_2 is empty, skipping Step 8');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 8', 'Table faers_b.rxnconso does not exist, skipping Step 8');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnrel'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 8', 'Table faers_b.rxnrel does not exist, skipping Step 8');
        RETURN;
    END IF;

    INSERT INTO faers_b.drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui, str, sab,
     tty, code, remapping_notes, remapping_rxaui, remapping_rxcui, remapping_str, remapping_sab,
     remapping_tty, remapping_code)
    SELECT DISTINCT e.drug_id, e.primaryid, e.drug_seq, e.role_cod, e.period, e.drugname, e.prod_ai, e.notes,
           e.rxaui, e.rxcui, e.str, e.sab, e.tty, e.code, '8',
           c1.rxaui AS remapping_rxaui,
           c1.rxcui AS remapping_rxcui,
           c1.str AS remapping_str,
           c1.sab AS remapping_sab,
           c1.tty AS remapping_tty,
           c1.code AS remapping_code
    FROM faers_b.drug_mapper_2 e
    INNER JOIN faers_b.rxnrel r ON e.rxcui = r.rxcui2
    INNER JOIN faers_b.rxnconso c ON r.rxcui1 = c.rxcui
    INNER JOIN faers_b.rxnrel r1 ON r.rxcui1 = r1.rxcui2
    INNER JOIN faers_b.rxnconso c1 ON r1.rxcui1 = c1.rxcui
    WHERE c.sab = 'RXNORM'
      AND c.tty = 'SCDC'
      AND c1.sab = 'RXNORM'
      AND c1.tty = 'IN'
      AND e.remapping_notes IS NULL;

    DELETE FROM faers_b.drug_mapper_2
    WHERE drug_id IN (
        SELECT drug_id FROM faers_b.drug_mapper_2 WHERE remapping_notes = '8'
    ) AND remapping_notes IS NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 8', 'RXNORM SCDC to IN insert completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 8', 'Error in Step 8: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 20: Step 9 - RXNORM IN Update with Notes
CREATE OR REPLACE FUNCTION faers_b.step_9_rxnorm_in_update_with_notes() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 9', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 9');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 9', 'Table faers_b.drug_mapper_2 is empty, skipping Step 9');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 9', 'Table faers_b.rxnconso does not exist, skipping Step 9');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnrel'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 9', 'Table faers_b.rxnrel does not exist, skipping Step 9');
        RETURN;
    END IF;

    UPDATE faers_b.drug_mapper_2
    SET remapping_rxcui = c.rxcui,
        remapping_rxaui = c.rxaui,
        remapping_str = c.str,
        remapping_sab = c.sab,
        remapping_tty = c.tty,
        remapping_code = c.code,
        remapping_notes = '9'
    FROM faers_b.rxnconso c
    RIGHT OUTER JOIN faers_b.rxnrel r ON c.rxaui = r.rxaui1
    RIGHT OUTER JOIN faers_b.drug_mapper_2 ON r.rxaui2 = drug_mapper_2.rxaui
    WHERE c.sab = 'RXNORM'
      AND c.tty = 'IN'
      AND drug_mapper_2.notes IS NOT NULL
      AND drug_mapper_2.remapping_notes IS NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 9', 'RXNORM IN update with notes completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 9', 'Error in Step 9: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 21: Step 10 - MTHSPL to RXNORM IN Insert
CREATE OR REPLACE FUNCTION faers_b.step_10_mthspl_to_rxnorm_in_insert() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 10', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 10');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 10', 'Table faers_b.drug_mapper_2 is empty, skipping Step 10');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 10', 'Table faers_b.rxnconso does not exist, skipping Step 10');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnrel'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 10', 'Table faers_b.rxnrel does not exist, skipping Step 10');
        RETURN;
    END IF;

    INSERT INTO faers_b.drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui, str, sab,
     tty, code, remapping_notes, remapping_rxaui, remapping_rxcui, remapping_str, remapping_sab,
     remapping_tty, remapping_code)
    SELECT DISTINCT e.drug_id, e.primaryid, e.drug_seq, e.role_cod, e.period, e.drugname, e.prod_ai, e.notes,
           e.rxaui, e.rxcui, e.str, e.sab, e.tty, e.code, '9',
           c1.rxaui AS remapping_rxaui,
           c1.rxcui AS remapping_rxcui,
           c1.str AS remapping_str,
           c1.sab AS remapping_sab,
           c1.tty AS remapping_tty,
           c1.code AS remapping_code
    FROM faers_b.drug_mapper_2 e
    INNER JOIN faers_b.rxnrel r ON e.rxaui = r.rxaui2
    INNER JOIN faers_b.rxnconso c ON r.rxaui1 = c.rxaui
    INNER JOIN faers_b.rxnrel r1 ON r.rxaui1 = r1.rxaui2 AND r.rela = 'HAS_ACTIVE_MOIETY'
    INNER JOIN faers_b.rxnconso c1 ON r1.rxaui1 = c1.rxaui
    WHERE c1.sab = 'RXNORM'
      AND c1.tty = 'IN'
      AND c.sab = 'MTHSPL'
      AND e.remapping_notes IS NULL;

    DELETE FROM faers_b.drug_mapper_2
    WHERE drug_id IN (
        SELECT drug_id FROM faers_b.drug_mapper_2 WHERE remapping_notes = '9'
    ) AND remapping_notes IS NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 10', 'MTHSPL to RXNORM IN insert completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 10', 'Error in Step 10: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 22: Step 11 - RXNORM IN Update
CREATE OR REPLACE FUNCTION faers_b.step_11_rxnorm_in_update() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 11', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 11');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 11', 'Table faers_b.drug_mapper_2 is empty, skipping Step 11');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 11', 'Table faers_b.rxnconso does not exist, skipping Step 11');
        RETURN;
    END IF;

    UPDATE faers_b.drug_mapper_2
    SET remapping_rxcui = c.rxcui,
        remapping_rxaui = c.rxaui,
        remapping_str = c.str,
        remapping_sab = c.sab,
        remapping_tty = c.tty,
        remapping_code = c.code,
        remapping_notes = '10'
    FROM faers_b.rxnconso c
    INNER JOIN faers_b.drug_mapper_2 ON drug_mapper_2.rxcui = c.rxcui
    WHERE c.tty = 'IN'
      AND drug_mapper_2.remapping_notes IS NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 11', 'RXNORM IN update completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 11', 'Error in Step 11: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 23: Step 12 - MMSL to RXNORM IN Insert with Exclusions
CREATE OR REPLACE FUNCTION faers_b.step_12_mmsl_to_rxnorm_in_insert_exclusions() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 12', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 12');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 12', 'Table faers_b.drug_mapper_2 is empty, skipping Step 12');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 12', 'Table faers_b.rxnconso does not exist, skipping Step 12');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnrel'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 12', 'Table faers_b.rxnrel does not exist, skipping Step 12');
        RETURN;
    END IF;

    INSERT INTO faers_b.drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui, str, sab,
     tty, code, remapping_notes, remapping_rxaui, remapping_rxcui, remapping_str, remapping_sab,
     remapping_tty, remapping_code)
    SELECT DISTINCT e.drug_id, e.primaryid, e.drug_seq, e.role_cod, e.period, e.drugname, e.prod_ai, e.notes,
           e.rxaui, e.rxcui, e.str, e.sab, e.tty, e.code, '11',
           c1.rxaui AS remapping_rxaui,
           c1.rxcui AS remapping_rxcui,
           c1.str AS remapping_str,
           c1.sab AS remapping_sab,
           c1.tty AS remapping_tty,
           c1.code AS remapping_code
    FROM faers_b.drug_mapper_2 e
    INNER JOIN faers_b.rxnrel r ON e.rxaui = r.rxaui2
    INNER JOIN faers_b.rxnconso c ON r.rxaui1 = c.rxaui
    INNER JOIN faers_b.rxnrel r1 ON r.rxaui1 = r1.rxaui2
    INNER JOIN faers_b.rxnconso c1 ON r1.rxaui1 = c1.rxaui
    WHERE c1.sab = 'MMSL'
      AND c1.tty = 'IN'
      AND e.sab = 'MMSL'
      AND c1.rxaui NOT IN ('2604414', '1182299', '1173735', '1287235')
      AND e.remapping_notes IS NULL;

    DELETE FROM faers_b.drug_mapper_2
    WHERE drug_id IN (
        SELECT drug_id FROM faers_b.drug_mapper_2 WHERE remapping_notes = '11'
    ) AND remapping_notes IS NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 12', 'MMSL to RXNORM IN insert with exclusions completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 12', 'Error in Step 12: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 24: Step 13 - RXNORM Cleanup Update
CREATE OR REPLACE FUNCTION faers_b.step_13_rxnorm_cleanup_update() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 13', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 13');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 13', 'Table faers_b.drug_mapper_2 is empty, skipping Step 13');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 13', 'Table faers_b.rxnconso does not exist, skipping Step 13');
        RETURN;
    END IF;

    UPDATE faers_b.drug_mapper_2
    SET remapping_notes = '12',
        remapping_rxaui = c.rxaui,
        remapping_rxcui = c.rxcui,
        remapping_str = c.str,
        remapping_sab = c.sab,
        remapping_tty = c.tty,
        remapping_code = c.code
    FROM faers_b.rxnconso c
    WHERE c.str = CASE
        WHEN POSITION('(' IN drug_mapper_2.remapping_str) > 0
             AND POSITION(')' IN drug_mapper_2.remapping_str) > 0
             AND POSITION('(' IN drug_mapper_2.remapping_str) < POSITION(')' IN drug_mapper_2.remapping_str)
        THEN REGEXP_REPLACE(drug_mapper_2.remapping_str, '\(.*?\)', '')
        ELSE drug_mapper_2.remapping_str
        END
      AND c.sab = 'RXNORM'
      AND c.tty IN ('IN', 'MIN', 'PIN')
      AND drug_mapper_2.remapping_notes IN ('9', '10', '11');

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 13', 'RXNORM cleanup update completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 13', 'Error in Step 13: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 25: Step 14 - Mark for Deletion
CREATE OR REPLACE FUNCTION faers_b.step_14_mark_for_deletion() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 14', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 14');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 14', 'Table faers_b.drug_mapper_2 is empty, skipping Step 14');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 14', 'Table faers_b.rxnconso does not exist, skipping Step 14');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnrel'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 14', 'Table faers_b.rxnrel does not exist, skipping Step 14');
        RETURN;
    END IF;

    UPDATE faers_b.drug_mapper_2
    SET remapping_notes = 'TO BE DELETED'
    FROM faers_b.rxnrel r
    INNER JOIN faers_b.rxnconso c ON r.rxcui1 = c.rxcui
    WHERE drug_mapper_2.remapping_rxcui = r.rxcui2
      AND c.sab = 'RXNORM'
      AND c.tty = 'IN'
      AND drug_mapper_2.remapping_notes = '12';

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 14', 'Mark for deletion completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 14', 'Error in Step 14: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 26: Step 15 - Reinsert from Deleted
CREATE OR REPLACE FUNCTION faers_b.step_15_reinsert_from_deleted() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 15', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 15');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 15', 'Table faers_b.drug_mapper_2 is empty, skipping Step 15');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 15', 'Table faers_b.rxnconso does not exist, skipping Step 15');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnrel'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 15', 'Table faers_b.rxnrel does not exist, skipping Step 15');
        RETURN;
    END IF;

    INSERT INTO faers_b.drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui, str, sab,
     tty, code, rela, remapping_notes, remapping_rxcui, remapping_sab, remapping_tty, remapping_code,
     remapping_str, remapping_rxaui)
    SELECT d.drug_id, d.primaryid, d.drug_seq, d.role_cod, d.period, d.drugname, d.prod_ai, d.notes,
           d.rxaui, d.rxcui, d.str, d.sab, d.tty, d.code, d.rela, '13',
           c.rxcui AS remapping_rxcui,
           c.sab AS remapping_sab,
           c.tty AS remapping_tty,
           c.code AS remapping_code,
           c.str AS remapping_str,
           c.rxaui AS remapping_rxaui
    FROM faers_b.drug_mapper_2 d
    INNER JOIN faers_b.rxnrel r ON d.remapping_rxcui = r.rxcui2
    INNER JOIN faers_b.rxnconso c ON r.rxcui1 = c.rxcui
    WHERE d.remapping_notes = 'TO BE DELETED'
      AND c.sab = 'RXNORM'
      AND c.tty = 'IN';

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 15', 'Reinsert from deleted completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 15', 'Error in Step 15: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 27: Step 16 - Delete Marked Rows
CREATE OR REPLACE FUNCTION faers_b.step_16_delete_marked_rows() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 16', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 16');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 16', 'Table faers_b.drug_mapper_2 is empty, skipping Step 16');
        RETURN;
    END IF;

    DELETE FROM faers_b.drug_mapper_2
    WHERE remapping_notes = 'TO BE DELETED';

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 16', 'Delete marked rows completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 16', 'Error in Step 16: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 28: Step 17 - Clean Duplicates
CREATE OR REPLACE FUNCTION faers_b.step_17_clean_duplicates() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 17', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 17');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 17', 'Table faers_b.drug_mapper_2 is empty, skipping Step 17');
        RETURN;
    END IF;

    DELETE FROM faers_b.drug_mapper_2
    WHERE (drug_id, rxaui, remapping_rxaui) IN (
        SELECT drug_id, rxaui, remapping_rxaui
        FROM (
            SELECT drug_id, rxaui, remapping_rxaui,
                   ROW_NUMBER() OVER (PARTITION BY drug_id, rxaui, remapping_rxaui ORDER BY drug_id, rxaui, remapping_rxaui) AS row_num
            FROM faers_b.drug_mapper_2
        ) t
        WHERE row_num > 1
    );

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 17', 'Clean duplicates completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 17', 'Error in Step 17: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 29: Step 18 - Update RXAUI Mappings
CREATE OR REPLACE FUNCTION faers_b.step_18_update_rxaui_mappings() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 18', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 18');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 18', 'Table faers_b.drug_mapper_2 is empty, skipping Step 18');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 18', 'Table faers_b.rxnconso does not exist, skipping Step 18');
        RETURN;
    END IF;

    UPDATE faers_b.drug_mapper_2
    SET remapping_rxcui = c.rxcui,
        remapping_str = c.str,
        remapping_sab = c.sab,
        remapping_tty = c.tty,
        remapping_code = c.code
    FROM faers_b.rxnconso c
    WHERE drug_mapper_2.remapping_rxaui = c.rxaui
      AND drug_mapper_2.remapping_rxaui IS NOT NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 18', 'Update RXAUI mappings completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 18', 'Error in Step 18: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 30: Step 19 - Non-RXNORM SAB Update
CREATE OR REPLACE FUNCTION faers_b.step_19_non_rxnorm_sab_update() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 19', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 19');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 19', 'Table faers_b.drug_mapper_2 is empty, skipping Step 19');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 19', 'Table faers_b.rxnconso does not exist, skipping Step 19');
        RETURN;
    END IF;

    UPDATE faers_b.drug_mapper_2
    SET remapping_rxaui = c.rxaui,
        remapping_rxcui = c.rxcui,
        remapping_str = c.str,
        remapping_sab = c.sab,
        remapping_tty = c.tty,
        remapping_code = c.code,
        remapping_notes = '14'
    FROM faers_b.rxnconso c
    WHERE drug_mapper_2.remapping_rxcui = c.rxcui
      AND c.sab = 'RXNORM'
      AND c.tty = 'IN'
      AND drug_mapper_2.sab != 'RXNORM';

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 19', 'Non-RXNORM SAB update completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 19', 'Error in Step 19: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 31: Step 20 - RXNORM SAB Specific Update
CREATE OR REPLACE FUNCTION faers_b.step_20_rxnorm_sab_specific_update() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 20', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 20');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 20', 'Table faers_b.drug_mapper_2 is empty, skipping Step 20');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 20', 'Table faers_b.rxnconso does not exist, skipping Step 20');
        RETURN;
    END IF;

    UPDATE faers_b.drug_mapper_2
    SET remapping_rxaui = c.rxaui,
        remapping_rxcui = c.rxcui,
        remapping_str = c.str,
        remapping_sab = c.sab,
        remapping_tty = c.tty,
        remapping_code = c.code,
        remapping_notes = '15'
    FROM faers_b.rxnconso c
    WHERE drug_mapper_2.remapping_rxcui = c.rxcui
      AND c.sab = 'RXNORM'
      AND c.tty = 'IN'
      AND drug_mapper_2.sab = 'RXNORM'
      AND drug_mapper_2.tty NOT IN ('IN', 'MIN');

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 20', 'RXNORM SAB specific update completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 20', 'Error in Step 20: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 32: Populate manual_remapper
CREATE OR REPLACE FUNCTION faers_b.populate_manual_remapper() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Populate Manual Remapper', 'Table faers_b.drug_mapper_2 does not exist, skipping');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Populate Manual Remapper', 'Table faers_b.drug_mapper_2 is empty, skipping');
        RETURN;
    END IF;

    INSERT INTO faers_b.manual_remapper
    (count, source_drugname, source_rxaui, source_rxcui, source_sab, source_tty)
    SELECT COUNT(drugname) AS count, drugname, remapping_rxaui, remapping_rxcui, remapping_sab, remapping_tty
    FROM faers_b.drug_mapper_2
    WHERE remapping_notes IS NOT NULL
    GROUP BY drugname, remapping_rxaui, remapping_rxcui, remapping_sab, remapping_tty;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Populate Manual Remapper', 'manual_remapper populated successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Populate Manual Remapper', 'Error in Populate Manual Remapper: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 33: Merge manual remappings into drug_mapper_3
CREATE OR REPLACE FUNCTION faers_b.merge_manual_remappings() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
    row RECORD;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'manual_remapper'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Merge Manual Remappings', 'Table faers_b.manual_remapper does not exist, skipping');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.manual_remapper;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Merge Manual Remappings', 'Table faers_b.manual_remapper is empty, skipping');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Merge Manual Remappings', 'Table faers_b.drug_mapper_2 does not exist, skipping');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Merge Manual Remappings', 'Table faers_b.drug_mapper_2 is empty, skipping');
        RETURN;
    END IF;

    INSERT INTO faers_b.drug_mapper_3
    SELECT d.drug_id, d.primaryid, d.drug_seq, d.role_cod, d.period, d.drugname, d.prod_ai, d.notes,
           d.rxaui, d.rxcui, d.str, d.sab, d.tty, d.code,
           d.remapping_rxaui, d.remapping_rxcui, d.remapping_str, d.remapping_sab, d.remapping_tty,
           d.remapping_code, d.remapping_notes
    FROM faers_b.drug_mapper_2 d
    LEFT JOIN faers_b.manual_remapper m
    ON d.drugname = m.source_drugname
    AND d.remapping_rxaui = m.source_rxaui
    AND d.remapping_rxcui = m.source_rxcui
    AND d.remapping_sab = m.source_sab
    AND d.remapping_tty = m.source_tty
    WHERE m.final_rxaui IS NOT NULL
      OR d.remapping_notes IS NOT NULL;

    FOR row IN (SELECT * FROM faers_b.manual_remapper WHERE final_rxaui IS NOT NULL)
    LOOP
        UPDATE faers_b.drug_mapper_3
        SET remapping_rxaui = row.final_rxaui::VARCHAR(8),
            remapping_notes = COALESCE(remapping_notes, '') || ' (Manual Remapped)'
        WHERE drugname = row.source_drugname
          AND remapping_rxaui = row.source_rxaui
          AND remapping_rxcui = row.source_rxcui
          AND remapping_sab = row.source_sab
          AND remapping_tty = row.source_tty;
    END LOOP;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Merge Manual Remappings', 'Merged manual remappings into drug_mapper_3 successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Merge Manual Remappings', 'Error in Merge Manual Remappings: ' || SQLERRM);
        RAISE;
END;
========
-- s10.sql: Create and set up remapping tables in faers_b schema

-- Statement 1: Verify database context
DO $$
BEGIN
    IF current_database() != 'faersdatabase' THEN
        RAISE EXCEPTION 'Must be connected to faersdatabase, current database is %', current_database();
    END IF;
END $$;

-- Statement 2: Ensure faers_b schema exists
CREATE SCHEMA IF NOT EXISTS faers_b AUTHORIZATION postgres;

-- Statement 3: Verify schema exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'faers_b') THEN
        RAISE EXCEPTION 'Schema faers_b failed to create';
    END IF;
END $$;

-- Statement 4: Grant privileges
GRANT ALL ON SCHEMA faers_b TO postgres;

-- Statement 5: Set search path
SET search_path TO faers_b, faers_combined, public;

-- Statement 6: Create logging table
CREATE TABLE IF NOT EXISTS faers_b.remapping_log (
    log_id SERIAL PRIMARY KEY,
    step VARCHAR(50),
    message TEXT,
    log_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Statement 7: Create drug_mapper table
CREATE TABLE IF NOT EXISTS faers_b.drug_mapper (
    drug_id TEXT,
    primaryid TEXT,
    drug_seq TEXT,
    role_cod TEXT,
    period TEXT,
    drugname TEXT,
    prod_ai TEXT,
    notes TEXT,
    rxaui VARCHAR(8),
    rxcui VARCHAR(8),
    str TEXT,
    sab VARCHAR(20),
    tty VARCHAR(20),
    code VARCHAR(50),
    remapping_rxaui VARCHAR(8),
    remapping_rxcui VARCHAR(8),
    remapping_str TEXT,
    remapping_sab VARCHAR(20),
    remapping_tty VARCHAR(20),
    remapping_code VARCHAR(50),
    remapping_notes TEXT
);

-- Statement 8: Create drug_mapper_2 table
CREATE TABLE IF NOT EXISTS faers_b.drug_mapper_2 (
    drug_id TEXT,
    primaryid TEXT,
    drug_seq TEXT,
    role_cod TEXT,
    period TEXT,
    drugname TEXT,
    prod_ai TEXT,
    notes TEXT,
    rxaui VARCHAR(8),
    rxcui VARCHAR(8),
    str TEXT,
    sab VARCHAR(20),
    tty VARCHAR(20),
    code VARCHAR(50),
    remapping_notes TEXT,
    rela TEXT,
    remapping_rxaui VARCHAR(8),
    remapping_rxcui VARCHAR(8),
    remapping_str TEXT,
    remapping_sab VARCHAR(20),
    remapping_tty VARCHAR(20),
    remapping_code VARCHAR(50)
);

-- Statement 9: Create drug_mapper_3 table
CREATE TABLE IF NOT EXISTS faers_b.drug_mapper_3 (
    drug_id TEXT,
    primaryid TEXT,
    drug_seq TEXT,
    role_cod TEXT,
    period TEXT,
    drugname TEXT,
    prod_ai TEXT,
    notes TEXT,
    rxaui VARCHAR(8),
    rxcui VARCHAR(8),
    str TEXT,
    sab VARCHAR(20),
    tty VARCHAR(20),
    code VARCHAR(50),
    remapping_rxaui VARCHAR(8),
    remapping_rxcui VARCHAR(8),
    remapping_str TEXT,
    remapping_sab VARCHAR(20),
    remapping_tty VARCHAR(20),
    remapping_code VARCHAR(50),
    remapping_notes TEXT
);

-- Statement 10: Create manual_remapper table
CREATE TABLE IF NOT EXISTS faers_b.manual_remapper (
    count INTEGER,
    source_drugname VARCHAR(3000),
    source_rxaui VARCHAR(8),
    source_rxcui VARCHAR(8),
    source_sab VARCHAR(20),
    source_tty VARCHAR(20),
    final_rxaui BIGINT,
    notes VARCHAR(100)
);

-- Statement 11: Create indexes for performance
DO $$
BEGIN
    IF EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'rxnconso'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_rxnconso_rxcui ON faers_b.rxnconso(rxcui) INCLUDE (rxaui, str, sab, tty, code);
        CREATE INDEX IF NOT EXISTS idx_rxnconso_rxaui ON faers_b.rxnconso(rxaui);
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Create Indexes', 'Created indexes on rxnconso');
    ELSE
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Create Indexes', 'Table rxnconso does not exist, skipping index creation');
    END IF;

    IF EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'rxnrel'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_rxnrel_rxcui ON faers_b.rxnrel(rxcui1, rxcui2) INCLUDE (rxaui1, rxaui2, rela);
        CREATE INDEX IF NOT EXISTS idx_rxnrel_rxaui ON faers_b.rxnrel(rxaui1, rxaui2);
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Create Indexes', 'Created indexes on rxnrel');
    ELSE
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Create Indexes', 'Table rxnrel does not exist, skipping index creation');
    END IF;

    IF EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'drug_mapper'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_drug_mapper_remapping ON faers_b.drug_mapper(remapping_rxcui, remapping_rxaui) INCLUDE (drug_id, remapping_notes);
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Create Indexes', 'Created indexes on drug_mapper');
    ELSE
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Create Indexes', 'Table drug_mapper does not exist, skipping index creation');
    END IF;
END $$;

-- Statement 12: Step 1 - Initial RXNORM Update
CREATE OR REPLACE FUNCTION faers_b.step_1_initial_rxnorm_update() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 1', 'Table faers_b.drug_mapper does not exist, skipping Step 1');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 1', 'Table faers_b.drug_mapper is empty, skipping Step 1');
        RETURN;
    END IF;

    UPDATE faers_b.drug_mapper
    SET remapping_rxcui = rxcui,
        remapping_rxaui = rxaui,
        remapping_str = str,
        remapping_sab = sab,
        remapping_tty = tty,
        remapping_code = code,
        remapping_notes = '1'
    FROM faers_b.rxnconso
    WHERE faers_b.drug_mapper.rxcui = faers_b.rxnconso.rxcui
      AND faers_b.rxnconso.sab = 'RXNORM'
      AND faers_b.rxnconso.tty = 'IN'
      AND faers_b.drug_mapper.remapping_notes IS NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 1', 'Initial RXNORM update completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 1', 'Error in Step 1: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 13: Step 2 - Create drug_mapper_2
CREATE OR REPLACE FUNCTION faers_b.step_2_create_drug_mapper_2() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 2', 'Table faers_b.drug_mapper does not exist, skipping Step 2');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 2', 'Table faers_b.drug_mapper is empty, skipping Step 2');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 2', 'Table faers_b.rxnconso does not exist, skipping Step 2');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnrel'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 2', 'Table faers_b.rxnrel does not exist, skipping Step 2');
        RETURN;
    END IF;

    INSERT INTO faers_b.drug_mapper_2
    SELECT c.drug_id, c.primaryid, c.drug_seq, c.role_cod, c.period, c.drugname, c.prod_ai, c.notes,
           c.rxaui, c.rxcui, c.str, c.sab, c.tty, c.code,
           CASE WHEN a.rxaui IS NULL THEN c.remapping_notes ELSE '2' END AS remapping_notes,
           b.rela,
           CASE WHEN a.rxaui IS NULL THEN c.remapping_rxaui ELSE a.rxaui END AS remapping_rxaui,
           CASE WHEN a.rxcui IS NULL THEN c.remapping_rxcui ELSE a.rxcui END AS remapping_rxcui,
           CASE WHEN a.str IS NULL THEN c.remapping_str ELSE a.str END AS remapping_str,
           CASE WHEN a.sab IS NULL THEN c.remapping_sab ELSE a.sab END AS remapping_sab,
           CASE WHEN a.tty IS NULL THEN c.remapping_tty ELSE a.tty END AS remapping_tty,
           CASE WHEN a.code IS NULL THEN c.remapping_code ELSE a.code END AS remapping_code
    FROM faers_b.rxnconso a
    INNER JOIN faers_b.rxnrel b ON a.rxcui = b.rxcui1 AND a.tty = 'IN' AND a.sab = 'RXNORM'
    RIGHT OUTER JOIN faers_b.drug_mapper c ON b.rxcui2 = c.rxcui
    WHERE c.remapping_notes IS NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 2', 'Created drug_mapper_2 successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 2', 'Error in Step 2: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 14: Step 3 - Manual Remapping Update
CREATE OR REPLACE FUNCTION faers_b.step_3_manual_remapping_update() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 3', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 3');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 3', 'Table faers_b.drug_mapper_2 is empty, skipping Step 3');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'hopefully_last_one_5_7_2021'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 3', 'Table faers_b.hopefully_last_one_5_7_2021 does not exist, skipping Step 3');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 3', 'Table faers_b.rxnconso does not exist, skipping Step 3');
        RETURN;
    END IF;

    UPDATE faers_b.drug_mapper_2
    SET remapping_notes = 'MAN_REM /',
        remapping_rxaui = h.last_rxaui,
        remapping_rxcui = r.rxcui,
        remapping_str = r.str,
        remapping_sab = r.sab,
        remapping_tty = r.tty,
        remapping_code = r.code
    FROM faers_b.hopefully_last_one_5_7_2021 h
    INNER JOIN faers_b.rxnconso r ON h.last_rxaui = r.rxaui
    WHERE drug_mapper_2.str = h.str
      AND drug_mapper_2.rxaui = h.rxaui
      AND drug_mapper_2.remapping_notes IS NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 3', 'Manual remapping update completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 3', 'Error in Step 3: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 15: Step 4 - Manual Remapping Insert
CREATE OR REPLACE FUNCTION faers_b.step_4_manual_remapping_insert() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 4', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 4');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 4', 'Table faers_b.drug_mapper_2 is empty, skipping Step 4');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'hopefully_last_one_5_7_2021'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 4', 'Table faers_b.hopefully_last_one_5_7_2021 does not exist, skipping Step 4');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 4', 'Table faers_b.rxnconso does not exist, skipping Step 4');
        RETURN;
    END IF;

    INSERT INTO faers_b.drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui,
     str, sab, tty, code, remapping_notes, rela, remapping_rxaui, remapping_rxcui, remapping_sab,
     remapping_tty, remapping_code, remapping_str)
    SELECT d.drug_id, d.primaryid, d.drug_seq, d.role_cod, d.period, d.drugname, d.prod_ai, d.notes,
           d.rxaui, d.rxcui, d.str, d.sab, d.tty, d.code,
           'MAN2/' || d.remapping_notes,
           d.rela,
           h.last_rxaui AS remapping_rxaui,
           r.rxcui AS remapping_rxcui,
           r.sab AS remapping_sab,
           r.tty AS remapping_tty,
           r.code AS remapping_code,
           r.str AS remapping_str
    FROM faers_b.drug_mapper_2 d
    INNER JOIN faers_b.hopefully_last_one_5_7_2021 h ON h.rxaui = d.rxaui AND h.str = d.str
    INNER JOIN faers_b.rxnconso r ON h.last_rxaui = r.rxaui
    WHERE h.last_rxaui IS NOT NULL
      AND d.remapping_notes LIKE 'MAN_REM /%';

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 4', 'Manual remapping insert completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 4', 'Error in Step 4: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 16: Step 5 - Manual Remapping Delete
CREATE OR REPLACE FUNCTION faers_b.step_5_manual_remapping_delete() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 5', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 5');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 5', 'Table faers_b.drug_mapper_2 is empty, skipping Step 5');
        RETURN;
    END IF;

    DELETE FROM faers_b.drug_mapper_2
    WHERE remapping_notes LIKE 'MAN_REM /%';

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 5', 'Manual remapping delete completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 5', 'Error in Step 5: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 17: Step 6 - VANDF Relationships
CREATE OR REPLACE FUNCTION faers_b.step_6_vandf_relationships() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 6', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 6');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 6', 'Table faers_b.drug_mapper_2 is empty, skipping Step 6');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 6', 'Table faers_b.rxnconso does not exist, skipping Step 6');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnrel'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 6', 'Table faers_b.rxnrel does not exist, skipping Step 6');
        RETURN;
    END IF;

    INSERT INTO faers_b.drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui, str, sab,
     tty, code, rela, remapping_notes, remapping_rxaui, remapping_rxcui, remapping_str, remapping_sab,
     remapping_tty, remapping_code)
    SELECT e.drug_id, e.primaryid, e.drug_seq, e.role_cod, e.period, e.drugname, e.prod_ai, e.notes,
           e.rxaui, e.rxcui, e.str, e.sab, e.tty, e.code, e.rela, '3',
           a.rxaui AS remapping_rxaui,
           a.rxcui AS remapping_rxcui,
           a.str AS remapping_str,
           a.sab AS remapping_sab,
           a.tty AS remapping_tty,
           a.code AS remapping_code
    FROM faers_b.rxnconso a
    INNER JOIN faers_b.rxnrel b ON a.rxcui = b.rxcui1 AND a.tty = 'IN' AND a.sab = 'RXNORM'
    INNER JOIN faers_b.rxnconso c ON b.rxcui2 = c.rxcui
    INNER JOIN faers_b.rxnrel d ON c.rxcui = d.rxcui1 AND d.rela = 'HAS_INGREDIENTS' AND c.sab = 'VANDF' AND c.tty = 'IN'
    INNER JOIN faers_b.drug_mapper_2 e ON d.rxcui2 = e.rxcui
    WHERE e.remapping_notes IS NULL;

    DELETE FROM faers_b.drug_mapper_2
    WHERE drug_id IN (
        SELECT drug_id FROM faers_b.drug_mapper_2 WHERE remapping_notes = '3'
    ) AND remapping_notes IS NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 6', 'VANDF relationships completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 6', 'Error in Step 6: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 18: Step 7 - MMSL to RXNORM Insert
CREATE OR REPLACE FUNCTION faers_b.step_7_mmsl_to_rxnorm_insert() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 7', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 7');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 7', 'Table faers_b.drug_mapper_2 is empty, skipping Step 7');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 7', 'Table faers_b.rxnconso does not exist, skipping Step 7');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnrel'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 7', 'Table faers_b.rxnrel does not exist, skipping Step 7');
        RETURN;
    END IF;

    INSERT INTO faers_b.drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui, str, sab,
     tty, code, remapping_notes, remapping_rxaui, remapping_rxcui, remapping_str, remapping_sab,
     remapping_tty, remapping_code)
    SELECT DISTINCT e.drug_id, e.primaryid, e.drug_seq, e.role_cod, e.period, e.drugname, e.prod_ai, e.notes,
           e.rxaui, e.rxcui, e.str, e.sab, e.tty, e.code, '7',
           c1.rxaui AS remapping_rxaui,
           c1.rxcui AS remapping_rxcui,
           c1.str AS remapping_str,
           c1.sab AS remapping_sab,
           c1.tty AS remapping_tty,
           c1.code AS remapping_code
    FROM faers_b.drug_mapper_2 e
    INNER JOIN faers_b.rxnrel r ON e.rxcui = r.rxcui2
    INNER JOIN faers_b.rxnconso c ON r.rxcui1 = c.rxcui
    INNER JOIN faers_b.rxnrel r1 ON r.rxcui1 = r1.rxcui2
    INNER JOIN faers_b.rxnconso c1 ON r1.rxcui1 = c1.rxcui
    WHERE e.sab = 'MMSL'
      AND c1.sab = 'RXNORM'
      AND c1.tty = 'IN'
      AND c.sab = 'RXNORM'
      AND c.tty = 'SCDC'
      AND c1.rxaui != '11794211'
      AND e.remapping_notes IS NULL;

    DELETE FROM faers_b.drug_mapper_2
    WHERE drug_id IN (
        SELECT drug_id FROM faers_b.drug_mapper_2 WHERE remapping_notes = '7'
    ) AND remapping_notes IS NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 7', 'MMSL to RXNORM insert completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 7', 'Error in Step 7: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 19: Step 8 - RXNORM SCDC to IN Insert
CREATE OR REPLACE FUNCTION faers_b.step_8_rxnorm_scdc_to_in_insert() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 8', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 8');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 8', 'Table faers_b.drug_mapper_2 is empty, skipping Step 8');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 8', 'Table faers_b.rxnconso does not exist, skipping Step 8');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnrel'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 8', 'Table faers_b.rxnrel does not exist, skipping Step 8');
        RETURN;
    END IF;

    INSERT INTO faers_b.drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui, str, sab,
     tty, code, remapping_notes, remapping_rxaui, remapping_rxcui, remapping_str, remapping_sab,
     remapping_tty, remapping_code)
    SELECT DISTINCT e.drug_id, e.primaryid, e.drug_seq, e.role_cod, e.period, e.drugname, e.prod_ai, e.notes,
           e.rxaui, e.rxcui, e.str, e.sab, e.tty, e.code, '8',
           c1.rxaui AS remapping_rxaui,
           c1.rxcui AS remapping_rxcui,
           c1.str AS remapping_str,
           c1.sab AS remapping_sab,
           c1.tty AS remapping_tty,
           c1.code AS remapping_code
    FROM faers_b.drug_mapper_2 e
    INNER JOIN faers_b.rxnrel r ON e.rxcui = r.rxcui2
    INNER JOIN faers_b.rxnconso c ON r.rxcui1 = c.rxcui
    INNER JOIN faers_b.rxnrel r1 ON r.rxcui1 = r1.rxcui2
    INNER JOIN faers_b.rxnconso c1 ON r1.rxcui1 = c1.rxcui
    WHERE c.sab = 'RXNORM'
      AND c.tty = 'SCDC'
      AND c1.sab = 'RXNORM'
      AND c1.tty = 'IN'
      AND e.remapping_notes IS NULL;

    DELETE FROM faers_b.drug_mapper_2
    WHERE drug_id IN (
        SELECT drug_id FROM faers_b.drug_mapper_2 WHERE remapping_notes = '8'
    ) AND remapping_notes IS NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 8', 'RXNORM SCDC to IN insert completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 8', 'Error in Step 8: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 20: Step 9 - RXNORM IN Update with Notes
CREATE OR REPLACE FUNCTION faers_b.step_9_rxnorm_in_update_with_notes() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 9', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 9');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 9', 'Table faers_b.drug_mapper_2 is empty, skipping Step 9');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 9', 'Table faers_b.rxnconso does not exist, skipping Step 9');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnrel'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 9', 'Table faers_b.rxnrel does not exist, skipping Step 9');
        RETURN;
    END IF;

    UPDATE faers_b.drug_mapper_2
    SET remapping_rxcui = c.rxcui,
        remapping_rxaui = c.rxaui,
        remapping_str = c.str,
        remapping_sab = c.sab,
        remapping_tty = c.tty,
        remapping_code = c.code,
        remapping_notes = '9'
    FROM faers_b.rxnconso c
    RIGHT OUTER JOIN faers_b.rxnrel r ON c.rxaui = r.rxaui1
    RIGHT OUTER JOIN faers_b.drug_mapper_2 ON r.rxaui2 = drug_mapper_2.rxaui
    WHERE c.sab = 'RXNORM'
      AND c.tty = 'IN'
      AND drug_mapper_2.notes IS NOT NULL
      AND drug_mapper_2.remapping_notes IS NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 9', 'RXNORM IN update with notes completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 9', 'Error in Step 9: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 21: Step 10 - MTHSPL to RXNORM IN Insert
CREATE OR REPLACE FUNCTION faers_b.step_10_mthspl_to_rxnorm_in_insert() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 10', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 10');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 10', 'Table faers_b.drug_mapper_2 is empty, skipping Step 10');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 10', 'Table faers_b.rxnconso does not exist, skipping Step 10');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnrel'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 10', 'Table faers_b.rxnrel does not exist, skipping Step 10');
        RETURN;
    END IF;

    INSERT INTO faers_b.drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui, str, sab,
     tty, code, remapping_notes, remapping_rxaui, remapping_rxcui, remapping_str, remapping_sab,
     remapping_tty, remapping_code)
    SELECT DISTINCT e.drug_id, e.primaryid, e.drug_seq, e.role_cod, e.period, e.drugname, e.prod_ai, e.notes,
           e.rxaui, e.rxcui, e.str, e.sab, e.tty, e.code, '9',
           c1.rxaui AS remapping_rxaui,
           c1.rxcui AS remapping_rxcui,
           c1.str AS remapping_str,
           c1.sab AS remapping_sab,
           c1.tty AS remapping_tty,
           c1.code AS remapping_code
    FROM faers_b.drug_mapper_2 e
    INNER JOIN faers_b.rxnrel r ON e.rxaui = r.rxaui2
    INNER JOIN faers_b.rxnconso c ON r.rxaui1 = c.rxaui
    INNER JOIN faers_b.rxnrel r1 ON r.rxaui1 = r1.rxaui2 AND r.rela = 'HAS_ACTIVE_MOIETY'
    INNER JOIN faers_b.rxnconso c1 ON r1.rxaui1 = c1.rxaui
    WHERE c1.sab = 'RXNORM'
      AND c1.tty = 'IN'
      AND c.sab = 'MTHSPL'
      AND e.remapping_notes IS NULL;

    DELETE FROM faers_b.drug_mapper_2
    WHERE drug_id IN (
        SELECT drug_id FROM faers_b.drug_mapper_2 WHERE remapping_notes = '9'
    ) AND remapping_notes IS NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 10', 'MTHSPL to RXNORM IN insert completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 10', 'Error in Step 10: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 22: Step 11 - RXNORM IN Update
CREATE OR REPLACE FUNCTION faers_b.step_11_rxnorm_in_update() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 11', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 11');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 11', 'Table faers_b.drug_mapper_2 is empty, skipping Step 11');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 11', 'Table faers_b.rxnconso does not exist, skipping Step 11');
        RETURN;
    END IF;

    UPDATE faers_b.drug_mapper_2
    SET remapping_rxcui = c.rxcui,
        remapping_rxaui = c.rxaui,
        remapping_str = c.str,
        remapping_sab = c.sab,
        remapping_tty = c.tty,
        remapping_code = c.code,
        remapping_notes = '10'
    FROM faers_b.rxnconso c
    INNER JOIN faers_b.drug_mapper_2 ON drug_mapper_2.rxcui = c.rxcui
    WHERE c.tty = 'IN'
      AND drug_mapper_2.remapping_notes IS NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 11', 'RXNORM IN update completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 11', 'Error in Step 11: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 23: Step 12 - MMSL to RXNORM IN Insert with Exclusions
CREATE OR REPLACE FUNCTION faers_b.step_12_mmsl_to_rxnorm_in_insert_exclusions() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 12', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 12');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 12', 'Table faers_b.drug_mapper_2 is empty, skipping Step 12');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 12', 'Table faers_b.rxnconso does not exist, skipping Step 12');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnrel'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 12', 'Table faers_b.rxnrel does not exist, skipping Step 12');
        RETURN;
    END IF;

    INSERT INTO faers_b.drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui, str, sab,
     tty, code, remapping_notes, remapping_rxaui, remapping_rxcui, remapping_str, remapping_sab,
     remapping_tty, remapping_code)
    SELECT DISTINCT e.drug_id, e.primaryid, e.drug_seq, e.role_cod, e.period, e.drugname, e.prod_ai, e.notes,
           e.rxaui, e.rxcui, e.str, e.sab, e.tty, e.code, '11',
           c1.rxaui AS remapping_rxaui,
           c1.rxcui AS remapping_rxcui,
           c1.str AS remapping_str,
           c1.sab AS remapping_sab,
           c1.tty AS remapping_tty,
           c1.code AS remapping_code
    FROM faers_b.drug_mapper_2 e
    INNER JOIN faers_b.rxnrel r ON e.rxaui = r.rxaui2
    INNER JOIN faers_b.rxnconso c ON r.rxaui1 = c.rxaui
    INNER JOIN faers_b.rxnrel r1 ON r.rxaui1 = r1.rxaui2
    INNER JOIN faers_b.rxnconso c1 ON r1.rxaui1 = c1.rxaui
    WHERE c1.sab = 'MMSL'
      AND c1.tty = 'IN'
      AND e.sab = 'MMSL'
      AND c1.rxaui NOT IN ('2604414', '1182299', '1173735', '1287235')
      AND e.remapping_notes IS NULL;

    DELETE FROM faers_b.drug_mapper_2
    WHERE drug_id IN (
        SELECT drug_id FROM faers_b.drug_mapper_2 WHERE remapping_notes = '11'
    ) AND remapping_notes IS NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 12', 'MMSL to RXNORM IN insert with exclusions completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 12', 'Error in Step 12: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 24: Step 13 - RXNORM Cleanup Update
CREATE OR REPLACE FUNCTION faers_b.step_13_rxnorm_cleanup_update() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 13', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 13');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 13', 'Table faers_b.drug_mapper_2 is empty, skipping Step 13');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 13', 'Table faers_b.rxnconso does not exist, skipping Step 13');
        RETURN;
    END IF;

    UPDATE faers_b.drug_mapper_2
    SET remapping_notes = '12',
        remapping_rxaui = c.rxaui,
        remapping_rxcui = c.rxcui,
        remapping_str = c.str,
        remapping_sab = c.sab,
        remapping_tty = c.tty,
        remapping_code = c.code
    FROM faers_b.rxnconso c
    WHERE c.str = CASE
        WHEN POSITION('(' IN drug_mapper_2.remapping_str) > 0
             AND POSITION(')' IN drug_mapper_2.remapping_str) > 0
             AND POSITION('(' IN drug_mapper_2.remapping_str) < POSITION(')' IN drug_mapper_2.remapping_str)
        THEN REGEXP_REPLACE(drug_mapper_2.remapping_str, '\(.*?\)', '')
        ELSE drug_mapper_2.remapping_str
        END
      AND c.sab = 'RXNORM'
      AND c.tty IN ('IN', 'MIN', 'PIN')
      AND drug_mapper_2.remapping_notes IN ('9', '10', '11');

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 13', 'RXNORM cleanup update completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 13', 'Error in Step 13: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 25: Step 14 - Mark for Deletion
CREATE OR REPLACE FUNCTION faers_b.step_14_mark_for_deletion() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 14', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 14');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 14', 'Table faers_b.drug_mapper_2 is empty, skipping Step 14');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 14', 'Table faers_b.rxnconso does not exist, skipping Step 14');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnrel'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 14', 'Table faers_b.rxnrel does not exist, skipping Step 14');
        RETURN;
    END IF;

    UPDATE faers_b.drug_mapper_2
    SET remapping_notes = 'TO BE DELETED'
    FROM faers_b.rxnrel r
    INNER JOIN faers_b.rxnconso c ON r.rxcui1 = c.rxcui
    WHERE drug_mapper_2.remapping_rxcui = r.rxcui2
      AND c.sab = 'RXNORM'
      AND c.tty = 'IN'
      AND drug_mapper_2.remapping_notes = '12';

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 14', 'Mark for deletion completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 14', 'Error in Step 14: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 26: Step 15 - Reinsert from Deleted
CREATE OR REPLACE FUNCTION faers_b.step_15_reinsert_from_deleted() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 15', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 15');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 15', 'Table faers_b.drug_mapper_2 is empty, skipping Step 15');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 15', 'Table faers_b.rxnconso does not exist, skipping Step 15');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnrel'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 15', 'Table faers_b.rxnrel does not exist, skipping Step 15');
        RETURN;
    END IF;

    INSERT INTO faers_b.drug_mapper_2
    (drug_id, primaryid, drug_seq, role_cod, period, drugname, prod_ai, notes, rxaui, rxcui, str, sab,
     tty, code, rela, remapping_notes, remapping_rxcui, remapping_sab, remapping_tty, remapping_code,
     remapping_str, remapping_rxaui)
    SELECT d.drug_id, d.primaryid, d.drug_seq, d.role_cod, d.period, d.drugname, d.prod_ai, d.notes,
           d.rxaui, d.rxcui, d.str, d.sab, d.tty, d.code, d.rela, '13',
           c.rxcui AS remapping_rxcui,
           c.sab AS remapping_sab,
           c.tty AS remapping_tty,
           c.code AS remapping_code,
           c.str AS remapping_str,
           c.rxaui AS remapping_rxaui
    FROM faers_b.drug_mapper_2 d
    INNER JOIN faers_b.rxnrel r ON d.remapping_rxcui = r.rxcui2
    INNER JOIN faers_b.rxnconso c ON r.rxcui1 = c.rxcui
    WHERE d.remapping_notes = 'TO BE DELETED'
      AND c.sab = 'RXNORM'
      AND c.tty = 'IN';

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 15', 'Reinsert from deleted completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 15', 'Error in Step 15: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 27: Step 16 - Delete Marked Rows
CREATE OR REPLACE FUNCTION faers_b.step_16_delete_marked_rows() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 16', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 16');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 16', 'Table faers_b.drug_mapper_2 is empty, skipping Step 16');
        RETURN;
    END IF;

    DELETE FROM faers_b.drug_mapper_2
    WHERE remapping_notes = 'TO BE DELETED';

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 16', 'Delete marked rows completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 16', 'Error in Step 16: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 28: Step 17 - Clean Duplicates
CREATE OR REPLACE FUNCTION faers_b.step_17_clean_duplicates() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 17', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 17');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 17', 'Table faers_b.drug_mapper_2 is empty, skipping Step 17');
        RETURN;
    END IF;

    DELETE FROM faers_b.drug_mapper_2
    WHERE (drug_id, rxaui, remapping_rxaui) IN (
        SELECT drug_id, rxaui, remapping_rxaui
        FROM (
            SELECT drug_id, rxaui, remapping_rxaui,
                   ROW_NUMBER() OVER (PARTITION BY drug_id, rxaui, remapping_rxaui ORDER BY drug_id, rxaui, remapping_rxaui) AS row_num
            FROM faers_b.drug_mapper_2
        ) t
        WHERE row_num > 1
    );

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 17', 'Clean duplicates completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 17', 'Error in Step 17: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 29: Step 18 - Update RXAUI Mappings
CREATE OR REPLACE FUNCTION faers_b.step_18_update_rxaui_mappings() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 18', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 18');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 18', 'Table faers_b.drug_mapper_2 is empty, skipping Step 18');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 18', 'Table faers_b.rxnconso does not exist, skipping Step 18');
        RETURN;
    END IF;

    UPDATE faers_b.drug_mapper_2
    SET remapping_rxcui = c.rxcui,
        remapping_str = c.str,
        remapping_sab = c.sab,
        remapping_tty = c.tty,
        remapping_code = c.code
    FROM faers_b.rxnconso c
    WHERE drug_mapper_2.remapping_rxaui = c.rxaui
      AND drug_mapper_2.remapping_rxaui IS NOT NULL;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 18', 'Update RXAUI mappings completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 18', 'Error in Step 18: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 30: Step 19 - Non-RXNORM SAB Update
CREATE OR REPLACE FUNCTION faers_b.step_19_non_rxnorm_sab_update() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 19', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 19');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 19', 'Table faers_b.drug_mapper_2 is empty, skipping Step 19');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 19', 'Table faers_b.rxnconso does not exist, skipping Step 19');
        RETURN;
    END IF;

    UPDATE faers_b.drug_mapper_2
    SET remapping_rxaui = c.rxaui,
        remapping_rxcui = c.rxcui,
        remapping_str = c.str,
        remapping_sab = c.sab,
        remapping_tty = c.tty,
        remapping_code = c.code,
        remapping_notes = '14'
    FROM faers_b.rxnconso c
    WHERE drug_mapper_2.remapping_rxcui = c.rxcui
      AND c.sab = 'RXNORM'
      AND c.tty = 'IN'
      AND drug_mapper_2.sab != 'RXNORM';

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 19', 'Non-RXNORM SAB update completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 19', 'Error in Step 19: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 31: Step 20 - RXNORM SAB Specific Update
CREATE OR REPLACE FUNCTION faers_b.step_20_rxnorm_sab_specific_update() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 20', 'Table faers_b.drug_mapper_2 does not exist, skipping Step 20');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 20', 'Table faers_b.drug_mapper_2 is empty, skipping Step 20');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'rxnconso'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 20', 'Table faers_b.rxnconso does not exist, skipping Step 20');
        RETURN;
    END IF;

    UPDATE faers_b.drug_mapper_2
    SET remapping_rxaui = c.rxaui,
        remapping_rxcui = c.rxcui,
        remapping_str = c.str,
        remapping_sab = c.sab,
        remapping_tty = c.tty,
        remapping_code = c.code,
        remapping_notes = '15'
    FROM faers_b.rxnconso c
    WHERE drug_mapper_2.remapping_rxcui = c.rxcui
      AND c.sab = 'RXNORM'
      AND c.tty = 'IN'
      AND drug_mapper_2.sab = 'RXNORM'
      AND drug_mapper_2.tty NOT IN ('IN', 'MIN');

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 20', 'RXNORM SAB specific update completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Step 20', 'Error in Step 20: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 32: Populate manual_remapper
CREATE OR REPLACE FUNCTION faers_b.populate_manual_remapper() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Populate Manual Remapper', 'Table faers_b.drug_mapper_2 does not exist, skipping');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Populate Manual Remapper', 'Table faers_b.drug_mapper_2 is empty, skipping');
        RETURN;
    END IF;

    INSERT INTO faers_b.manual_remapper
    (count, source_drugname, source_rxaui, source_rxcui, source_sab, source_tty)
    SELECT COUNT(drugname) AS count, drugname, remapping_rxaui, remapping_rxcui, remapping_sab, remapping_tty
    FROM faers_b.drug_mapper_2
    WHERE remapping_notes IS NOT NULL
    GROUP BY drugname, remapping_rxaui, remapping_rxcui, remapping_sab, remapping_tty;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Populate Manual Remapper', 'manual_remapper populated successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Populate Manual Remapper', 'Error in Populate Manual Remapper: ' || SQLERRM);
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Statement 33: Merge manual remappings into drug_mapper_3
CREATE OR REPLACE FUNCTION faers_b.merge_manual_remappings() RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_count BIGINT;
    row RECORD;
BEGIN
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'manual_remapper'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Merge Manual Remappings', 'Table faers_b.manual_remapper does not exist, skipping');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.manual_remapper;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Merge Manual Remappings', 'Table faers_b.manual_remapper is empty, skipping');
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'faers_b' AND table_name = 'drug_mapper_2'
    ) INTO table_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Merge Manual Remappings', 'Table faers_b.drug_mapper_2 does not exist, skipping');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO row_count FROM faers_b.drug_mapper_2;
    IF row_count = 0 THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Merge Manual Remappings', 'Table faers_b.drug_mapper_2 is empty, skipping');
        RETURN;
    END IF;

    INSERT INTO faers_b.drug_mapper_3
    SELECT d.drug_id, d.primaryid, d.drug_seq, d.role_cod, d.period, d.drugname, d.prod_ai, d.notes,
           d.rxaui, d.rxcui, d.str, d.sab, d.tty, d.code,
           d.remapping_rxaui, d.remapping_rxcui, d.remapping_str, d.remapping_sab, d.remapping_tty,
           d.remapping_code, d.remapping_notes
    FROM faers_b.drug_mapper_2 d
    LEFT JOIN faers_b.manual_remapper m
    ON d.drugname = m.source_drugname
    AND d.remapping_rxaui = m.source_rxaui
    AND d.remapping_rxcui = m.source_rxcui
    AND d.remapping_sab = m.source_sab
    AND d.remapping_tty = m.source_tty
    WHERE m.final_rxaui IS NOT NULL
      OR d.remapping_notes IS NOT NULL;

    FOR row IN (SELECT * FROM faers_b.manual_remapper WHERE final_rxaui IS NOT NULL)
    LOOP
        UPDATE faers_b.drug_mapper_3
        SET remapping_rxaui = row.final_rxaui::VARCHAR(8),
            remapping_notes = COALESCE(remapping_notes, '') || ' (Manual Remapped)'
        WHERE drugname = row.source_drugname
          AND remapping_rxaui = row.source_rxaui
          AND remapping_rxcui = row.source_rxcui
          AND remapping_sab = row.source_sab
          AND remapping_tty = row.source_tty;
    END LOOP;

    INSERT INTO faers_b.remapping_log (step, message) VALUES ('Merge Manual Remappings', 'Merged manual remappings into drug_mapper_3 successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.remapping_log (step, message) VALUES ('Merge Manual Remappings', 'Error in Merge Manual Remappings: ' || SQLERRM);
        RAISE;
END;
>>>>>>>> 36-bootstrapping-logging-framework:src/s10.sql
$$ LANGUAGE plpgsql;