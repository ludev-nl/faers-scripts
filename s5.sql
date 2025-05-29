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

-- Create logging table for errors and progress
CREATE TABLE IF NOT EXISTS faers_b.s5_log (
    log_id SERIAL PRIMARY KEY,
    step VARCHAR(50),
    message TEXT,
    log_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create drug_mapper table
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'drug_mapper'
    ) THEN
        CREATE TABLE faers_b.drug_mapper (
            drug_id INTEGER NOT NULL,
            primaryid BIGINT,
            caseid BIGINT,
            drug_seq BIGINT,
            role_cod VARCHAR(2),
            period VARCHAR(4),
            drugname VARCHAR(500),
            prod_ai VARCHAR(400),
            nda_num VARCHAR(200),
            notes VARCHAR(100),
            rxaui BIGINT,
            rxcui BIGINT,
            str VARCHAR(3000),
            sab VARCHAR(20),
            tty VARCHAR(20),
            code VARCHAR(50),
            remapping_notes VARCHAR(100),
            remapping_rxaui VARCHAR(8),
            remapping_rxcui VARCHAR(8),
            remapping_str VARCHAR(3000),
            remapping_sab VARCHAR(20),
            remapping_tty VARCHAR(20),
            remapping_code VARCHAR(50)
        );

        CREATE INDEX idx_drug_mapper_drugname ON faers_b.drug_mapper (drugname);

        INSERT INTO faers_b.s5_log (step, message) VALUES ('Create drug_mapper', 'Table faers_b.drug_mapper created successfully');
    ELSE
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Create drug_mapper', 'Table faers_b.drug_mapper already exists, skipping creation');
    END IF;
END $$;

-- Populate drug_mapper
DO $$
DECLARE
    table_exists BOOLEAN;
    aligned_exists BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_combined') 
        AND relname = 'drug_combined'
    ) INTO table_exists;

    SELECT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_combined') 
        AND relname = 'aligned_demo_drug_reac_indi_ther'
    ) INTO aligned_exists;

    IF NOT table_exists THEN
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Populate drug_mapper', 'Table faers_combined.drug_combined does not exist, skipping population');
        RETURN;
    END IF;

    IF NOT aligned_exists THEN
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Populate drug_mapper', 'Table faers_combined.aligned_demo_drug_reac_indi_ther does not exist, skipping population');
        RETURN;
    END IF;

    INSERT INTO faers_b.drug_mapper (drug_id, primaryid, caseid, drug_seq, role_cod, drugname, prod_ai, nda_num, period)
    SELECT drug_id, primaryid, caseid, drug_seq, role_cod, drugname, prod_ai, nda_num, period
    FROM faers_combined.drug_combined
    WHERE primaryid IN (SELECT primaryid FROM faers_combined.aligned_demo_drug_reac_indi_ther);

    INSERT INTO faers_b.s5_log (step, message) VALUES ('Populate drug_mapper', 'Populated faers_b.drug_mapper successfully');
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Populate drug_mapper', 'Error populating drug_mapper: ' || SQLERRM);
        RAISE;
END $$;

-- Create RxNorm tables
DO $$
BEGIN
    -- RXNATOMARCHIVE
    IF NOT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'rxnatomarchive'
    ) THEN
        CREATE TABLE faers_b.rxnatomarchive (
            rxaui VARCHAR(8) NOT NULL,
            aui VARCHAR(10),
            str VARCHAR(4000) NOT NULL,
            archive_timestamp VARCHAR(280) NOT NULL,
            created_timestamp VARCHAR(280) NOT NULL,
            updated_timestamp VARCHAR(280) NOT NULL,
            code VARCHAR(50),
            is_brand VARCHAR(1),
            lat VARCHAR(3),
            last_released VARCHAR(30),
            saui VARCHAR(50),
            vsab VARCHAR(40),
            rxcui VARCHAR(8),
            sab VARCHAR(20),
            tty VARCHAR(20),
            merged_to_rxcui VARCHAR(8)
        );
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Create rxnatomarchive', 'Table faers_b.rxnatomarchive created successfully');
    ELSE
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Create rxnatomarchive', 'Table faers_b.rxnatomarchive already exists, skipping creation');
    END IF;

    -- RXNCONSO
    IF NOT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'rxnconso'
    ) THEN
        CREATE TABLE faers_b.rxnconso (
            rxcui VARCHAR(8) NOT NULL,
            lat VARCHAR(3) DEFAULT 'ENG' NOT NULL,
            ts VARCHAR(1),
            lui VARCHAR(8),
            stt VARCHAR(3),
            sui VARCHAR(8),
            ispref VARCHAR(1),
            rxaui VARCHAR(8) NOT NULL,
            saui VARCHAR(50),
            scui VARCHAR(50),
            sdui VARCHAR(50),
            sab VARCHAR(20) NOT NULL,
            tty VARCHAR(20) NOT NULL,
            code VARCHAR(50) NOT NULL,
            str VARCHAR(3000) NOT NULL,
            srl VARCHAR(10),
            suppress VARCHAR(1),
            cvf VARCHAR(50)
        );
        CREATE INDEX idx_rxnconso_rxcui ON faers_b.rxnconso (rxcui);
        CREATE INDEX idx_rxnconso_rxaui ON faers_b.rxnconso (rxaui);
        CREATE INDEX idx_rxnconso_sab ON faers_b.rxnconso (sab);
        CREATE INDEX idx_rxnconso_tty ON faers_b.rxnconso (tty);
        CREATE INDEX idx_rxnconso_code ON faers_b.rxnconso (code);
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Create rxnconso', 'Table faers_b.rxnconso created successfully');
    ELSE
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Create rxnconso', 'Table faers_b.rxnconso already exists, skipping creation');
    END IF;

    -- RXNREL
    IF NOT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'rxnrel'
    ) THEN
        CREATE TABLE faers_b.rxnrel (
            rxcui1 VARCHAR(8),
            rxaui1 VARCHAR(8),
            stype1 VARCHAR(50),
            rel VARCHAR(4),
            rxcui2 VARCHAR(8),
            rxaui2 VARCHAR(8),
            stype2 VARCHAR(50),
            rela VARCHAR(100),
            rui VARCHAR(10),
            srui VARCHAR(50),
            sab VARCHAR(20) NOT NULL,
            sl VARCHAR(1000),
            dir VARCHAR(1),
            rg VARCHAR(10),
            suppress VARCHAR(1),
            cvf VARCHAR(50)
        );
        CREATE INDEX idx_rxnrel_rxcui1 ON faers_b.rxnrel (rxcui1);
        CREATE INDEX idx_rxnrel_rxcui2 ON faers_b.rxnrel (rxcui2);
        CREATE INDEX idx_rxnrel_rxaui1 ON faers_b.rxnrel (rxaui1);
        CREATE INDEX idx_rxnrel_rxaui2 ON faers_b.rxnrel (rxaui2);
        CREATE INDEX idx_rxnrel_rela ON faers_b.rxnrel (rela);
        CREATE INDEX idx_rxnrel_rel ON faers_b.rxnrel (rel);
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Create rxnrel', 'Table faers_b.rxnrel created successfully');
    ELSE
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Create rxnrel', 'Table faers_b.rxnrel already exists, skipping creation');
    END IF;

    -- RXNSAB
    IF NOT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'rxnsab'
    ) THEN
        CREATE TABLE faers_b.rxnsab (
            vcui VARCHAR(8),
            rcui VARCHAR(8),
            vsab VARCHAR(40),
            rsab VARCHAR(20) NOT NULL,
            son VARCHAR(3000),
            sf VARCHAR(20),
            sver VARCHAR(20),
            vstart VARCHAR(10),
            vend VARCHAR(10),
            imeta VARCHAR(10),
            rmeta VARCHAR(10),
            slc VARCHAR(1000),
            scc VARCHAR(1000),
            srl INTEGER,
            tfr INTEGER,
            cfr INTEGER,
            cxty VARCHAR(50),
            ttyl VARCHAR(300),
            atnl VARCHAR(1000),
            lat VARCHAR(3),
            cenc VARCHAR(20),
            curver VARCHAR(1),
            sabin VARCHAR(1),
            ssn VARCHAR(3000),
            scit VARCHAR(4000)
        );
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Create rxnsab', 'Table faers_b.rxnsab created successfully');
    ELSE
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Create rxnsab', 'Table faers_b.rxnsab already exists, skipping creation');
    END IF;

    -- RXNSAT
    IF NOT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'rxnsat'
    ) THEN
        CREATE TABLE faers_b.rxnsat (
            rxcui VARCHAR(8),
            lui VARCHAR(8),
            sui VARCHAR(8),
            rxaui VARCHAR(9),
            stype VARCHAR(50),
            code VARCHAR(50),
            atui VARCHAR(11),
            satui VARCHAR(50),
            atn VARCHAR(1000) NOT NULL,
            sab VARCHAR(20) NOT NULL,
            atv VARCHAR(4000),
            suppress VARCHAR(1),
            cvf VARCHAR(50)
        );
        CREATE INDEX idx_rxnsat_rxcui ON faers_b.rxnsat (rxcui);
        CREATE INDEX idx_rxnsat_rxaui ON faers_b.rxnsat (rxcui, rxaui);
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Create rxnsat', 'Table faers_b.rxnsat created successfully');
    ELSE
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Create rxnsat', 'Table faers_b.rxnsat already exists, skipping creation');
    END IF;

    -- RXNSTY
    IF NOT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'rxnsty'
    ) THEN
        CREATE TABLE faers_b.rxnsty (
            rxcui VARCHAR(8) NOT NULL,
            tui VARCHAR(4),
            stn VARCHAR(100),
            sty VARCHAR(50),
            atui VARCHAR(11),
            cvf VARCHAR(50)
        );
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Create rxnsty', 'Table faers_b.rxnsty created successfully');
    ELSE
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Create rxnsty', 'Table faers_b.rxnsty already exists, skipping creation');
    END IF;

    -- RXNDOC
    IF NOT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'rxndoc'
    ) THEN
        CREATE TABLE faers_b.rxndoc (
            dockey VARCHAR(50) NOT NULL,
            value VARCHAR(1000),
            type VARCHAR(50) NOT NULL,
            expl VARCHAR(1000)
        );
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Create rxndoc', 'Table faers_b.rxndoc created successfully');
    ELSE
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Create rxndoc', 'Table faers_b.rxndoc already exists, skipping creation');
    END IF;

    -- RXNCUICHANGES
    IF NOT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'rxncuichanges'
    ) THEN
        CREATE TABLE faers_b.rxncuichanges (
            rxaui VARCHAR(8),
            code VARCHAR(50),
            sab VARCHAR(20),
            tty VARCHAR(20),
            str VARCHAR(3000),
            old_rxcui VARCHAR(8) NOT NULL,
            new_rxcui VARCHAR(8) NOT NULL
        );
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Create rxncuichanges', 'Table faers_b.rxncuichanges created successfully');
    ELSE
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Create rxncuichanges', 'Table faers_b.rxncuichanges already exists, skipping creation');
    END IF;

    -- RXNCUI
    IF NOT EXISTS (
        SELECT FROM pg_class 
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'faers_b') 
        AND relname = 'rxncui'
    ) THEN
        CREATE TABLE faers_b.rxncui (
            cui1 VARCHAR(8),
            ver_start VARCHAR(40),
            ver_end VARCHAR(40),
            cardinality VARCHAR(8),
            cui2 VARCHAR(8)
        );
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Create rxncui', 'Table faers_b.rxncui created successfully');
    ELSE
        INSERT INTO faers_b.s5_log (step, message) VALUES ('Create rxncui', 'Table faers_b.rxncui already exists, skipping creation');
    END IF;
END $$;

-- Load RxNorm data (to be executed separately if files are available)
-- Note: \copy commands are commented out and should be run manually if files exist
DO $$
BEGIN
    INSERT INTO faers_b.s5_log (step, message) VALUES ('Load RxNorm Data', 'Run \copy commands manually when data files are available');
END $$;

-- Example \copy commands (uncomment and adjust paths as needed):
/*
\copy faers_b.rxnatomarchive FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNATOMARCHIVE.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);
\copy faers_b.rxnconso FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNCONSO.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);
\copy faers_b.rxnrel FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNREL.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);
\copy faers_b.rxnsab FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNSAB.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);
\copy faers_b.rxnsat FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNSAT.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);
\copy faers_b.rxnsty FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNSTY.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);
\copy faers_b.rxndoc FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNDOC.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);
\copy faers_b.rxncuichanges FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNCUICHANGES.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);
\copy faers_b.rxncui FROM '/data/faers/FAERS_MAK/2.LoadDataToDatabase/RxNorm_full_06052023/rrf/RXNCUI.RRF' WITH (FORMAT CSV, DELIMITER '|', NULL '', HEADER FALSE);
*/