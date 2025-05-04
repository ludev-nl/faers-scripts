/****************** CREATE FAERS_A DATABASE  ******************************/
-- TODO make this dynamic again with fallback
/* IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = '[FAERS_A]') */
/*   BEGIN */
/*     CREATE DATABASE [FAERS_A] */
/*     END */
/*     GO */

/* 	USE [FAERS_A] */
/*     GO */
/* USE [FAERS_A] */
/* GO */

/****************** MINIMIZE DATABASE LOG SIZE  ******************************/

/* ALTER DATABASE [FAERS_A] SET RECOVERY SIMPLE; */

/*
Function to determine the last completed year-quarter combination from the current date.
*/
CREATE OR REPLACE FUNCTION get_completed_year_quarters(start_year INT DEFAULT 4)
RETURNS TABLE (year INT, quarter INT)
AS
$$
DECLARE
    current_year INT;
    current_quarter INT;
    last_year INT;
    last_quarter INT;
    y INT;
    q INT;
BEGIN
    -- Get the current year and quarter
    SELECT EXTRACT(YEAR FROM CURRENT_DATE)::INT INTO current_year;
    SELECT EXTRACT(QUARTER FROM CURRENT_DATE)::INT INTO current_quarter;
    
    -- Determine last completed quarter
    IF current_quarter = 1 THEN
        last_year := current_year - 1;
        last_quarter := 4;
    ELSE
        last_year := current_year;
        last_quarter := current_quarter - 1;
    END IF;

    -- Generate year-quarter pairs
    y := start_year;
    WHILE y < last_year OR (y = last_year AND q <= last_quarter) LOOP
        q := 1;
        WHILE q <= 4 LOOP
            IF y = last_year AND q > last_quarter THEN
                EXIT;
            END IF;
            year := y;
            quarter := q;
            RETURN NEXT;
            q := q + 1;
        END LOOP;
        y := y + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

/*
Helper function to load from a config-table (schema_config.json)
 the different types of tables to be copied (DEMO, DRUG, REAC, INDI, RPSR, THER)
*/

DROP TABLE IF EXISTS faers_schema_config;
CREATE TABLE faers_schema_config (
    table_name TEXT PRIMARY KEY,
    schema_json JSONB NOT NULL  -- two-level map: { "YYYYQx": { "col1": "type", ... } }
);

-- Function to determine table schema based on year-quarter
CREATE OR REPLACE FUNCTION get_schema_for_period(table_name TEXT, year INT, quarter INT)
RETURNS TEXT AS
$$
DECLARE
    schema_rec RECORD;
    target TEXT := LPAD(year::TEXT, 4, '0') || 'Q' || quarter;
    best_key TEXT := NULL;
    key TEXT;
    def JSONB;
    col_spec TEXT := '';
BEGIN
    SELECT schema_json INTO def
    FROM faers_schema_config
    WHERE table_name = UPPER(table_name);

    IF def IS NULL THEN
        RAISE EXCEPTION 'No schema found for table %', table_name;
    END IF;

    FOR key IN SELECT jsonb_object_keys(def) LOOP
        IF key <= target AND (best_key IS NULL OR key > best_key) THEN
            best_key := key;
        END IF;
    END LOOP;

    IF best_key IS NULL THEN
        RAISE EXCEPTION 'No schema version available for table % and period %', table_name, target;
    END IF;

    FOR schema_rec IN SELECT * FROM jsonb_each_text(def -> best_key) LOOP
        col_spec := col_spec || format('%I %s, ', schema_rec.key, schema_rec.value);
    END LOOP;

    RETURN RTRIM(col_spec, ', ');
END;
$$ LANGUAGE plpgsql;

/******************  CREATING RAW DATA TABLES, AND BULK INSERTION ******************************
>>>JUST REPLACE THE FOLDER DIRECTORY, TO THE DATA LOCATION IN YOUR PC, BEFORE STARTING<<<

WHAT THIS CODE DOES: 
- For all drugs, adversary reactions, indications, demographic and outcomes, create a table and bulk insert the data for each quarter
  from 2004Q1 to 2023Q1
- Combine all quarterly entries for each of the above into a single table, such as DRUG_COMBINED, DEMO_COMBINED, etc.

************************************************************************************************/

/********************************* CREATE TABLES FOR RAW FILES ******************************************/
/********************************* 
For all core tables from 2004Q1 to 2023Q1 , create a table and copy (bulk insert before postgresql) the data from the FAERS_MAK folder
******************************************/

CREATE OR REPLACE FUNCTION load_all_faers_core_tables(root_dir TEXT, start_year INT DEFAULT 4)
RETURNS void AS
$$
DECLARE
    rec RECORD;
    period_upper TEXT;
    period_lower TEXT;
    period_full TEXT;
    core_table TEXT;
    table_prefix TEXT;
    file_prefix TEXT;
    schema_def TEXT;
    sql TEXT;
BEGIN
    -- Core table names to process
    FOR core_table IN SELECT DISTINCT table_name FROM faers_schema_config LOOP
        table_prefix := lower(core_table.table_name);
        file_prefix := upper(core_table.table_name);

        FOR rec IN SELECT * FROM get_completed_year_quarters(start_year) LOOP
            period_upper := LPAD(rec.year::TEXT, 2, '0') || 'Q' || rec.quarter::TEXT;
            period_lower := LPAD(rec.year::TEXT, 2, '0') || 'q' || rec.quarter::TEXT;
            period_full := '20' || LPAD(rec.year::TEXT, 2, '0') || 'Q' || rec.quarter::TEXT;

            -- Load appropriate schema definition
            schema_def := get_schema_for_period(core_table.table_name, rec.year + 2000, rec.quarter); -- convert short year to full

            sql := format($sql$
                DROP TABLE IF EXISTS %I;
                CREATE TABLE %I (%s);
                COPY %I
                FROM %L
                WITH (
                    FORMAT csv,
                    HEADER true,
                    DELIMITER '$',
                    QUOTE E'\b'
                );
            $sql$,
                table_prefix || period_lower,
                table_prefix || period_lower,
                schema_def,
                table_prefix || period_lower,
                root_dir || '/faers_ascii_' || period_full || '/' || file_prefix || period_upper || '.txt'
            );
            EXECUTE sql;
        END LOOP;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


/********************************* COMBINE RAW FILES ******************************************/
/* For each category (DRUG, REAC, INDI, DEMO, OUTCOME, RPSR) take the quarterly data and add it to the combined data under 'PERIOD' */
CREATE OR REPLACE FUNCTION combine_all_faers_tables(start_year INT DEFAULT 4)
RETURNS void AS
$$
DECLARE
    rec RECORD;
    core_table TEXT;
    combined_table TEXT;
    table_prefix TEXT;
    period_lower TEXT;
    union_sql TEXT;
    part_table TEXT;
    col_list TEXT;
    schema_def TEXT;
    first BOOLEAN;
BEGIN
    -- Loop over each core table type
    FOR core_table IN SELECT DISTINCT table_name FROM faers_schema_config LOOP
        table_prefix := lower(core_table.table_name);
        combined_table := table_prefix || '_combined';
        union_sql := '';
        first := TRUE;

        -- Loop over each completed year-quarter
        FOR rec IN SELECT * FROM get_completed_year_quarters(start_year) LOOP
            period_lower := LPAD(rec.year::TEXT, 2, '0') || 'q' || rec.quarter::TEXT;
            part_table := table_prefix || period_lower;

            BEGIN
                -- Get the schema for the period to generate SELECT list
                schema_def := get_schema_for_period(core_table.table_name, rec.year + 2000, rec.quarter);
                col_list := '';
                FOR schema_col IN SELECT * FROM regexp_matches(schema_def, '([^,]+)', 'g') LOOP
                    col_list := col_list || split_part(schema_col[1], ' ', 1) || ', ';
                END LOOP;
                col_list := RTRIM(col_list, ', ');

                IF first THEN
                    EXECUTE format('DROP TABLE IF EXISTS %I; CREATE TABLE %I AS SELECT %s FROM %I;',
                                   combined_table, combined_table, col_list, part_table);
                    first := FALSE;
                ELSE
                    EXECUTE format('INSERT INTO %I (%s) SELECT %s FROM %I;',
                                   combined_table, col_list, col_list, part_table);
                END IF;

            EXCEPTION WHEN OTHERS THEN
                -- If table doesn't exist, skip (e.g., file missing for that quarter)
                RAISE NOTICE 'Skipping %', part_table;
            END;
        END LOOP;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- TODO: implement trimming and clean-up steps