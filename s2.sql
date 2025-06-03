-- Ensure this file is saved in UTF-8 encoding without BOM

/****** CONFIGURE DATABASE **********/
SET client_encoding = 'UTF8';

/****** CREATE SCHEMA **********/
CREATE SCHEMA IF NOT EXISTS faers_a;

/****** FUNCTION: Get Completed Year-Quarters **********/
CREATE OR REPLACE FUNCTION faers_a.get_completed_year_quarters(start_year INT DEFAULT 4)
RETURNS TABLE (year INT, quarter INT)
AS $func$
DECLARE
    current_year INT;
    current_quarter INT;
    last_year INT;
    last_quarter INT;
    y INT;
    q INT;
BEGIN
    -- Get current year and quarter
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

    -- Generate year-quarter pairs from start_year
    y := 2000 + start_year; -- Assuming start_year is relative to 2000
    WHILE y <= last_year LOOP
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
$func$ LANGUAGE plpgsql;

/****** PROCEDURE: Process FAERS File **********/
CREATE OR REPLACE PROCEDURE faers_a.process_faers_file(
    p_file_path TEXT,
    p_schema_name VARCHAR,
    p_year INT,
    p_quarter INT,
    p_columns_json JSONB
)
AS $proc$
DECLARE
    v_table_name TEXT;
    v_column_def TEXT;
    v_columns TEXT;
    v_column RECORD;
    v_header_count INT;
    v_expected_count INT;
BEGIN
    -- Construct table name (e.g., faers_a.demo23q1)
    v_table_name := format('faers_a.%s%02dq%d', LOWER(p_schema_name), p_year % 100, p_quarter);

    -- Build column definitions for CREATE TABLE
    v_column_def := '';
    v_columns := '';
    FOR v_column IN
        SELECT key, value
        FROM jsonb_each_text(p_columns_json)
    LOOP
        v_column_def := v_column_def || format('%I %s, ', v_column.key, v_column.value);
        v_columns := v_columns || format('%I, ', v_column.key);
    END LOOP;
    v_column_def := rtrim(v_column_def, ', ');
    v_columns := rtrim(v_columns, ', ');

    -- Create table if it doesn't exist
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %s (
            %s
        )', v_table_name, v_column_def);

    -- Validate file header (check column count)
    v_expected_count := (SELECT count(*) FROM jsonb_object_keys(p_columns_json));
    EXECUTE format('
        CREATE TEMP TABLE temp_header_check (
            header TEXT
        );
        \copy temp_header_check FROM %L WITH (FORMAT csv, DELIMITER ''$'', HEADER false, ENCODING ''UTF8'');
        SELECT array_length(string_to_array((SELECT header FROM temp_header_check LIMIT 1), ''$''), 1)
        INTO v_header_count;
        DROP TABLE temp_header_check;
    ', p_file_path) INTO v_header_count;

    IF v_header_count != v_expected_count THEN
        RAISE EXCEPTION 'Header column count mismatch: expected %, got %', v_expected_count, v_header_count;
    END IF;

    -- Import data using \copy
    EXECUTE format('
        \copy %s (%s) FROM %L WITH (FORMAT csv, DELIMITER ''$'', HEADER true, NULL '''', ENCODING ''UTF8'')
    ', v_table_name, v_columns, p_file_path);

    -- Log success
    RAISE NOTICE 'Imported % into %', p_file_path, v_table_name;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error processing %: %', p_file_path, SQLERRM;
        ROLLBACK;
        RAISE;
END;
$proc$ LANGUAGE plpgsql;