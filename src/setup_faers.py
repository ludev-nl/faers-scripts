import json
import logging
import os
import psycopg
import re
from google.cloud import storage
import tempfile
import time
import sys
import chardet
from constants import CONFIG_DIR, LOGS_DIR
from error import get_logger, fatal_error

# --- Configuration ---
CONFIG_FILE = CONFIG_DIR / "config.json"
SCHEMA_FILE = CONFIG_DIR / "schema_config.json"
CONFIG_FILE = "config.json"
SCHEMA_FILE = "schema_config.json"
SQL_FILE = "setup_faers.sql"
SKIPPED_FILES_LOG = "skipped_files.log"

logger = get_logger()

def check_psycopg_version():
    """Check psycopg version."""
    version = psycopg.__version__
    logger.info(f"Using psycopg version: {version}")
    if not version.startswith("3."):
        logger.error("This script requires psycopg 3. Found version %s", version)
        sys.exit(1)

def load_config():
    """Load configuration from config.json."""
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)
        logger.info(f"Loaded configuration from {CONFIG_FILE}")
        return config
    except FileNotFoundError:
        logger.error(f"Config file {CONFIG_FILE} not found")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding {CONFIG_FILE}: {e}")
        raise

def load_schema_config():
    """Load schema configuration from schema_config.json."""
    try:
        with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
            schema_config = json.load(f)
        logger.info(f"Loaded schema configuration from {SCHEMA_FILE}")
        return schema_config
    except FileNotFoundError:
        logger.error(f"Schema file {SCHEMA_FILE} not found")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding {SCHEMA_FILE}: {e}")
        raise

def execute_sql_file(conn, sql_file):
    """Execute an SQL file."""
    try:
        with open(sql_file, "r", encoding="utf-8") as f:
            sql = f.read()
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        logger.info(f"Executed SQL file {sql_file}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error executing {sql_file}: {e}")
        raise

def check_file_exists(bucket_name, file_name):
    """Check if a file exists in GCS."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        exists = blob.exists()
        logger.info(f"File {file_name} exists: {exists}")
        return exists
    except Exception as e:
        logger.error(f"Error checking file existence: {e}")
        return False

def download_gcs_file(bucket_name, file_name, local_path):
    """Download a file from GCS."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.download_to_filename(local_path)
        logger.info(f"Downloaded {file_name} to {local_path}")
        return True
    except Exception as e:
        logger.error(f"Error downloading {file_name}: {e}")
        return False

def detect_encoding(file_path):
    """Detect file encoding."""
    with open(file_path, "rb") as f:
        raw_data = f.read(100000)  # Read first 100KB
        result = chardet.detect(raw_data)
        encoding = result["encoding"]
        confidence = result["confidence"]
        logger.info(f"Detected encoding for {file_path}: {encoding} (confidence: {confidence})")
        return encoding

def preprocess_file(input_path, output_path):
    """Preprocess file to ensure UTF-8 compatibility."""
    try:
        with open(input_path, "rb") as f:
            raw_data = f.read()
        # Decode with replacement for invalid characters
        content = raw_data.decode("utf-8", errors="replace")
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"Preprocessed {input_path} to {output_path} as UTF-8")
        return True
    except Exception as e:
        logger.error(f"Error preprocessing {input_path}: {e}")
        return False

def get_schema_for_period(schema_config, table_name, year, quarter):
    table_schemas = schema_config.get(table_name.upper())
    if not table_schemas:
        raise ValueError(f"No schema found for table {table_name}")

    target_date = f"{year}Q{quarter}"
    for schema_info in table_schemas:
        start_date, end_date = schema_info["date_range"]
        start_year, start_quarter = int(start_date[:4]), int(start_date[5])
        end_year = 9999 if end_date == "9999Q4" else int(end_date[:4])
        end_quarter = 4 if end_date == "9999Q4" else int(end_date[5])

        if (start_year <= year <= end_year) and \
           (start_year < year or start_quarter <= quarter) and \
           (year < end_year or quarter <= end_quarter):
            logger.info(f"Schema for {table_name} {target_date}: {schema_info['columns'].keys()}")
            return schema_info["columns"]

    raise ValueError(f"No schema available for table {table_name} in period {target_date}")

def create_table_if_not_exists(conn, table_name, schema):
    try:
        with conn.cursor() as cur:
            schema_name = table_name.split('.')[0]
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            # Check if table exists and get its current structure
            cur.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}' AND table_name = '{table_name.split('.')[1]}'
                ORDER BY ordinal_position
            """)
            existing_cols = {row[0]: row[1] for row in cur.fetchall()}

            # Compare with new schema
            if existing_cols:
                new_cols = set(schema.items())
                current_cols = set(existing_cols.items())
                if new_cols != current_cols:
                    logger.warning(f"Schema mismatch for {table_name}. Dropping and recreating.")
                    cur.execute(f"DROP TABLE {table_name}")

            columns_def = ", ".join([f"{col_name} {data_type}" for col_name, data_type in schema.items()])
            cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})")
        conn.commit()
        logger.info(f"Table {table_name} created or already exists")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating table {table_name}: {e}")
        raise

def validate_data_file(file_path, schema):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            header = f.readline().strip().split('$')
            expected_columns = len(schema)
            if len(header) != expected_columns:
                logger.error(f"Header in {file_path} has {len(header)} columns, expected {expected_columns}. Found: {header}, Expected: {list(schema.keys())}")
                return False
            return True
    except Exception as e:
        logger.error(f"Error validating {file_path}: {e}")
        return False

def import_data_file(conn, file_path, table_name, schema_name, year, quarter, schema_config, max_retries=3):
    for attempt in range(max_retries):
        try:
            schema = get_schema_for_period(schema_config, schema_name, year, quarter)
            create_table_if_not_exists(conn, table_name, schema)
            temp_file = f"{file_path}.utf8"
            if not preprocess_file(file_path, temp_file):
                logger.error(f"Skipping {file_path} due to preprocessing failure")
                with open(SKIPPED_FILES_LOG, "a", encoding="utf-8") as f:
                    f.write(f"{file_path}: Preprocessing failed\n")
                return
            if not validate_data_file(temp_file, schema):
                logger.error(f"Validation failed for {temp_file}")
                with open(SKIPPED_FILES_LOG, "a", encoding="utf-8") as f:
                    f.write(f"{temp_file}: Validation failed\n")
                os.remove(temp_file)
                return
            with conn.cursor() as cur:
                with open(temp_file, "rb") as f:
                    copy_sql = f"""
                    COPY {table_name} ({', '.join(schema.keys())})
                    FROM STDIN WITH (FORMAT csv, DELIMITER '$', HEADER true, NULL '', ENCODING 'UTF8')
                    """
                    with cur.copy(copy_sql) as copy:
                        while True:
                            chunk = f.read(8192)
                            if not chunk:
                                break
                            copy.write(chunk)
            conn.commit()
            logger.info(f"Imported {temp_file} into {table_name}")
            os.remove(temp_file)
            return
        except Exception as e:
            conn.rollback()
            logger.error(f"Attempt {attempt + 1}/{max_retries} failed for {file_path}: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
    logger.error(f"Failed to import {file_path} after {max_retries} attempts")
    with open(SKIPPED_FILES_LOG, "a", encoding="utf-8") as f:
        f.write(f"{file_path}: Failed after {max_retries} attempts: {str(e)}\n")

def list_files_in_gcs_directory(bucket_name, directory_path):
    """List .txt files in GCS directory."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=directory_path)
        return [blob.name for blob in blobs if blob.name.lower().endswith(".txt")]
    except Exception as e:
        logger.error(f"Error listing GCS files: {e}")
        return []

def execute_sql_file(conn, sql_file):
    logger.info(f"Executing SQL file from absolute path: {os.path.abspath(sql_file)}")
    try:
        with open(sql_file, "r", encoding="utf-8") as f:
            sql = f.read()
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        logger.info(f"Executed SQL file {sql_file}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error executing {sql_file}: {e}")
        raise

def main():
    """Main function to set up and load FAERS data."""
    check_psycopg_version()
    config = load_config()
    schema_config = load_schema_config()
    db_params = config.get("database", {})
    bucket_name = config.get("bucket_name")
    gcs_directory = config.get("gcs_directory", "ascii/")
    local_dir = config.get("root_dir", "/tmp")

    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    # Initialize skipped files log
    with open(SKIPPED_FILES_LOG, "w", encoding="utf-8") as f:
        f.write("Skipped files during setup_faers.py execution:\n")

    try:
        with psycopg.connect(**db_params) as conn:
            # Execute setup SQL
            execute_sql_file(conn, SQL_FILE)

            # Get valid year-quarters
            with conn.cursor() as cur:
                cur.execute("SELECT year, quarter FROM get_completed_year_quarters(4)")
                valid_quarters = [(row[0], row[1]) for row in cur.fetchall()]
                logger.info(f"Valid year-quarters: {valid_quarters}")

            # List GCS files
            files_to_process = list_files_in_gcs_directory(bucket_name, gcs_directory)
            if not files_to_process:
                logger.info(f"No .txt files found in gs://{bucket_name}/{gcs_directory}")
                return

            for gcs_file_path in sorted(files_to_process):
                match = re.match(r"([A-Z]+)(\d{2})Q(\d)\.txt", os.path.basename(gcs_file_path), re.IGNORECASE)
                if not match:
                    logger.warning(f"Skipping file with unexpected name format: {gcs_file_path}")
                    continue

                schema_name = match.group(1).upper()
                year = 2000 + int(match.group(2))
                quarter = int(match.group(3))

                # Check if year-quarter is valid
                if (year, quarter) not in valid_quarters:
                    logger.warning(f"Skipping invalid year-quarter: {year}Q{quarter}")
                    continue

                table_name = f"faers_a.{schema_name.lower()}{year % 100:02d}q{quarter}"
                local_path = os.path.join(local_dir, os.path.basename(gcs_file_path))
                if download_gcs_file(bucket_name, gcs_file_path, local_path):
                    import_data_file(conn, local_path, table_name, schema_name, year, quarter, schema_config)
                    if os.path.exists(local_path):
                        os.remove(local_path)

            # Verify loaded tables
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'faers_a'
                    ORDER BY table_name
                """)
                for row in cur.fetchall():
                    table_name = row[0]
                    cur.execute(f"SELECT COUNT(*) FROM faers_a.\"{table_name}\"")
                    row_count = cur.fetchone()[0]
                    logger.info(f"Table faers_a.{table_name} has {row_count} rows")

    except psycopg.Error as e:
        logger.error(f"Database error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
