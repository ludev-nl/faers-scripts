import json
import logging
import os
import psycopg
import re
from google.cloud import storage
import tempfile
import time
import sys

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/s2_execution.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Configuration ---
CONFIG_FILE = "config/config.json"
SCHEMA_FILE = "config/schema_config.json"

def check_psycopg_version():
    """Check psycopg version."""
    import psycopg
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

def get_schema_for_period(schema_config, table_name, year, quarter):
    """Get schema for a table and period."""
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
            return schema_info["columns"]

    raise ValueError(f"No schema available for table {table_name} in period {target_date}")

def create_table_if_not_exists(conn, table_name, schema):
    """Create a table if it does not exist."""
    try:
        with conn.cursor() as cur:
            schema_name = table_name.split('.')[0]
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            columns_def = ", ".join([f"{col_name} {data_type}" for col_name, data_type in schema.items()])
            cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})")
        conn.commit()
        logger.info(f"Table {table_name} created or already exists")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating table {table_name}: {e}")
        raise

def validate_data_file(file_path, schema):
    """Validate data file against schema."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            header = f.readline().strip().split('$')
            expected_columns = len(schema)
            if len(header) != expected_columns:
                logger.error(f"Header in {file_path} has {len(header)} columns, expected {expected_columns}")
                return False
            return True
    except Exception as e:
        logger.error(f"Error validating {file_path}: {e}")
        return False

def import_data_file(conn, file_path, table_name, schema_name, year, quarter, schema_config, max_retries=3):
    """Import data file into a table."""
    for attempt in range(max_retries):
        try:
            schema = get_schema_for_period(schema_config, schema_name, year, quarter)
            create_table_if_not_exists(conn, table_name, schema)

            if not validate_data_file(file_path, schema):
                logger.error(f"Validation failed for {file_path}")
                return

            with conn.cursor() as cur:
                with open(file_path, "rb") as f:
                    copy_sql = f"""
                    COPY {table_name} ({', '.join(schema.keys())})
                    FROM STDIN WITH (
                        FORMAT csv,
                        DELIMITER '$',
                        QUOTE E'\\b',  -- disables quoting
                        ESCAPE E'\\b',  -- disables escaping
                        HEADER true,
                        NULL '',
                        ENCODING 'UTF8'
                    )
                    """
                    with cur.copy(copy_sql) as copy:
                        while True:
                            chunk = f.read(8192)
                            if not chunk:
                                break
                            copy.write(chunk)
            conn.commit()
            logger.info(f"Imported {file_path} into {table_name}")
            return
        except Exception as e:
            conn.rollback()
            logger.error(f"Attempt {attempt + 1}/{max_retries} failed for {file_path}: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
    logger.error(f"Failed to import {file_path} after {max_retries} attempts")

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

def main():
    """Main function to load FAERS data."""
    check_psycopg_version()
    config = load_config()
    schema_config = load_schema_config()
    db_params = config.get("database", {})
    bucket_name = config.get("bucket_name")
    gcs_directory = config.get("gcs_directory", "ascii/")
    local_dir = config.get("root_dir", "/tmp")

    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    try:
        with psycopg.connect(**db_params) as conn:
            files_to_process = list_files_in_gcs_directory(bucket_name, gcs_directory)
            if not files_to_process:
                logger.info(f"No .txt files found in gs://{bucket_name}/{gcs_directory}")
                return

            for gcs_file_path in files_to_process:
                match = re.match(r"([A-Z]+)(\d{2})Q(\d)\.txt", os.path.basename(gcs_file_path), re.IGNORECASE)
                if not match:
                    logger.warning(f"Skipping file with unexpected name format: {gcs_file_path}")
                    continue

                schema_name = match.group(1).upper()
                year = 2000 + int(match.group(2))
                quarter = int(match.group(3))
                table_name = f"faers_a.{schema_name.lower()}{year % 100:02d}q{quarter}"

                local_path = os.path.join(local_dir, os.path.basename(gcs_file_path))
                if download_gcs_file(bucket_name, gcs_file_path, local_path):
                    import_data_file(conn, local_path, table_name, schema_name, year, quarter, schema_config)
                    os.remove(local_path)

    except psycopg.Error as e:
        logger.error(f"Database error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
