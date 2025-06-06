import json
import logging
import os
import psycopg
import re
from google.cloud import storage
import sys
import time

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("s2_execution.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Configuration ---
CONFIG_FILE = "config.json"
SCHEMA_FILE = "schema_config.json"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

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

def main():
    """Main function to orchestrate FAERS data loading."""
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
                local_path = os.path.join(local_dir, os.path.basename(gcs_file_path))

                # Get schema for the period
                try:
                    columns = get_schema_for_period(schema_config, schema_name, year, quarter)
                except ValueError as e:
                    logger.error(f"Schema error for {gcs_file_path}: {e}")
                    continue

                if download_gcs_file(bucket_name, gcs_file_path, local_path):
                    for attempt in range(MAX_RETRIES):
                        try:
                            with conn.cursor() as cur:
                                # Pass columns as JSONB
                                cur.execute(
                                    "CALL faers_a.process_faers_file(%s, %s, %s, %s, %s::jsonb)",
                                    (local_path, schema_name, year, quarter, json.dumps(columns))
                                )
                            conn.commit()
                            logger.info(f"Processed {gcs_file_path} via SQL procedure")
                            break
                        except psycopg.Error as e:
                            conn.rollback()
                            logger.error(f"Attempt {attempt + 1}/{MAX_RETRIES} failed for {gcs_file_path}: {e}")
                            if attempt < MAX_RETRIES - 1:
                                time.sleep(RETRY_DELAY)
                            else:
                                logger.error(f"Failed to process {gcs_file_path} after {MAX_RETRIES} attempts")
                        except Exception as e:
                            conn.rollback()
                            logger.error(f"Unexpected error for {gcs_file_path}: {e}")
                            break
                    os.remove(local_path)
                    logger.info(f"Removed local file {local_path}")

    except psycopg.Error as e:
        logger.error(f"Database error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
