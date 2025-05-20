import os
import subprocess
import psycopg
import json
import re
from google.cloud import storage
import logging
import traceback
import tempfile
import time  # Import the time module for retry delays

# --- Configuration Loading ---
#config_file = "config.json"
config_file = "/home/xocas04/faers-scripts/config.json"

schema_file = "schema_config.json"

try:
    with open(config_file, "r") as f:
        config = json.load(f)
except FileNotFoundError:
    logging.error(f"Error: {config_file} not found. Please ensure it exists.")
    exit(1)  # Exit the script if the config file is missing
except json.JSONDecodeError as e:
    logging.error(f"Error decoding {config_file}: {e}. Please ensure it's valid JSON.")
    exit(1)

db_params = config.get("database", {})
bucket_name = config.get("bucket_name")
gcs_directory = config.get("gcs_directory")
root_dir = config.get("root_dir")

if not all([db_params, bucket_name, gcs_directory, root_dir]):
    logging.error(f"Missing configuration parameters in {config_file}. Please check the file.")
    exit(1)

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Helper Functions ---
def check_file_exists(bucket_name, file_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        return blob.exists()
    except Exception as e:
        logging.error(f"Error checking file existence: {e}")
        return False

def copy_file_from_bucket(bucket_name, file_name, local_file_path):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.download_to_filename(local_file_path)
        logging.info(f"File {file_name} copied to {local_file_path}.")
        return True
    except Exception as e:
        logging.error(f"Error copying {file_name}: {e}")
        return False

def get_schema_for_period(schema_config: dict, table_name: str, year: int, quarter: int) -> dict:
    table_schemas = schema_config.get(table_name)
    if not table_schemas:
        raise ValueError(f"No schema found for table {table_name}")

    target_date = f"{year}Q{quarter}"
    best_schema = None

    for schema_info in table_schemas:
        start_date, end_date = schema_info["date_range"]
        # Convert date strings to integers for comparison
        start_year_str = str(start_date[:4])  # Ensure year is treated as a string
        start_year, start_quarter = list(map(int, start_year_str)), int(start_date[5])
        if end_date == "9999Q4":
            end_year, end_quarter = [9999], 4  # Wrap 9999 in a list
        else:
            end_year_str = str(end_date[:4])  # Ensure year is treated as a string
            end_year, end_quarter = list(map(int, end_year_str)), int(end_date[5])
        target_year, target_quarter = year, quarter

        if (start_year[0] < target_year or (start_year[0] == target_year and start_quarter <= target_quarter)) and \
           (end_year[0] > target_year or (end_year[0] == target_year and end_quarter >= target_quarter)):
            best_schema = schema_info["columns"]
            break

    if not best_schema:
        raise ValueError(f"No schema available for table {table_name} in period {target_date}")

    return best_schema

def create_table_if_not_exists(conn: psycopg.Connection, table_name: str, schema: dict):
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {table_name.split('.')[0]}")
            columns_def = ", ".join([f"{col_name} {data_type}" for col_name, data_type in schema.items()])
            cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})")
        conn.commit()
        logging.info(f"Table {table_name} created or already exists.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error creating table {table_name}: {e}")
        raise

def import_data_file(conn: psycopg.Connection, file_path: str, table_name: str, schema_name: str, year: int, quarter: int, schema_config: dict, max_retries=3):
    for attempt in range(max_retries):
        try:
            schema = get_schema_for_period(schema_config, schema_name, year, quarter)
            create_table_if_not_exists(conn, table_name, schema)

            # --- Basic Data Validation ---
            if not validate_data_file(file_path, schema):
                logging.error(f"Data validation failed for {file_path}. Skipping import.")
                return  # Skip the import if validation fails

            columns = list(schema.keys())
            copy_sql = f"""
            \\copy {table_name} ({', '.join(columns)}) FROM '{file_path}' WITH (FORMAT csv, DELIMITER '$', HEADER true, NULL '', ENCODING 'UTF8');
            """

            # Create a temporary file and write the \copy command to it
            with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as temp_sql_file:
                temp_sql_file.write(copy_sql)
                temp_sql_file_path = temp_sql_file.name

            psql_cmd = ["psql",
                        "-h", conn.info.host,
                        "-p", str(conn.info.port),
                        "-U", conn.info.user,
                        "-d", conn.info.dbname,
                        "-f", temp_sql_file_path]  # Use -f to execute the file

            env = os.environ.copy()
            env["PGPASSWORD"] = conn.info.password
            result = subprocess.run(psql_cmd, capture_output=True, text=True, env=env)

            # Clean up the temporary file
            os.remove(temp_sql_file_path)

            if result.returncode == 0:
                logging.info(f"Successfully imported {file_path} into {table_name}")
                # --- Log stderr even on "success" ---
                if result.stderr:
                    logging.warning(f"psql stderr (even though returncode is 0): {result.stderr}")

                # --- Row Count Verification ---
                expected_row_count = get_expected_row_count(file_path)  # Function to get expected count
                actual_row_count = get_actual_row_count(conn, table_name)  # Function to query DB

                if actual_row_count == expected_row_count:
                    logging.info(f"Verified: {table_name} has {actual_row_count} rows (as expected).")
                    return  # Success, exit the function
                else:
                    logging.error(f"Row count mismatch for {table_name}: Expected {expected_row_count}, but found {actual_row_count}. Attempt {attempt + 1}/{max_retries}")
                    if attempt < max_retries - 1:
                        logging.info(f"Retrying import for {table_name} after a short delay...")
                        time.sleep(5)  # Wait for 5 seconds before retrying
            else:
                logging.error(f"Error importing {file_path} into {table_name}: {result.stderr}")
        except Exception as e:
            logging.error(f"Error importing data (attempt {attempt + 1}/{max_retries}): {e}")
            logging.error(traceback.format_exc())
            conn.rollback()
            if attempt < max_retries - 1:
                logging.info(f"Retrying import for {table_name} after an error...")
                time.sleep(5)
    logging.error(f"Failed to import {file_path} into {table_name} after {max_retries} attempts.")

def validate_data_file(file_path, schema):
    """
    Performs basic data validation on the TXT file before import.
    Returns True if validation passes, False otherwise.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            header = f.readline().strip().split('$')  # Assuming $ delimiter
            expected_columns = len(schema)
            if len(header) != expected_columns:
                logging.error(f"Header row in {file_path} has {len(header)} columns, expected {expected_columns}.")
                return False

            for line_num, line in enumerate(f, 2):  # Start line_num at 2 (after header)
                row = line.strip().split('$')
                if len(row) != expected_columns:
                    logging.warning(f"Row {line_num} in {file_path} has {len(row)} columns, expected {expected_columns}. Skipping row.")
                    continue  # Skip the row, but continue validating other rows

                # (Optional) Add more specific data type/format checks here
                # For example, check if date columns have a valid date format
                # or if numeric columns contain only numbers.

            return True  # Validation passed
    except Exception as e:
        logging.error(f"Error during data validation for {file_path}: {e}")
        return False

def get_expected_row_count(file_path):
    """
    Counts the number of data rows (excluding the header) in the TXT file.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            # Assuming the header row exists, subtract 1 from the total line count
            return sum(1 for line in f) - 1
    except Exception as e:
        logging.error(f"Error getting expected row count for {file_path}: {e}")
        return -1

def get_actual_row_count(conn, table_name):
    """
    Queries the database to get the actual row count for the specified table.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            return cur.fetchone()[0]
    except Exception as e:
        logging.error(f"Error getting row count for {table_name}: {e}")
        return -1  # Indicate an error

def list_files_in_gcs_directory(bucket_name, directory_path):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=directory_path)
        files = [blob.name for blob in blobs if blob.name.endswith(".txt")]
        return files
    except Exception as e:
        logging.error(f"Error listing files in GCS: {e}")
        return []

# --- Main Script ---
def main():
    try:
        with open(schema_file, "r") as f:
            schema_config = json.load(f)
    except FileNotFoundError:
        logging.error(f"Error: {schema_file} not found. Please ensure it exists.")
        return
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding {schema_file}: {e}. Please ensure it's valid JSON.")
        return

    try:
        with psycopg.connect(**db_params) as conn:
            files_to_process = list_files_in_gcs_directory(bucket_name, gcs_directory)

            if not files_to_process:
                logging.info(f"No .txt files found in gs://{bucket_name}/{gcs_directory}.  Exiting.")
                return

            for gcs_file_path in files_to_process:
                match = re.match(r"([A-Z]+)(\d{2})Q(\d)\.txt", os.path.basename(gcs_file_path))
                if not match:
                    logging.warning(f"Skipping file with unexpected name format: {gcs_file_path}")
                    continue

                schema_name = match.group(1)
                year = 2000 + int(match.group(2))
                quarter = int(match.group(3))
                table_name = f"faers_a.{schema_name.lower()}{year:02d}q{quarter}"

                local_path = os.path.join(root_dir, os.path.basename(gcs_file_path))

                if not copy_file_from_bucket(bucket_name, gcs_file_path, local_path):
                    logging.warning(f"Failed to copy {gcs_file_path}. Skipping.")
                    continue

                import_data_file(conn, local_path, table_name, schema_name, year, quarter, schema_config)

    except psycopg.Error as e:
        logging.error(f"Database error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
