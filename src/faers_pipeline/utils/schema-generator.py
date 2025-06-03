import json
import logging
import os
import re
import sys
from collections import defaultdict
from google.cloud import storage
from io import StringIO
import pandas as pd

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("schema_generation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Configuration ---
CONFIG_FILE = "config.json"
OUTPUT_SCHEMA_FILE = "auto_schema_config.json"
SAMPLE_ROWS = 100  # Number of rows to sample for type inference
DEFAULT_VARCHAR_LENGTH = 100  # Default length if no data
MAX_VARCHAR_LENGTH = 1000  # Cap for varchar lengths

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

def download_gcs_file_content(bucket_name, file_name):
    """Download file content from GCS as a string."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_text(encoding="utf-8")
        logger.info(f"Downloaded content of {file_name}")
        return content
    except Exception as e:
        logger.error(f"Error downloading {file_name}: {e}")
        return None

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

def infer_sql_type(values):
    """Infer SQL type from a list of values."""
    if not values or all(v is None or v == ""):
        return f"varchar({DEFAULT_VARCHAR_LENGTH})"

    max_length = DEFAULT_VARCHAR_LENGTH
    is_integer = True
    is_float = True
    is_string = False

    for val in values:
        if val is None or val == "":
            continue
        try:
            # Try converting to int
            int_val = int(val)
            if is_integer and str(int_val) != val:
                is_integer = False
        except (ValueError, TypeError):
            is_integer = False

        try:
            # Try converting to float
            float(val)
        except (ValueError, TypeError):
            is_float = False

        # Track max length for strings
        max_length = max(max_length, len(str(val)))
        is_string = True

    if is_integer:
        return "bigint"
    elif is_float:
        return "float(24)"
    elif is_string:
        # Cap varchar length
        length = min(max_length, MAX_VARCHAR_LENGTH)
        # Round up to nearest 10, 100, etc., for cleaner lengths
        if length <= 100:
            length = (length + 9) // 10 * 10
        elif length <= 1000:
            length = (length + 99) // 100 * 100
        return f"varchar({length})"
    else:
        return f"varchar({DEFAULT_VARCHAR_LENGTH})"

def parse_file_content(content):
    """Parse file content and infer schema."""
    try:
        # Read only the header and a sample of rows
        df = pd.read_csv(StringIO(content), delimiter="$", nrows=SAMPLE_ROWS)
        columns = {}
        for col in df.columns:
            # Get non-null values for this column
            values = df[col].dropna().astype(str).tolist()
            sql_type = infer_sql_type(values)
            columns[col.lower()] = sql_type
        return columns
    except Exception as e:
        logger.error(f"Error parsing file content: {e}")
        return None

def determine_date_range(year, quarter, all_periods):
    """Determine the date range for a schema based on all periods."""
    # Convert year-quarter to a sortable key
    current_key = year * 10 + quarter
    sorted_periods = sorted(all_periods, key=lambda x: x[0] * 10 + x[1])

    # Find the start and end of the date range
    start_year, start_quarter = year, quarter
    end_year, end_quarter = year, quarter

    for y, q in sorted_periods:
        key = y * 10 + q
        if key <= current_key:
            start_year, start_quarter = min((start_year, start_quarter), (y, q), key=lambda x: x[0] * 10 + x[1])
        if key >= current_key:
            end_year, end_quarter = max((end_year, end_quarter), (y, q), key=lambda x: x[0] * 10 + x[1])

    start_date = f"{start_year}Q{start_quarter}"
    end_date = f"{end_year}Q{end_quarter}" if end_year != sorted_periods[-1][0] or end_quarter != sorted_periods[-1][1] else "9999Q4"
    return [start_date, end_date]

def main():
    """Main function to generate schema configuration."""
    config = load_config()
    bucket_name = config.get("bucket_name")
    gcs_directory = config.get("gcs_directory", "ascii/")

    # Dictionary to store schemas: {table_name: [(year, quarter, columns), ...]}
    schemas = defaultdict(list)
    periods = set()

    # List all .txt files
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
        periods.add((year, quarter))

        # Download and parse file content
        content = download_gcs_file_content(bucket_name, gcs_file_path)
        if content:
            columns = parse_file_content(content)
            if columns:
                schemas[schema_name].append((year, quarter, columns))
                logger.info(f"Inferred schema for {gcs_file_path}")

    # Organize schemas into the final JSON structure
    schema_config = {}
    for table_name, entries in schemas.items():
        schema_config[table_name] = []
        # Group entries by identical schemas
        schema_groups = defaultdict(list)
        for year, quarter, columns in entries:
            schema_groups[json.dumps(columns, sort_keys=True)].append((year, quarter))

        # Create schema entries with date ranges
        for columns_json, periods in schema_groups.items():
            columns = json.loads(columns_json)
            date_range = determine_date_range(periods[0][0], periods[0][1], periods)
            schema_config[table_name].append({
                "date_range": date_range,
                "columns": columns
            })

        # Sort by start date
        schema_config[table_name].sort(key=lambda x: x["date_range"][0])

    # Write to output file
    try:
        with open(OUTPUT_SCHEMA_FILE, "w", encoding="utf-8") as f:
            json.dump(schema_config, f, indent=2, sort_keys=True)
        logger.info(f"Schema configuration written to {OUTPUT_SCHEMA_FILE}")
    except Exception as e:
        logger.error(f"Error writing schema file: {e}")

if __name__ == "__main__":
    main()
