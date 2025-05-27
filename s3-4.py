import os
import subprocess
import psycopg
import json
import logging
import time

config_file = "config.json"


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

try:
    with psycopg.connect(**db_params) as conn:
        psql_cmd = ["psql",
                    "-h", conn.info.host,
                    "-p", str(conn.info.port),
                    "-U", conn.info.user,
                    "-d", conn.info.dbname,
                    "-f", '/home/sgfffacetime/faers-scripts/s3.sql']  # Use -f to execute the file

        env = os.environ.copy()
        env["PGPASSWORD"] = conn.info.password
        logging.info("Loading script 3 sql")
        result3 = subprocess.run(psql_cmd, capture_output=True, text=True, env=env)
        print(result3.stdout)

        psql_cmd[-1] = psql_cmd[-1].replace('s3.sql','s4test.sql')
        print(psql_cmd)
        time.sleep(1)
        logging.info("Loading script 4 sql")
        result4 = subprocess.run(psql_cmd, capture_output=True, text=True, env=env)
        print(result4.stdout)
        print(result4.stderr)
except psycopg.Error as e:
    logging.error(f"Database error: {e}")
except Exception as e:
    logging.error(f"An unexpected error occurred: {e}")