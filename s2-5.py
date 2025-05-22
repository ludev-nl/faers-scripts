import os
import subprocess
import json
import logging

# --- Configuration Loading ---
config_file = "/home/epprechtkai/faers-scripts/config.json"
sql_file = "/home/epprechtkai/faers-scripts/s2-5.sql"

try:
    with open(config_file, "r") as f:
        config = json.load(f)
except FileNotFoundError:
    logging.error(f"Error: {config_file} not found. Please ensure it exists.")
    exit(1)
except json.JSONDecodeError as e:
    logging.error(f"Error decoding {config_file}: {e}. Please ensure it's valid JSON.")
    exit(1)

db_params = config.get("database", {})

if not db_params:
    logging.error(f"Missing database parameters in {config_file}. Please check the file.")
    exit(1)

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Execute SQL File ---
try:
    psql_cmd = [
        "psql",
        "-h", db_params["host"],
        "-p", str(db_params["port"]),
        "-U", db_params["user"],
        "-d", db_params["dbname"],
        "-f", sql_file
    ]
    env = os.environ.copy()
    env["PGPASSWORD"] = db_params["password"]
    result = subprocess.run(psql_cmd, capture_output=True, text=True, env=env)
    if result.returncode == 0:
        logging.info(f"Successfully executed {sql_file}")
        if result.stderr:
            logging.warning(f"psql stderr: {result.stderr}")
    else:
        logging.error(f"Error executing {sql_file}: {result.stderr}")
except Exception as e:
    logging.error(f"Error executing SQL file: {e}")