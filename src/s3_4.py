<<<<<<< HEAD
import os
import subprocess
import psycopg
import json
import logging
import time
from constants import CONFIG_DIR, SQL_PATH

#TODO: add logging to LOG_DIR

config_file = CONFIG_DIR / "config.json"

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
bucket_name = config.get("bucket_name")
gcs_directory = config.get("gcs_directory", "ascii/")
root_dir = config.get("root_dir", "/tmp/")

if not all([db_params, bucket_name, gcs_directory, root_dir]):
    logging.error(f"Missing configuration parameters in {config_file}. Please check the file.")
    exit(1)

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

try:
    with psycopg.connect(**db_params) as conn:
        # Check if DEMO_Combined exists
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'faers_combined' 
                    AND table_name = 'DEMO_Combined'
                );
            """)
            table_exists = cur.fetchone()[0]
            if not table_exists:
                logging.error("Table faers_combined.\"DEMO_Combined\" does not exist. Please run s2-5.py first.")
                exit(1)
        
        psql_cmd = ["psql",
                    "-h", conn.info.host,
                    "-p", str(conn.info.port),
                    "-U", conn.info.user,
                    "-d", conn.info.dbname,
                    "-f", str(SQL_PATH / 's3.sql')]
        env = os.environ.copy()
        env["PGPASSWORD"] = conn.info.password
        logging.info("Loading script 3 sql")
        result3 = subprocess.run(psql_cmd, capture_output=True, text=True, env=env)
        print(result3.stdout)
        if result3.returncode != 0:
            logging.error(f"Error executing s3.sql: {result3.stderr}")
            exit(1)

        psql_cmd[-1] = str(SQL_PATH / 's4.sql')
        print(psql_cmd)
        time.sleep(1)
        logging.info("Loading script 4 sql")
        result4 = subprocess.run(psql_cmd, capture_output=True, text=True, env=env)
        if result4.returncode != 0:
            logging.error(f"Error executing s4test.sql: {result4.stderr}")
            if "No such file or directory" in result4.stderr:
                logging.warning("File path issue detected. Please verify the path to reporter_countries.csv at /Users/kaiepprecht/Desktop/untitled folder/faers-scripts/data/reporter_countries.csv.")
            exit(1)
        else:
            print(result4.stdout)
            print(result4.stderr)

except psycopg.Error as e:
    logging.error(f"Database error: {e}")
    exit(1)
except Exception as e:
    logging.error(f"An unexpected error occurred: {e}")
=======
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
    exit(1)
except json.JSONDecodeError as e:
    logging.error(f"Error decoding {config_file}: {e}. Please ensure it's valid JSON.")
    exit(1)

db_params = config.get("database", {})
bucket_name = config.get("bucket_name")
gcs_directory = config.get("gcs_directory", "ascii/")
root_dir = config.get("root_dir", "/tmp/")

if not all([db_params, bucket_name, gcs_directory, root_dir]):
    logging.error(f"Missing configuration parameters in {config_file}. Please check the file.")
    exit(1)

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

try:
    with psycopg.connect(**db_params) as conn:
        # Check if DEMO_Combined exists
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'faers_combined' 
                    AND table_name = 'DEMO_Combined'
                );
            """)
            table_exists = cur.fetchone()[0]
            if not table_exists:
                logging.error("Table faers_combined.\"DEMO_Combined\" does not exist. Please run s2-5.py first.")
                exit(1)
        
        psql_cmd = ["psql",
                    "-h", conn.info.host,
                    "-p", str(conn.info.port),
                    "-U", conn.info.user,
                    "-d", conn.info.dbname,
                    "-f", 's3.sql']
        env = os.environ.copy()
        env["PGPASSWORD"] = conn.info.password
        logging.info("Loading script 3 sql")
        result3 = subprocess.run(psql_cmd, capture_output=True, text=True, env=env)
        print(result3.stdout)
        if result3.returncode != 0:
            logging.error(f"Error executing s3.sql: {result3.stderr}")
            exit(1)

        psql_cmd[-1] = 's4.sql'
        print(psql_cmd)
        time.sleep(1)
        logging.info("Loading script 4 sql")
        result4 = subprocess.run(psql_cmd, capture_output=True, text=True, env=env)
        if result4.returncode != 0:
            logging.error(f"Error executing s4test.sql: {result4.stderr}")
            if "No such file or directory" in result4.stderr:
                logging.warning("File path issue detected. Please verify the path to reporter_countries.csv at /Users/kaiepprecht/Desktop/untitled folder/faers-scripts/data/reporter_countries.csv.")
            exit(1)
        else:
            print(result4.stdout)
            print(result4.stderr)

except psycopg.Error as e:
    logging.error(f"Database error: {e}")
    exit(1)
except Exception as e:
    logging.error(f"An unexpected error occurred: {e}")
>>>>>>> 36-bootstrapping-logging-framework
    exit(1)