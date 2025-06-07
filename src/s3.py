import psycopg
import json
import logging

CONFIG_FILE = "config.json"


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Loads config file from path defined by variable 'CONFIG_FILE'
def load_config() -> dict:
    try:
        logging.info(f'Loading configuration from: {CONFIG_FILE}')
        with open(CONFIG_FILE, "r") as f:
            config = json.load(f)
    except json.JSONDecodeError:
        logging.error(f"Error: Failed to decode JSON file: '{CONFIG_FILE}'")
        raise
    except FileNotFoundError:
        logging.error(f"Error: configuration file '{CONFIG_FILE}' not found")
        raise
    return config


# Runs s3.sql against a database defined in config.json with the help op psycopg
# Args:
#       config (dict): this is the config file loaded in 'load_config' containing
#                      information about the database
def run_s3_sql (config:dict):

    try:
        connection_params = config['database']
        params_list = []
        for param_key in connection_params:
            params_list.append(f'{param_key}: {connection_params[param_key]}')
        param_string = "\n\t".join(params_list)
        logging.info(f'Connection parameters: \n\t{param_string}')

        with psycopg.connect(**connection_params) as conn:
            conn.autocommit = True  # Enable autocommit for DDL changes
            with conn.cursor() as cur:
                logging.info("Connected to the database.")
                with open("s3.sql", "r") as f:
                    sql_script = f.read()
                logging.info("Read SQL script from s3.sql")

                # Split the script into individual statements, handling semicolons and filtering empty statements
                statements = [s.strip() for s in sql_script.split(";") if s.strip()]
                for i, statement in enumerate(statements):
                    #  Replace "USE FAERS_A" with a no-op or equivalent for PostgreSQL
                    if "USE FAERS_A" in statement:
                        logging.info("Skipping 'USE FAERS_A' statement (not applicable to PostgreSQL).")
                        continue

                    # Log first 100 chars
                    if len(statement) > 100:
                        logging.info(f"Executing statement {i+1}: {statement[:100]}...")
                    else:
                        logging.info(f"Executing statement {i+1}: {statement[:100]}")

                    try:
                        cur.execute(statement)
                        # Fetch and print results if it's a SELECT query
                        if statement.lower().startswith("select"):
                            results = cur.fetchall()
                            for row in results:
                                logging.info(f"  Query result: {row}")

                        logging.info(f"Statement {i+1} executed successfully.")
                    except psycopg.Error as e:
                        logging.error(f"Error executing statement {i+1}: {e}")
                        raise  # Re-raise the exception to stop execution

                logging.info("SQL script s3.sql executed successfully.")
    except (psycopg.Error, KeyError, FileNotFoundError) as e:
        if isinstance(e, FileNotFoundError):
            logging.error(f"Error: SQL file 's3.sql' not found.")
        elif isinstance(e, KeyError):
            logging.error(f"Error: Missing 'database' section or connection parameters in '{CONFIG_FILE}'.")
        else:
            logging.error(f"Error executing SQL: {e}")
        raise  # Re-raise the exception to signal failure


loaded_config = load_config()
run_s3_sql(loaded_config)
