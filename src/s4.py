import psycopg
import json
import logging
from constants import CONFIG_DIR, SQL_PATH


# Configure logging for better error tracking
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_s4_sql_psycopg(config_file=str(CONFIG_DIR / "config.json")):
    """
    Runs the SQL script s4.sql against a database using psycopg,
    reading connection parameters from a JSON configuration file.

    Args:
        config_file: Path to the JSON configuration file (default: "config.json").

    Returns:
        None.  The function executes the SQL script.
        Raises an exception if there's an error during execution.
    """
    try:
        logging.info(f"Loading configuration from: {config_file}")
        with open(config_file, "r") as f:
            config = json.load(f)
        connection_params = config["database"]
        logging.info(f"Connection parameters: {connection_params}")

        with psycopg.connect(**connection_params) as conn:
            conn.autocommit = True  # Enable autocommit for DDL changes
            with conn.cursor() as cur:
                logging.info("Connected to the database.")
                with open(str(SQL_PATH / "s4.sql", "r")) as f:
                    sql_script = f.read()
                logging.info("Read SQL script from s4.sql")

                # Split the script into individual statements, handling semicolons and filtering empty statements
                statements = [s.strip() for s in sql_script.split(";") if s.strip()]

                for i, statement in enumerate(statements):
                    #  Replace "USE FAERS_A" with a no-op or equivalent for PostgreSQL
                    if "USE FAERS_A" in statement:
                        logging.info("Skipping 'USE FAERS_A' statement (not applicable to PostgreSQL).")
                        continue
                    logging.info(f"Executing statement {i+1}: {statement[:100]}...")  # Log first 100 chars
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

                logging.info("SQL script s4.sql executed successfully.")

    except (psycopg.Error, FileNotFoundError, KeyError, json.JSONDecodeError) as e:
        if isinstance(e, FileNotFoundError):
            logging.error(f"Error: Configuration file '{config_file}' not found.")
        elif isinstance(e, json.JSONDecodeError):
            logging.error(f"Error: Invalid JSON format in '{config_file}'.")
        elif isinstance(e, KeyError):
            logging.error(f"Error: Missing 'database' section or connection parameters in '{config_file}'.")
        else:
            logging.error(f"Error executing SQL: {e}")
        raise  # Re-raise the exception to signal failure

# Example usage (uncomment to run directly):
# try:
#     run_s4_sql_psycopg()  # Uses config.json by default
# except Exception as e:
#     logging.error(f"An error occurred: {e}")

run_s4_sql_psycopg()
