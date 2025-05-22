import os
import psycopg
import json
import logging

# Configure logging for better error tracking
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_remapping_sql_psycopg(config_file="/home/epprechtkai/faers-scripts/config.json"):
    """
    Runs the SQL script s10.sql against a PostgreSQL database using psycopg,
    reading connection parameters from a JSON configuration file.

    Args:
        config_file: Path to the JSON configuration file (default: "/home/epprechtkai/faers-scripts/config.json").

    Returns:
        None. Executes the SQL script and logs progress or errors.
        Raises an exception if there's an error during execution.
    """
    try:
        logging.info(f"Loading configuration from: {config_file}")
        with open(config_file, "r") as f:
            config = json.load(f)
        connection_params = config.get("database")
        if not connection_params:
            raise KeyError("Missing 'database' section in configuration file")
        logging.info(f"Connection parameters loaded successfully")

        with psycopg.connect(**connection_params) as conn:
            conn.autocommit = True  # Enable autocommit for DDL and stored procedure execution
            with conn.cursor() as cur:
                logging.info("Connected to the database.")
                sql_file = "/home/epprechtkai/faers-scripts/s10.sql"
                logging.info(f"Attempting to read SQL script from {sql_file}")
                with open(sql_file, "r") as f:
                    sql_script = f.read()
                logging.info(f"Read SQL script from {sql_file}")

                # Split the script into individual statements, handling semicolons and filtering empty statements
                statements = [s.strip() for s in sql_script.split(";") if s.strip()]

                for i, statement in enumerate(statements):
                    # Skip 'USE' statements as they are not applicable in PostgreSQL
                    if "USE" in statement.upper():
                        logging.info(f"Skipping 'USE' statement {i+1} (not applicable to PostgreSQL).")
                        continue
                    logging.info(f"Executing statement {i+1}: {statement[:100]}...")  # Log first 100 chars
                    try:
                        cur.execute(statement)

                        # Fetch and log results for SELECT queries (e.g., the final SELECT run_all_remapping_steps())
                        if statement.lower().startswith("select"):
                            results = cur.fetchall()
                            for row in results:
                                logging.info(f"  Query result: {row}")

                        logging.info(f"Statement {i+1} executed successfully.")
                    except psycopg.Error as e:
                        logging.error(f"Error executing statement {i+1}: {e}")
                        raise  # Re-raise to stop execution and log the error

                logging.info(f"SQL script {sql_file} executed successfully.")

    except (psycopg.Error, FileNotFoundError, KeyError, json.JSONDecodeError) as e:
        if isinstance(e, FileNotFoundError):
            if 'sql' in str(e).lower():
                logging.error(f"Error: SQL file '{sql_file}' not found.")
            else:
                logging.error(f"Error: Configuration file '{config_file}' not found.")
        elif isinstance(e, json.JSONDecodeError):
            logging.error(f"Error: Invalid JSON format in '{config_file}'.")
        elif isinstance(e, KeyError):
            logging.error(f"Error: Missing 'database' section or connection parameters in '{config_file}'.")
        else:
            logging.error(f"Error executing SQL: {e}")
        raise  # Re-raise to signal failure

if __name__ == "__main__":
    try:
        run_remapping_sql_psycopg()  # Uses default config path
    except Exception as e:
        logging.error(f"An error occurred: {e}")
