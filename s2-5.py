import json
import logging
import os
import psycopg
import re
import time
from psycopg import errors as pg_errors

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/home/xocas04/faers-scripts/s2-5_execution.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Configuration ---
CONFIG_FILE = "/home/xocas04/faers-scripts/config.json"
SQL_FILE_PATH = "/home/xocas04/faers-scripts/s2-5.sql"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

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

def execute_with_retry(cur, statement, retries=MAX_RETRIES, delay=RETRY_DELAY):
    """Execute a SQL statement with retries for transient errors."""
    for attempt in range(1, retries + 1):
        try:
            cur.execute(statement)
            logger.debug(f"Statement executed successfully on attempt {attempt}")
            return True
        except (pg_errors.OperationalError, pg_errors.DatabaseError) as e:
            logger.warning(f"Attempt {attempt} failed: {e}")
            if attempt < retries:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"Failed after {retries} attempts: {e}")
                raise
        except (pg_errors.DuplicateTable, pg_errors.DuplicateObject) as e:
            logger.info(f"Object already exists: {e}. Skipping.")
            return True
        except pg_errors.SyntaxError as e:
            logger.warning(f"Syntax error, possibly non-executable statement: {e}")
            return True
        except pg_errors.Error as e:
            logger.error(f"Non-retryable database error: {e}")
            raise
    return False

def verify_tables(cur, tables):
    """Verify that all expected tables exist and log their row counts."""
    for table in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM faers_combined.\"{table}\"")
            count = cur.fetchone()[0]
            logger.info(f"Table faers_combined.\"{table}\" exists with {count} rows")
        except pg_errors.Error as e:
            logger.error(f"Table faers_combined.\"{table}\" does not exist or is inaccessible: {e}")

def parse_sql_statements(sql_script):
    """Parse SQL script into valid statements, preserving DO blocks."""
    statements = []
    current_statement = []
    in_do_block = False
    do_block_start = re.compile(r'^\s*DO\s*\$\$', re.IGNORECASE)
    do_block_end = re.compile(r'^\s*\$\$;?$', re.IGNORECASE)

    lines = sql_script.splitlines()
    for line in lines:
        line = line.strip()
        if not line or re.match(r'^\s*--', line):
            continue

        if do_block_start.match(line):
            in_do_block = True
            current_statement.append(line)
        elif do_block_end.match(line) and in_do_block:
            current_statement.append(line)
            statements.append("\n".join(current_statement))
            current_statement = []
            in_do_block = False
        elif in_do_block:
            current_statement.append(line)
        else:
            if line.endswith(";"):
                current_statement.append(line[:-1])
                statements.append("\n".join(current_statement))
                current_statement = []
            else:
                current_statement.append(line)

    if current_statement:
        statements.append("\n".join(current_statement))

    return [s.strip() for s in statements if s.strip()]

def run_s2_5_sql():
    """Execute s2-5.sql to create combined tables in faers_combined schema."""
    config = load_config()
    db_params = config.get("database", {})
    required_keys = ["host", "port", "user", "dbname", "password"]
    if not all(key in db_params for key in required_keys):
        logger.error(f"Missing required database parameters: {required_keys}")
        raise ValueError("Missing database configuration")

    logger.info(f"Connection parameters: {db_params}")

    tables = [
        "DEMO_Combined", "DRUG_Combined", "INDI_Combined", "THER_Combined",
        "REAC_Combined", "RPSR_Combined", "OUTC_Combined", "COMBINED_DELETED_CASES"
    ]

    try:
        with psycopg.connect(**db_params) as conn:
            logger.info("Connected to database")
            conn.autocommit = False
            with conn.cursor() as cur:
                if not os.path.exists(SQL_FILE_PATH):
                    logger.error(f"SQL file {SQL_FILE_PATH} not found")
                    raise FileNotFoundError(f"SQL file {SQL_FILE_PATH} not found")

                with open(SQL_FILE_PATH, "r", encoding="utf-8") as f:
                    sql_script = f.read()
                logger.info(f"Read SQL script from {SQL_FILE_PATH}")

                statements = parse_sql_statements(sql_script)

                for i, stmt in enumerate(statements, 1):
                    logger.debug(f"Statement {i} (length: {len(stmt)}): {stmt[:1000]}...")

                for i, stmt in enumerate(statements, 1):
                    logger.info(f"Executing statement {i}: {stmt[:100]}...")
                    try:
                        with conn.transaction():
                            execute_with_retry(cur, stmt)
                    except pg_errors.Error as e:
                        logger.error(f"Error executing statement {i}: {e}")
                        logger.error(f"Failed statement: {stmt[:1000]}...")
                        conn.rollback()
                        continue
                    except Exception as e:
                        logger.error(f"Unexpected error in statement {i}: {e}")
                        conn.rollback()
                        raise

                conn.commit()
                logger.info("All statements executed successfully")

                verify_tables(cur, tables)

    except psycopg.Error as e:
        logger.error(f"Database error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    try:
        run_s2_5_sql()
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        exit(1)
