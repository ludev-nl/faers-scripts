import json
import logging
import os
import psycopg
import re
import time
from psycopg import errors as pg_errors
<<<<<<< HEAD
from constants import SQL_PATH, LOGS_DIR, CONFIG_DIR

# --- Configuration ---
CONFIG_FILE = CONFIG_DIR / "config.json"
SQL_FILE_PATH = SQL_PATH / "s10.sql"
MAX_RETRIES = 1
RETRY_DELAY = 1  # seconds

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(str(LOGS_DIR / "s10_execution.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

=======

from error import get_logger, fatal_error

logger = get_logger()

# --- Configuration ---
CONFIG_FILE = "config.json"
SQL_FILE_PATH = "s10.sql"
MAX_RETRIES = 1
RETRY_DELAY = 1  # seconds

>>>>>>> 36-bootstrapping-logging-framework
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

def check_postgresql_version(cur):
    """Check PostgreSQL server version."""
    try:
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        logger.info(f"PostgreSQL server version: {version}")
        return version
    except pg_errors.Error as e:
        logger.error(f"Error checking PostgreSQL version: {e}")
        raise

def check_database_exists(cur, dbname):
    """Check if database exists."""
    try:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
        exists = cur.fetchone()
        if exists:
            logger.info(f"{dbname} already exists")
            return True
        logger.info(f"{dbname} does not exist")
        return False
    except pg_errors.Error as e:
        logger.error(f"Error checking database existence: {e}")
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
        except pg_errors.Error as e:
            logger.error(f"Non-retryable database error: {e}")
            raise
    return False

def verify_tables(cur, schema, tables):
    """Verify that all expected tables exist and log their row counts."""
    for table in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {schema}.\"{table}\"")
            count = cur.fetchone()[0]
            if count == 0:
                logger.warning(f"Table {schema}.\"{table}\" is empty")
            else:
                logger.info(f"Table {schema}.\"{table}\" exists with {count} rows")
        except pg_errors.Error as e:
            logger.warning(f"Table {schema}.\"{table}\" does not exist or is inaccessible: {e}")

def parse_sql_statements(sql_script):
    """Parse SQL script into valid statements, preserving DO blocks."""
    statements = []
    current_statement = []
    in_do_block = False
    do_block_start = re.compile(r'^\s*DO\s*\$\$', re.IGNORECASE)
    do_block_end = re.compile(r'^\s*\$\s*\$;?$', re.IGNORECASE)

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

def run_s10_sql():
    """Execute s10.sql to create and populate tables in faers_b schema."""
    config = load_config()
    db_params = config.get("database", {})
    required_keys = ["host", "port", "user", "dbname", "password"]
    if not all(key in db_params for key in required_keys):
        logger.error(f"Missing required database parameters: {required_keys}")
        raise ValueError("Missing database configuration")

    logger.info(f"Connection parameters: {db_params}")

    tables = [
        "drug_mapper", "drug_mapper_2", "drug_mapper_3", "manual_remapper", "remapping_log"
    ]

    try:
        # Connect to PostgreSQL server (default database)
        with psycopg.connect(**{**db_params, "dbname": "postgres"}) as server_conn:
            server_conn.autocommit = True
            with server_conn.cursor() as server_cur:
                check_postgresql_version(server_cur)
                if not check_database_exists(server_cur, db_params["dbname"]):
                    logger.error(f"Database {db_params['dbname']} does not exist")
                    raise ValueError(f"Database {db_params['dbname']} does not exist")

        # Connect to faersdatabase
        with psycopg.connect(**db_params) as conn:
            logger.info(f"Connected to {db_params['dbname']}")
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
                    logger.info(f"Executing statement {i}...")
                    try:
                        with conn.transaction():
                            execute_with_retry(cur, stmt)
                        conn.commit()  # Commit each statement
                    except pg_errors.Error as e:
                        logger.warning(f"Error executing statement {i}: {e}")
                        logger.warning(f"Failed statement: {stmt[:1000]}...")
                        conn.rollback()  # Rollback only the failed statement
                        continue
                    except Exception as e:
                        logger.error(f"Unexpected error in statement {i}: {e}")
                        conn.rollback()
                        raise

                logger.info("All statements executed successfully")
                logger.info("Note: Manual remapping via external tool (e.g., MS Access) may be required for manual_remapper")

                # Verify schema and tables
                cur.execute("SELECT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'faers_b')")
                if not cur.fetchone()[0]:
                    logger.error("Schema faers_b does not exist")
                    raise ValueError("Schema faers_b does not exist")
                logger.info("Schema faers_b exists")

                verify_tables(cur, "faers_b", tables)

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
        run_s10_sql()
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
<<<<<<< HEAD
        exit(1)
=======
        exit(1)
>>>>>>> 36-bootstrapping-logging-framework
