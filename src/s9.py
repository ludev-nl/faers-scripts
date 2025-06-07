import json
import logging
import os
import psycopg
import re
import time
from psycopg import errors as pg_errors
from constants import SQL_PATH, LOGS_DIR, CONFIG_DIR


# Configuration
CONFIG_FILE = CONFIG_DIR / "config.json"
SQL_FILE_PATH = SQL_PATH / "s9.sql"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Logging Setup
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(str(LOGS_DIR / "s9_execution.log"), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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
        except (pg_errors.DuplicateTable, pg_errors.DuplicateObject, pg_errors.DuplicateIndex) as e:
            logger.info(f"Object already exists: {e}. Skipping.")
            return True
        except pg_errors.Error as e:
            logger.error(f"Database error: {e}")
            raise
    return False

def verify_tables():
    """Verify that expected tables exist and log their row counts, warning if missing."""
    tables = [
        "DRUG_Mapper"
    ]
    try:
        with psycopg.connect(**{**load_config().get("database", {}), "dbname": "faersdatabase"}) as conn:
            with conn.cursor() as cur:
                # Verify schema
                cur.execute("SELECT nspname FROM pg_namespace WHERE nspname = 'faers_b'")
                if not cur.fetchone():
                    logger.warning("Schema faers_b does not exist, skipping table verification")
                    return
                logger.info("Schema faers_b exists")

                for table in tables:
                    try:
                        cur.execute(f"SELECT COUNT(*) FROM faers_b.\"{table}\"")
                        count = cur.fetchone()[0]
                        if count == 0:
                            logger.warning(f"Table faers_b.\"{table}\" exists but is empty")
                        else:
                            logger.info(f"Table faers_b.\"{table}\" exists with {count} rows")
                    except pg_errors.Error as e:
                        logger.warning(f"Table faers_b.\"{table}\" does not exist or is inaccessible: {e}")
    except Exception as e:
        logger.error(f"Error verifying tables: {e}")

def parse_sql_statements(sql_script):
    """Parse SQL script into individual statements, preserving DO blocks and separating UPDATEs."""
    statements = []
    current_statement = []
    in_do_block = False
    in_function = False
    do_block_start = re.compile(r'^\s*DO\s*\$\$', re.IGNORECASE)
    function_start = re.compile(r'^\s*CREATE\s+(OR\s+REPLACE\s+)?FUNCTION\s+', re.IGNORECASE)
    update_start = re.compile(r'^\s*UPDATE\s+', re.IGNORECASE)
    dollar_quote = re.compile(r'\$\$')
    comment_line = re.compile(r'^\s*--.*$', re.MULTILINE)
    comment_inline = re.compile(r'--.*$', re.MULTILINE)
    copy_command = re.compile(r'^\s*\\copy\s+', re.IGNORECASE)

    # Remove BOM and comments
    sql_script = sql_script.lstrip('\ufeff')
    sql_script = re.sub(comment_line, '', sql_script)
    sql_script = re.sub(comment_inline, '', sql_script)

    lines = sql_script.splitlines()
    dollar_count = 0
    for line in lines:
        line = line.strip()
        if not line:
            continue

        if copy_command.match(line):
            logger.debug(f"Skipping \\copy command: {line[:100]}...")
            continue

        if do_block_start.match(line) and not in_function:
            in_do_block = True
            dollar_count = 0
            current_statement.append(line)
        elif function_start.match(line):
            in_function = True
            dollar_count = 0
            current_statement.append(line)
        elif dollar_quote.search(line):
            dollar_count += len(dollar_quote.findall(line))
            current_statement.append(line)
            if (in_do_block or in_function) and dollar_count % 2 == 0:
                if in_do_block:
                    in_do_block = False
                if in_function and line.strip().endswith('LANGUAGE plpgsql;'):
                    in_function = False
                statements.append("\n".join(current_statement))
                current_statement = []
        elif in_do_block or in_function:
            current_statement.append(line)
        else:
            current_statement.append(line)
            if line.endswith(";"):
                statements.append("\n".join(current_statement))
                current_statement = []

    if current_statement:
        statements.append("\n".join(current_statement))

    return [s.strip() for s in statements if s.strip() and not re.match(r'^\s*CREATE\s*DATABASE\s*', s, re.IGNORECASE)]

def run_s9_sql():
    """Execute s9.sql to update DRUG_Mapper with cleaned and mapped data in faers_b schema."""
    config = load_config()
    db_params = config.get("database", {})
    required_keys = ["host", "port", "user", "dbname", "password"]
    if not all(key in db_params for key in required_keys):
        logger.error(f"Missing required database parameters: {required_keys}")
        raise ValueError("Missing database configuration")

    logger.info(f"Connection parameters: {db_params}")

    try:
        with psycopg.connect(**db_params) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                logger.info("Connected to PostgreSQL server")
                cur.execute("SELECT version();")
                pg_version = cur.fetchone()[0]
                logger.info(f"PostgreSQL server version: {pg_version}")

                cur.execute("SELECT 1 FROM pg_database WHERE datname = 'faersdatabase'")
                if not cur.fetchone():
                    logger.info("faersdatabase does not exist, creating it")
                    cur.execute("CREATE DATABASE faersdatabase")
                    logger.info("Created faersdatabase")
                else:
                    logger.info("faersdatabase already exists")

        with psycopg.connect(**{**db_params, "dbname": "faersdatabase"}) as conn:
            logger.info("Connected to faersdatabase")
            conn.autocommit = True  # Use autocommit to ensure each statement is committed
            with conn.cursor() as cur:
                if not os.path.exists(SQL_FILE_PATH):
                    logger.error(f"SQL file {SQL_FILE_PATH} not found")
                    raise FileNotFoundError(SQL_FILE_PATH)

                with open(SQL_FILE_PATH, "r", encoding="utf-8-sig") as f:
                    sql_script = f.read()
                logger.info(f"Read SQL script from {SQL_FILE_PATH}")

                statements = parse_sql_statements(sql_script)

                for i, stmt in enumerate(statements, 1):
                    logger.debug(f"Statement {i} (length: {len(stmt)}): {stmt[:1000]}...")

                for i, stmt in enumerate(statements, 1):
                    logger.info(f"Executing statement {i}...")
                    try:
                        execute_with_retry(cur, stmt)
                    except pg_errors.Error as e:
                        logger.warning(f"Error executing statement {i}: {e}")
                        logger.warning(f"Failed statement: {stmt[:1000]}...")
                        continue
                    except Exception as e:
                        logger.error(f"Unexpected error in statement {i}: {e}")
                        raise

                logger.info("All statements executed successfully")

                verify_tables()

    except pg_errors.Error as e:
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
        run_s9_sql()
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        exit(1)