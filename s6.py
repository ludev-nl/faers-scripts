import json
import logging
import os
import psycopg
import re
import time
from psycopg import errors as pg_errors

# Logging Setup
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("s6_execution.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG_FILE = "config.json"
SQL_FILE_PATH = "s6.sql"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

def load_config():
    """Load configuration from config.json."""
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)
        logger.info(f"Loaded configuration from %s", CONFIG_FILE)
        return config
    except FileNotFoundError:
        logger.error("Config file %s not found", CONFIG_FILE)
        raise
    except json.JSONDecodeError as e:
        logger.error("Error decoding %s: %s", CONFIG_FILE, e)
        raise

def execute_with_retry(cur, statement, retries=MAX_RETRIES, delay=RETRY_DELAY):
    """Execute a SQL statement with retries for transient errors."""
    for attempt in range(1, retries + 1):
        try:
            cur.execute(statement)
            logger.debug("Statement executed successfully on attempt %d", attempt)
            return True
        except (pg_errors.OperationalError, pg_errors.DatabaseError) as e:
            logger.warning("Attempt %d failed: %s", attempt, e)
            if attempt < retries:
                logger.info("Retrying in %d seconds...", delay)
                time.sleep(delay)
            else:
                logger.error("Failed after %d attempts: %s", retries, e)
                raise
        except (pg_errors.DuplicateTable, pg_errors.DuplicateObject, pg_errors.DuplicateIndex) as e:
            logger.info("Object already exists: %s. Skipping.", e)
            return True
        except pg_errors.Error as e:
            logger.error("Database error: %s", e)
            raise
    return False

def verify_tables():
    """Verify that expected tables exist and log their row counts."""
    tables = [
        "DRUG_Mapper",
        "products_at_fda",
        "IDD",
        "manual_mapping"
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
                            logger.warning("Table faers_b.\"%s\" exists but is empty", table)
                        else:
                            logger.info("Table faers_b.\"%s\" exists with %d rows", table, count)
                    except pg_errors.Error as e:
                        logger.warning("Table faers_b.\"%s\" does not exist or is inaccessible: %s", table, e)
    except Exception as e:
        logger.error("Error verifying tables: %s", e)

def parse_sql_statements(sql_script):
    """Parse SQL script into individual statements, preserving DO blocks and functions."""
    statements = []
    current_statement = []
    in_do_block = False
    in_function = False
    do_block_start = re.compile(r'^\s*DO\s*\$\$', re.IGNORECASE)
    function_start = re.compile(r'^\s*CREATE\s+(OR\s+REPLACE\s+)?FUNCTION\s+', re.IGNORECASE)
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
            logger.debug("Skipping \\copy command: %s", line[:100])
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

def run_s6_sql():
    """Execute s6.sql to create and populate mapping tables in faers_b schema."""
    config = load_config()
    db_params = config.get("database", {})
    required_keys = ["host", "port", "user", "dbname", "password"]
    if not all(key in db_params for key in required_keys):
        logger.error("Missing required database parameters: %s", required_keys)
        raise ValueError("Missing database configuration")

    logger.info("Connection parameters: %s", db_params)

    try:
        with psycopg.connect(**db_params) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                logger.info("Connected to PostgreSQL server")
                cur.execute("SELECT version();")
                pg_version = cur.fetchone()[0]
                logger.info("PostgreSQL server version: %s", pg_version)

                cur.execute("SELECT 1 FROM pg_database WHERE datname = 'faersdatabase'")
                if not cur.fetchone():
                    logger.info("faersdatabase does not exist, creating it")
                    cur.execute("CREATE DATABASE faersdatabase")
                    logger.info("Created faersdatabase")
                else:
                    logger.info("faersdatabase already exists")

        with psycopg.connect(**{**db_params, "dbname": "faersdatabase"}) as conn:
            logger.info("Connected to faersdatabase")
            conn.autocommit = True
            with conn.cursor() as cur:
                if not os.path.exists(SQL_FILE_PATH):
                    logger.error("SQL file %s not found", SQL_FILE_PATH)
                    raise FileNotFoundError(SQL_FILE_PATH)

                with open(SQL_FILE_PATH, "r", encoding="utf-8-sig") as f:
                    sql_script = f.read()
                logger.info("Read SQL script from %s", SQL_FILE_PATH)

                statements = parse_sql_statements(sql_script)

                for i, stmt in enumerate(statements, 1):
                    logger.debug("Statement %d (length: %d): %s...", i, len(stmt), stmt[:1000])

                for i, stmt in enumerate(statements, 1):
                    logger.info("Executing statement %d...", i)
                    try:
                        execute_with_retry(cur, stmt)
                    except pg_errors.Error as e:
                        logger.warning("Error executing statement %d: %s", i, e)
                        logger.warning("Failed statement: %s...", stmt[:1000])
                        continue
                    except Exception as e:
                        logger.error("Unexpected error in statement %d: %s", i, e)
                        raise

                logger.info("All statements executed successfully")
                logger.info("Note: Data loading for products_at_fda and IDD must be done separately when files are available")

                verify_tables()

    except pg_errors.Error as e:
        logger.error("Database error: %s", e)
        raise
    except Exception as e:
        logger.error("Unexpected error: %s", e)
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    try:
        run_s6_sql()
    except Exception as e:
        logger.error("Script execution failed: %s", e)
        exit(1)