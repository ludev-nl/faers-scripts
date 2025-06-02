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
        logging.FileHandler("s11_execution.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG_FILE = "config.json"
SQL_FILE_PATH = "s11.sql"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

def load_config():
    """Load configuration from config.json."""
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)
        logger.info("Loaded configuration from %s", CONFIG_FILE)
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
        "drugs_standardized",
        "adverse_reactions",
        "drug_adverse_reactions_pairs",
        "drug_adverse_reactions_count",
        "drug_indications",
        "demographics",
        "case_outcomes",
        "therapy_dates",
        "report_sources",
        "drug_margin",
        "event_margin",
        "total_count",
        "contingency_table",
        "proportionate_analysis"
    ]
    try:
        with psycopg.connect(**{**load_config().get("database", {}), "dbname": "faersdatabase"}) as conn:
            conn.autocommit = True
            for table in tables:
                try:
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT COUNT(*) FROM faers_b.\"{table}\"")
                        count = cur.fetchone()[0]
                        if count == 0:
                            logger.warning("Table faers_b.\"%s\" exists but is empty", table)
                        else:
                            logger.info("Table faers_b.\"%s\" exists with %d rows", table, count)
                except pg_errors.Error as e:
                    logger.warning("Table faers_b.\"%s\" does not exist or is inaccessible: %s", table, e)
                    conn.rollback()  # Reset transaction state
    except Exception as e:
        logger.error("Error verifying tables: %s", e)

def parse_sql_statements(sql_script):
    """Parse SQL script into individual statements, preserving DO blocks and functions."""
    statements = []
    current_statement = []
    in_dollar_quoted = False
    dollar_quote = re.compile(r'\$\$')
    comment_line = re.compile(r'^\s*--.*$', re.MULTILINE)
    comment_inline = re.compile(r'--.*$', re.MULTILINE)
    copy_command = re.compile(r'^\s*\\copy\s+', re.IGNORECASE)

    # Clean script
    sql_script = sql_script.lstrip('\ufeff')
    sql_script = re.sub(comment_line, '', sql_script)
    sql_script = re.sub(comment_inline, '', sql_script)

    lines = sql_script.splitlines()
    for line in lines:
        line = line.strip()
        if not line:
            continue

        if copy_command.match(line):
            logger.debug("Skipping \\copy command: %s", line[:100])
            continue

        current_statement.append(line)

        # Track dollar-quoted blocks
        if dollar_quote.search(line):
            if not in_dollar_quoted:
                in_dollar_quoted = True
            else:
                in_dollar_quoted = False

        # End of a statement
        if not in_dollar_quoted and line.endswith(';'):
            statements.append("\n".join(current_statement))
            current_statement = []

    # Handle any remaining statement
    if current_statement:
        statements.append("\n".join(current_statement))
        logger.warning("Incomplete statement detected: %s", "\n".join(current_statement)[:100])

    return [s.strip() for s in statements if s.strip() and not re.match(r'^\s*CREATE\s*DATABASE\s*', s, re.IGNORECASE)]

def run_s11_sql():
    """Execute s11.sql to create dataset tables for FAERS analysis."""
    config = load_config()
    db_params = config.get("database", {})
    required_keys = ["host", "port", "user", "dbname", "password"]
    if not all(key in db_params for key in required_keys):
        logger.error("Missing required database parameters: %s", required_keys)
        raise ValueError("Missing database configuration")

    logger.info("Connection parameters: %s", {k: v for k, v in db_params.items() if k != 'password'})

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
                logger.info("Dataset tables created. Check faers_b.remapping_log for details.")

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
        run_s11_sql()
    except Exception as e:
        logger.error("Script execution failed: %s", e)
        exit(1)