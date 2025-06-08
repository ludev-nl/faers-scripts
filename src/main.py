import psycopg
import json

def execute_debugging_queries(config_file="config.json"):
    """
    Connects to a PostgreSQL database, executes debugging queries
    to check the state of tables and data after running s4.sql, and
    prints the results.

    Args:
        config_file: Path to the JSON configuration file.

    Returns:
        None. Prints the results of the debugging queries.
    """
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
        connection_params = config["database"]

        with psycopg.connect(**connection_params) as conn:
            with conn.cursor() as cur:
                # 1. Check DEMO_Combined structure and data
                print("-- Check DEMO_Combined structure and data")
                cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'DEMO_Combined' AND column_name IN ('AGE_Years_fixed', 'COUNTRY_CODE', 'Gender');")
                print(cur.fetchall())
                cur.execute("SELECT COUNT(*) FROM \"DEMO_Combined\" WHERE \"AGE_Years_fixed\" IS NOT NULL;")
                print(cur.fetchone())
                cur.execute("SELECT COUNT(*) FROM \"DEMO_Combined\" WHERE \"COUNTRY_CODE\" IS NOT NULL;")
                print(cur.fetchone())
                cur.execute("SELECT COUNT(*) FROM \"DEMO_Combined\" WHERE \"Gender\" IS NOT NULL;")
                print(cur.fetchone())
                print("\n")

                # 2. Check ALIGNED_DEMO_DRUG_REAC_INDI_THER table
                print("-- Check ALIGNED_DEMO_DRUG_REAC_INDI_THER table")
                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'ALIGNED_DEMO_DRUG_REAC_INDI_THER';")
                print(cur.fetchall())
                cur.execute("SELECT COUNT(*) FROM \"ALIGNED_DEMO_DRUG_REAC_INDI_THER\";")
                print(cur.fetchone())
                cur.execute("SELECT * FROM \"ALIGNED_DEMO_DRUG_REAC_INDI_THER\" LIMIT 10;")  # Sample data
                print(cur.fetchall())
                print("\n")

                # 3. Check deletion from COMBINED_DELETED_CASES_REPORTS
                print("-- Check deletion from COMBINED_DELETED_CASES_REPORTS")
                cur.execute("SELECT COUNT(*) FROM \"Aligned_DEMO_DRUG_REAC_INDI_THER\" WHERE CASEID IN (SELECT Field1 FROM \"COMBINED_DELETED_CASES_REPORTS\");")
                print(cur.fetchone())
                print("\n")

                print("-- End of debugging queries")

    except (psycopg.Error, FileNotFoundError, KeyError, json.JSONDecodeError) as e:
        if isinstance(e, FileNotFoundError):
            print(f"Error: Configuration file '{config_file}' not found.")
        elif isinstance(e, json.JSONDecodeError):
            print(f"Error: Invalid JSON format in '{config_file}'.")
        elif isinstance(e, KeyError):
            print(f"Error: Missing 'database' section or connection parameters in '{config_file}'.")
        else:
            print(f"Error executing debugging queries: {e}")

# Example usage:
execute_debugging_queries()
