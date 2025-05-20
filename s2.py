import psycopg

def execute_sql_file(filename: str, root_dir: str, conn: psycopg.Connection) -> None:
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            sql_code = f.read()
        # Replace {root_dir} placeholder, ensuring forward slashes
        sql_code = sql_code.replace('{root_dir}', root_dir.replace('\\', '/'))
        with conn.cursor() as cur:
            cur.execute(sql_code)
        conn.commit()
        print(f"Copy into {filename} completed.")
    except Exception as e:
        print(f"Error executing {filename}: {e}")
        conn.rollback()

with psycopg.connect(
    dbname="faers_a",
    user="sa",
    password="123",
    host="localhost",
    port="5433"
) as conn:
    execute_sql_file('s2.sql', 'C:/Users/xocas/OneDrive/Desktop/faers-scripts/', conn)

