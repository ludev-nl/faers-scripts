# https://www.psycopg.org/psycopg3/docs/api/sql.html#sql-sql-string-composition
import psycopg
from psycopg import sql

# files = [
#     {'filename': '/faers/data/faers_ascii_2012Q4/drug12q4.txt', 'table': 'drug12q4'},
#     {'filename': '/faers/data/faers_ascii_2013Q1/DRUG13Q1.txt', 'table': 'drug13q1'}
# ]
files = {'root_dir': '/faers/data/'}

def execute_sql_file(filename: str, params: dict, conn: psycopg.Connection) -> None:
    with open(filename, 'r') as f:
        sql_code = f.read()
    
    # PROBLEM: SQL Injection is a big risk here
    #TODO can we  Dynamically construct the COPY command with copy_expert()
    # instead? is that better?

    # TODO try except blocks everywhere

    # print(sql.SQL(sql_code).format(root_dir=sql.Literal(params['root_dir'])).as_string())
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(sql_code).format(
                root_dir = sql.Literal(params['root_dir'])
            )
        )
    conn.commit()
    print(f"Copy into s2 completed.")

with psycopg.connect(
    # dbname="faers_a",
    dbname="test_noah",
    user="sa",
    host="/var/run/postgresql"
) as conn:
    execute_sql_file('s2.sql', files, conn)

