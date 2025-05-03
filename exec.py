# https://www.psycopg.org/psycopg3/docs/api/sql.html#sql-sql-string-composition
import psycopg
from psycopg import sql

files = [
    {'filename': '/faers/data/faers_ascii_2012Q4/drug12q4.txt', 'table': 'drug12q4'},
    {'filename': '/faers/data/faers_ascii_2013Q1/DRUG13Q1.txt', 'table': 'drug13q1'}
]

def execute_sql_file(filename: str, params: dict, conn: psycopg.Connection) -> None:
    with open(filename, 'r') as f:
        sql_code = f.read()
    
    # PROBLEM: SQL Injection is a big risk here
    #TODO can we  Dynamically construct the COPY command with copy_expert()
    # instead? is that better?

    # TODO try except blocks everywhere

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(sql_code).format(
                table = sql.Identifier(params['table']),
                filename = sql.Literal(params['filename'])
            )
        )
    conn.commit()
    print(f"Copy into {params['table']} completed.")

with psycopg.connect(
    dbname="faers_a",
    user="sa",
    host="/var/run/postgresql"
) as conn:
    execute_sql_file('2dynamic-pgtest.sql', files[0], conn)
    execute_sql_file('2dynamic-pgtest.sql', files[1], conn)

