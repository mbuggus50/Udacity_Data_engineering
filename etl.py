import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    
    
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

    
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    try: 
        cur.execute("SELECT * from staging_songs limit 10;")


    except psycopg2.Error as e: 
        print("Error: select *")
        print (e)

    row = cur.fetchone()
    while row:
        print(row)
        row = cur.fetchone()

    conn.close()


if __name__ == "__main__":
    main()