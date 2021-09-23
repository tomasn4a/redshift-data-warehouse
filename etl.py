import configparser
import time

import psycopg2

from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        t0 = time.time()
        print(f'Loading data from S3 into {query[0]} table')
        cur.execute(query[1])
        conn.commit()
        print(f'\tSuccesfully loaded {query[0]} in {time.time() - t0} seconds\n')


def insert_tables(cur, conn):
    for query in insert_table_queries:
        t0 = time.time()
        print(f'Loading data from staging tables into {query[0]} table')
        cur.execute(query[1])
        conn.commit()
        print(f'\tSuccesfully loaded {query[0]} in {time.time() - t0} seconds\n')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    )
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()