"""
This module defines an ETL pipeline that imports data song and log
data from .json files in AWS S3 buckets into a Redshift cluster.

"""
import configparser
import psycopg2
import json
from sql_queries import copy_table_queries, insert_table_queries

CFG_FILE = 'dwh_config.json'


def load_staging_tables(cur, conn):


    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    with open(CFG_FILE) as f:
        config = json.load(f)

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            config['CLUSTER']['HOST'],
            config['CLUSTER']['DB_NAME'],
            config['CLUSTER']['DB_USER'],
            config['CLUSTER']['DB_PASSWORD'],
            config['CLUSTER']['DB_PORT'],
        )
    )

    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
