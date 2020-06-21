"""
This module defines an ETL pipeline that imports data song and log
data from .json files in AWS S3 buckets into a star-schema Redshift database
with the following structure:

#####################    FACT TABLE:     ####################
songplays   - records in log data associated with song plays
##################### DIMENSION TABLES: #####################
users       - users in the app
songs       - songs in music database
artists     - artists in music database
time        - timestamps of records in songplays broken down into specific units
"""
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
    config.read('dwh_config.json')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
