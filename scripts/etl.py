"""
This module defines an ETL pipeline that imports data song and log
data from .json files in AWS S3 buckets into a Redshift cluster DB.
"""
import configparser
import psycopg2
import json
from scripts.sql_queries import copy_table_queries, insert_table_queries

CFG_FILE = 'dwh_config.json'


def load_staging_tables(cur, conn):
    """ Loads song and log data from Udacity S3 buckets
    into the staging tables.

    Args:
        cur: Psycopg2 DB cursor object
        conn: psycopg2 DB connection object

    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print("Loaded the staging tables")


def insert_tables(cur, conn):
    """ Inserts data from staging tables into the
    final star-schema fact & dimension tables

    Args:
        cur: Psycopg2 DB cursor object
        conn: psycopg2 DB connection object

    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    print("Loaded the production tables")


def etl():
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
