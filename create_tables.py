import json
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

CFG_FILE = 'dwh_config.json'


def drop_tables(cur, conn):
    """ Drops all the tables in Redshift Cluster

    Args:
        cur: Psycopg2 DB cursor object
        conn: psycopg2 DB connection object

    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print("All tables dropped")


def create_tables(cur, conn):
    """ Creates the tables for Redshift cluster

    staging_events, staging_songs, songplays,
    users, songs, artists, times

    Args:
        cur: Psycopg2 DB cursor object
        conn: psycopg2 DB connection object

    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("All tables created")


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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
