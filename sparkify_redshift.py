""" This script creates the Sparkify songplays DB on a Redshift cluster.

This script calls scripts for setting up the necessary permissions, roles and
resources on Redshift to create the Redshift data warehouse as well as create
and copy the data to the tables from the S3 json log file to the songplays
DB, a star-schema DB optimised for queries on songs played by Sparkify users.

Typical Usage example:
    $ export AWS_ACCESS_KEY_ID=<your_aws_access_key_id>
    $ export AWS_SECRET_ACCESS_KEY=<your_aws_secret_access_key>
    $ python3 sparkify_redshift.py
"""
import sys
import getopt
from scripts.setup_redshift import setup_redshift_cluster
from scripts.create_tables import create_db_tables
from scripts.etl import etl


def main(argv):
    try:
        opts, args = getopt.getopt(argv, "c")
    except getopt.GetoptError:
        print("USAGE: sparkify_redshift.py -c")
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-c':
            setup_redshift_cluster()
            create_db_tables()
    etl()


if __name__ == "__main__":
   main(sys.argv[1:])
