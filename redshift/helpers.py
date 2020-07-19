import os
import json
import boto3

AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET = os.environ['AWS_SECRET_ACCESS_KEY']
CFG_FILE = 'redshift/dwh_config.json'


def load_config(config_file=CFG_FILE):
    """ Loads the configurations from the specified file """
    with open(config_file) as f:
        return json.load(f)


def save_config(config, config_file=CFG_FILE):
    """ Saves configurations to the specified file"""
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)


def get_aws_clients(config):
    """Retrieves AWS clients for iam & redshift services

    Args:
        config: a ConfigParser object

    Returns:
        s3: AWS client object for s3 service
        redshift: AWS client object for Redshift service
    """

    iam = boto3.client("iam",
                       region_name=config['AWS']['REGION'],
                       aws_access_key_id=AWS_ACCESS_KEY,
                       aws_secret_access_key=AWS_SECRET
                       )

    redshift = boto3.client("redshift",
                            region_name=config['AWS']['REGION'],
                            aws_access_key_id=AWS_ACCESS_KEY,
                            aws_secret_access_key=AWS_SECRET
                            )
    return iam, redshift
