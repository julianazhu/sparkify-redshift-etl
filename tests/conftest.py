import pytest
import os
import json
import boto3
from moto import mock_redshift, mock_iam

CFG_FILE = 'dwh_config.json'


@pytest.fixture(scope='module')
def config():
    cur_path = os.path.dirname(__file__)
    cfg_path = os.path.relpath('../' + CFG_FILE, cur_path)

    with open(cfg_path) as f:
        config = json.load(f)
    return config


@pytest.fixture(scope='module')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'


@pytest.fixture(scope='function')
def iam(aws_credentials):
    with mock_iam():
        yield boto3.client('iam', region_name='eu-central-1')


@pytest.fixture(scope='function')
def redshift(aws_credentials):
    with mock_redshift():
        yield boto3.client('redshift', region_name='eu-central-1')


@pytest.fixture(scope='function')
def cluster(redshift, config):
    redshift.create_cluster(
        ClusterIdentifier=config['CLUSTER']['IDENTIFIER'],
        NodeType=config['CLUSTER']['NODE_TYPE'],
        MasterUsername=config['CLUSTER']['DB_USER'],
        MasterUserPassword=config['CLUSTER']['DB_PASSWORD']
    )