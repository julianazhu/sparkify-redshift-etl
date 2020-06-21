"""Defines tests for setting up the Redshift cluster.

VERY IMPORTANT: Use local Python imports in each test to ensure moto mocks
established before clients are set up (avoiding potential actual AWS
infrastructure modifications).
"""
import os
import pytest
import boto3
import json
from moto import mock_redshift, mock_iam

CFG_FILE = 'dwh_config.json'


@pytest.fixture(scope='function')
def config():
    cur_path = os.path.dirname(__file__)
    cfg_path = os.path.relpath('../' + CFG_FILE, cur_path)

    with open(cfg_path) as f:
        config = json.load(f)
    return config


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'


@pytest.fixture(scope='function')
def redshift(aws_credentials):
    with mock_redshift():
        yield boto3.client('redshift', region_name='eu-central-1')


@pytest.fixture(scope='function')
def iam(aws_credentials):
    with mock_iam():
        yield boto3.client('iam', region_name='eu-central-1')


def test_creates_iam_role_and_attach_policy(config, iam):
    from setup_redshift import create_iam_role
    from setup_redshift import attach_iam_role_policy

    with mock_iam():
        result = create_iam_role(config, iam)
        assert result['Role']['RoleName'] == config['IAM_ROLE']['NAME']

        response = attach_iam_role_policy(config, iam)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200


def test_creates_redshift_cluster(config, redshift):
    from setup_redshift import start_redshift_cluster

    with mock_redshift():
        role_arn = "arn:aws:iam::711914867513:role/dwhRole"

        response = start_redshift_cluster(config, redshift, role_arn)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200


def test_confirms_cluster_available(config, redshift, monkeypatch):
    from setup_redshift import confirm_cluster_available
    import time

    def sleep(seconds):
        pass

    monkeypatch.setattr(time, 'sleep', sleep)

    with mock_redshift():
        redshift.create_cluster(
            ClusterIdentifier=config['CLUSTER']['IDENTIFIER'],
            NodeType=config['CLUSTER']['NODE_TYPE'],
            MasterUsername=config['CLUSTER']['DB_USER'],
            MasterUserPassword=config['CLUSTER']['DB_PASSWORD']
        )

        result = confirm_cluster_available(config, redshift)
        assert result == 'available'
