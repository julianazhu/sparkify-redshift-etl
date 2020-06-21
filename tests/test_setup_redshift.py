"""Defines tests for setting up the Redshift cluster.

VERY IMPORTANT: Use local Python imports in each test to ensure moto mocks
established before clients are set up (avoiding potential actual AWS
infrastructure modifications).
"""
import os
import pytest
import boto3
import json
from moto import mock_s3, mock_iam

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
    with mock_s3():
        yield boto3.client('redshift', region_name='eu-central-1')


@pytest.fixture(scope='function')
def iam(aws_credentials):
    with mock_iam():
        yield boto3.client('iam', region_name='eu-central-1')


def test_create_iam_role_and_attach_policy(config, iam):
    from setup_redshift import create_iam_role
    from setup_redshift import attach_iam_role_policy

    with mock_iam():
        result = create_iam_role(config, iam)
        assert result['Role']['RoleName'] == config['IAM_ROLE']['NAME']

        response = attach_iam_role_policy(config, iam)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200