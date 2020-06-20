"""
This testing suite defines tests for the S3 to Redshift ETL pipeline.

VERY IMPORTANT: Imports functions from `etl.py` individually so
that the `moto` mocks are set up before the `boto3` client is established.
"""
import os
import pytest
import boto3
from moto import mock_s3


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'


@pytest.fixture(scope='function')
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='eu-central-1')

