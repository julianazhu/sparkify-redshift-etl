"""Cleans up AWS resources that were required for the ETL Pipeline.

This module should be called from the terminal to clean up the AWS resources
created in `setup_redshift.py` once the Redshift cluster is no longer needed.

Typical Usage example:
    $ python3 cleanup_redshift.py
"""
import os
import json
from setup_redshift import get_aws_clients

AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET = os.environ['AWS_SECRET_ACCESS_KEY']
CFG_FILE = 'dwh_config.json'


def delete_redshift_cluster(config, redshift):
    """ Deletes the Redshift cluster specified in config

    Args:
        config: a ConfigParser object
        redshift: a boto3 client object for the AWS Redshift service
    """
    try:
        print("Deleting Redshift Cluster: ", config['CLUSTER']['IDENTIFIER'])
        return redshift.delete_cluster(
            ClusterIdentifier=config['CLUSTER']['IDENTIFIER'],
            SkipFinalClusterSnapshot=True
        )
    except Exception as e:
        print(e)


def detach_and_delete_iam_policy(config, iam):
    """ Detaches policy from Redshift role & deletes the policy

    Args:
        config: a ConfigParser object
        iam: a boto3 client object for the AWS IAM service

    Returns:
        dict with AWS API response
    """
    try:
        print("Detaching policy: ", config['IAM_ROLE']['POLICY_NAME'])
        iam.detach_role_policy(RoleName=config['IAM_ROLE']['NAME'],
                               PolicyArn=config['IAM_ROLE']['POLICY_ARN'])

        print("Deleting policy: ", config['IAM_ROLE']['POLICY_NAME'])
        return iam.delete_policy(PolicyArn=config['IAM_ROLE']['POLICY_ARN'])
    except Exception as e:
        print(e)


def delete_iam_role(config, iam):
    """ Deletes the IAM role used by the Redshift cluster

    Args:
        config: a ConfigParser object
        iam: a boto3 client object for the AWS IAM service

    Returns:
        dict with AWS API response
    """
    try:
        print("Deleting IAM Role: ", config['IAM_ROLE']['NAME'])
        return iam.delete_role(RoleName=config['IAM_ROLE']['NAME'])
    except Exception as e:
        print(e)


def cleanup_redshift_cluster():
    with open(CFG_FILE) as f:
        config = json.load(f)

    iam, redshift = get_aws_clients(config)
    delete_redshift_cluster(config, redshift)
    detach_and_delete_iam_policy(config, iam)
    delete_iam_role(config, iam)

    print("All done, exit script.")


if __name__ == "__main__":
    cleanup_redshift_cluster()
