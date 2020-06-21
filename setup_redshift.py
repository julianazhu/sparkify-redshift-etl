"""Sets up AWS resources that are required for the ETL Pipeline.

This module should be called from the terminal to set up the AWS resources
required for the `etl.py` pipeline script. You will need to export AWS
credentials for an existing user with administrator rights before running
this script.

This will create:
    - 1x IAM Role with S3 readonly access & Redshift Service access to EC2
    - 1x Redshift Cluster

Role & cluster details e.g. number of nodes can be configured by updating
`dwh.cfg` prior to running this script.

Typical Usage example:
    $ setenv AWS_ACCESS_KEY_ID=<your_aws_access_key_id>
    $ setenv AWS_ACCESS_KEY_ID=<your_aws_access_key_id>
    $ python3 setup_redshift.py
"""
import os
import json
import configparser
import boto3

AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET = os.environ['AWS_SECRET_ACCESS_KEY']


def get_aws_clients(config):
    ec2 = boto3.resource("ec2",
                         region_name=config['AWS']['REGION'],
                         aws_access_key_id=AWS_ACCESS_KEY,
                         aws_secret_access_key=AWS_SECRET
                         )

    s3 = boto3.resource("s3",
                        region_name=config['AWS']['REGION'],
                        aws_access_key_id=AWS_ACCESS_KEY,
                        aws_secret_access_key=AWS_SECRET
                        )

    iam = boto3.client("iam",
                       region_name=config['AWS']['REGION'],
                       aws_access_key_id=AWS_ACCESS_KEY,
                       aws_secret_access_key=AWS_SECRET
                       )

    redshift = boto3.client("redshift",
                            region_name=REGION,
                            aws_access_key_id=AWS_ACCESS_KEY,
                            aws_secret_access_key=AWS_SECRET
                            )
    return ec2, s3, iam, redshift


def create_iam_role(config, iam):
    """Creates an AWS IAM Role for Redshift

    Args:
        config: a ConfigParser object
        iam: a boto3 client object for the AWS IAM service
    """
    try:
        dwhRole = iam.create_role(
            Path='/',
            RoleName=config['IAM_ROLE']['NAME'],
            AssumeRolePolicyDocument=json.dumps(
                config['IAM_ROLE']['TRUST_POLICY']
            ),
            Description='Role for Accessing Redshift data warehouse'
        )
        print(f"Created IAM Role: {config['IAM_ROLE']['NAME']}")
    except Exception as e:
        print(e)
    return dwhRole


def attach_managed_policy(config, iam):
    """Attaches S3 & EC2 access policy to the newly created IAM role

    Attaches a policy to an AWS IAM role for read-only access to
    S3 buckets and Redshift service-role access to EC2.

    Args:
        config: a ConfigParser object
        iam: a boto3 client object for the AWS IAM service

    Returns:
        None
    """
    managed_policy_name = config['IAM_ROLE']['NAME'] + "-policy"
    response = iam.create_policy(
        PolicyName=managed_policy_name,
        PolicyDocument=json.dumps(config['IAM_ROLE']['MANAGED_POLICY'])
    )
    print(f" Created managed policy: {managed_policy_name}")

    iam.attach_role_policy(
        PolicyArn=response['ARN'],
        RoleName=config['IAM_ROLE']['NAME']
    )
    print(f"Attached policy: {managed_policy_name} to IAM role:"
          f" {config['IAM_ROLE']['NAME']}")


def start_redshift_cluster(config, redshift):
    try:
        response = redshift.create_cluster(
            DBName=config['CLUSTER']['DB_NAME'],
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            ClusterType='multi-node',
            NodeType='dc2.large',
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            Port=int(DWH_PORT),
            NumberOfNodes=2,
            PubliclyAccessible=True,
            IamRoles=['arn:aws:iam::711914867513:role/dwhRole']
        )

    except Exception as e:
        print(e)


def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    ec2, s3, iam, redshift = get_aws_clients(config)
    dwhRole = create_iam_role(config, iam)
    attach_managed_policy(config, iam)
    start_redshift_cluster(config, redshift)



if __name__ == "__main__":
    main()
