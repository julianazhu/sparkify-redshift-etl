"""Sets up AWS resources that are required for the ETL Pipeline.

This will create:
    - 1x IAM Role with S3 readonly access & Redshift Service access to EC2
    - 1x Redshift Cluster
"""
import os
import json
import boto3
import time

AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET = os.environ['AWS_SECRET_ACCESS_KEY']
CFG_FILE = 'dwh_config.json'


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


def create_iam_role(config, iam):
    """Creates an AWS IAM Role for Redshift

    Args:
        config: a ConfigParser object
        iam: a boto3 client object for the AWS IAM service

    Returns:
        A dict of the created IAM role's attributes
    """
    print("Creating IAM Role: ", config['IAM_ROLE']['NAME'])
    return iam.create_role(
        Path='/',
        RoleName=config['IAM_ROLE']['NAME'],
        AssumeRolePolicyDocument=json.dumps(
            config['IAM_ROLE']['TRUST_POLICY']
        ),
        Description='Role for Accessing Redshift data warehouse'
    )


def attach_iam_role_policy(config, iam):
    """Creates & attaches S3 & EC2 policy to the newly created IAM role

    Attaches a policy to an AWS IAM role for read-only access to
    S3 buckets and Redshift service-role access to EC2.

    Args:
        config: a ConfigParser object
        iam: a boto3 client object for the AWS IAM service

    Returns:
        A dict with AWS API response metadata on the attach_role_policy call
    """
    managed_policy_name = config['IAM_ROLE']['POLICY_NAME']
    print("Creating managed policy: ", managed_policy_name)

    response = iam.create_policy(
        PolicyName=managed_policy_name,
        PolicyDocument=json.dumps(config['IAM_ROLE']['MANAGED_POLICY'])
    )

    print(
        "Attaching policy: {} to IAM Role {}".format(
            managed_policy_name,
            config['IAM_ROLE']['NAME']
        )
    )
    return iam.attach_role_policy(
        PolicyArn=response['Policy']['Arn'],
        RoleName=config['IAM_ROLE']['NAME']
    )


def start_redshift_cluster(config, redshift, role_arn):
    """ Creates a Redshift cluster based on configs

    Args:
        config: a ConfigParser object
        redshift: a boto3 client object for the AWS Redshift service
        role_arn: String

    Returns:
        A dict with the AWS API response metadata of the create_cluster call
    """
    print("Creating Redshift Cluster: ", config['CLUSTER']['IDENTIFIER'])
    return redshift.create_cluster(
        DBName=config['CLUSTER']['DB_NAME'],
        ClusterIdentifier=config['CLUSTER']['IDENTIFIER'],
        ClusterType=config['CLUSTER']['CLUSTER_TYPE'],
        NodeType=config['CLUSTER']['NODE_TYPE'],
        MasterUsername=config['CLUSTER']['DB_USER'],
        MasterUserPassword=config['CLUSTER']['DB_PASSWORD'],
        Port=int(config['CLUSTER']['DB_PORT']),
        NumberOfNodes=int(config['CLUSTER']['NUM_NODES']),
        IamRoles=[role_arn]
    )


def confirm_cluster_available(config, redshift):
    """ Repeatedly polls a redshift cluster until its status is `available`

    Args:
        config: a ConfigParser object
        redshift: a boto3 client object for the AWS Redshift service

    Returns:
         String describing cluster status: `available` or `not_available`
    """
    print("Waiting for cluster to become live:")

    timeout = time.time() + 60 * 6
    cluster_status = "not_available"
    while cluster_status != "available":
        print("...", end='', flush=True)    # loading "bar"
        if time.time() >= timeout:
            print("Cluster response has timed out. Please run "
                  "redshift_cleanup.")
            return cluster_status

        cluster_status = redshift.describe_clusters(
            ClusterIdentifier=config['CLUSTER']['IDENTIFIER']
        )['Clusters'][0]['ClusterStatus']
        time.sleep(30)

    print("Cluster {} is now live!".format(config['CLUSTER']['IDENTIFIER']))
    return cluster_status


def save_cluster_endpoint(config, redshift):
    config['CLUSTER']['HOST'] = redshift.describe_clusters(
        ClusterIdentifier=config['CLUSTER']['IDENTIFIER']
    )['Clusters'][0]['Endpoint']['Address']

    with open(CFG_FILE, 'w') as f:
        json.dump(config, f, indent=2)


def setup_redshift_cluster():
    with open(CFG_FILE) as f:
        config = json.load(f)

    iam, redshift = get_aws_clients(config)
    create_iam_role(config, iam)
    attach_iam_role_policy(config, iam)
    role_arn = iam.get_role(RoleName=config['IAM_ROLE']['NAME'])['Role']['Arn']
    start_redshift_cluster(config, redshift, role_arn)
    confirm_cluster_available(config, redshift)
    save_cluster_endpoint(config, redshift)
