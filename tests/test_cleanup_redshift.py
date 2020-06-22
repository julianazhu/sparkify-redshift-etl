"""Defines tests for cleaning up the Redshift cluster, IAM roles & polices.

VERY IMPORTANT: Use local Python imports in each test to ensure moto mocks
established before clients are set up (avoiding potential actual AWS
infrastructure modifications).
"""


def test_deletes_redshift_cluster(config, redshift):
    from cleanup_redshift import delete_redshift_cluster

    redshift.create_cluster(
        ClusterIdentifier=config['CLUSTER']['IDENTIFIER'],
        NodeType=config['CLUSTER']['NODE_TYPE'],
        MasterUsername=config['CLUSTER']['DB_USER'],
        MasterUserPassword=config['CLUSTER']['DB_PASSWORD']
    )

    response = delete_redshift_cluster(config, redshift)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


def test_deletes_iam_role(config, iam):
    from cleanup_redshift import delete_iam_role

    iam.create_role(
        RoleName=config['IAM_ROLE']['NAME'],
        AssumeRolePolicyDocument="test"
    )

    response = delete_iam_role(config, iam)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200