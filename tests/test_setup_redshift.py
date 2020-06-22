"""Defines tests for setting up the Redshift cluster.

VERY IMPORTANT: Use local Python imports in each test to ensure moto mocks
established before clients are set up (avoiding potential actual AWS
infrastructure modifications).
"""


def test_creates_iam_role_and_attaches_policy(config, iam):
    from setup_redshift import create_iam_role
    from setup_redshift import attach_iam_role_policy

    result = create_iam_role(config, iam)
    assert result['Role']['RoleName'] == config['IAM_ROLE']['NAME']

    response = attach_iam_role_policy(config, iam)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


def test_creates_redshift_cluster(config, redshift):
    from setup_redshift import start_redshift_cluster

    role_arn = "arn:aws:iam::711914867513:role/dwhRole"

    response = start_redshift_cluster(config, redshift, role_arn)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


def test_confirms_cluster_available(config, redshift, cluster, monkeypatch):
    from setup_redshift import confirm_cluster_available
    import time

    def sleep(seconds):
        pass

    monkeypatch.setattr(time, 'sleep', sleep)
    result = confirm_cluster_available(config, redshift)
    assert result == 'available'