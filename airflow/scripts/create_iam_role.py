import boto3
import configparser
import json

def create_iam_role_f(iam_client):
    role = iam_client.create_role(
        RoleName='HieuEmrRole',
        Description='Allows EMR to call AWS services on your behalf',
        AssumeRolePolicyDocument=json.dumps({
            'Version': '2012-10-17',
            'Statement': [{
                'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'elasticmapreduce.amazonaws.com'}
            }]
        })
    )

    iam_client.attach_role_policy(
        RoleName='HieuEmrRole',
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess'
    )

    iam_client.attach_role_policy(
        RoleName='HieuEmrRole',
        PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
    )

    return role

def main():
    """Main Script to setup the cluster and bucket
    Args:
        None
        
    Returns:
        None
    """

    config = configparser.ConfigParser()
    config.read('/opt/airflow/scripts/dl.cfg')

    iam_client = boto3.client('iam')
    create_iam_role_f(iam_client)
    

if __name__ == '__main__':
    main()


