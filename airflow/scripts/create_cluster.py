import boto3
import time
import json
import configparser
from botocore.exceptions import ClientError

redshift_client = boto3.client('redshift', region_name='ap-southeast-1')
ec2 = boto3.resource('ec2', region_name='ap-southeast-1')

def create_udacity_cluster(config):
    """Create an Amazon Redshift cluster

    Args:
        config: configurations file
    Returns:
        response['Cluster']: return cluster dictionary information
    Raises:
        ClientError
    """

    try:
        response = redshift_client.create_cluster(
            ClusterIdentifier='udacity-cluster',
            ClusterType='multi-node',
            NumberOfNodes=2,
            NodeType='dc2.large',
            PubliclyAccessible=True,
            DBName=config.get('CLUSTER', 'DB_NAME'),
            MasterUsername=config.get('CLUSTER', 'DB_USER'),
            MasterUserPassword=config.get('CLUSTER', 'DB_PASSWORD'),
            Port=int(config.get('CLUSTER', 'DB_PORT')),
            IamRoles=[config.get('IAM_ROLE', 'ROLE_ARN')],
            VpcSecurityGroupIds=['sg-077f9a08ba80c09e4']
        )
    except ClientError as e:
        print(f'ERROR: {e}')
        return None
    else:
        return response['Cluster']



def wait_for_creation(cluster_id):
    """Wait for cluster creation

    Args:
        cluster_id: Cluster identifier
    Returns:
        cluster_info: return cluster dictionary information
    Raises:
        None
    """
    while True:
        response = redshift_client.describe_clusters(ClusterIdentifier=cluster_id)
        cluster_info = response['Clusters'][0]
        if cluster_info['ClusterStatus'] == 'available':
            break
        time.sleep(30)

    return cluster_info


def opentcp(config,cluster_info):
    """Open an incoming  TCP port to access the cluster endpoint
    Args:
        config: configurations file
        cluster_info: cluster dictionary information
    Returns:
        None
    Raises:
        None
    """
    try:
        vpc = ec2.Vpc(id=cluster_info['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(config.getint('CLUSTER', 'DB_PORT')),
            ToPort=int(config.getint('CLUSTER', 'DB_PORT'))
        )
    except Exception as e:
        print(e)


def main():
    """Create cluster"""

    config = configparser.ConfigParser()
    config.read('../dwh.cfg')
    
    cluster_info = create_udacity_cluster(config)
    
    if cluster_info is not None:
        print('Cluster is being created')
        cluster_info = wait_for_creation(cluster_info['ClusterIdentifier'])
        print(f'Cluster has been created.')
        print(f"Endpoint to copy={cluster_info['Endpoint']['Address']}")
        opentcp(config,cluster_info)


if __name__ == '__main__':
    main()
