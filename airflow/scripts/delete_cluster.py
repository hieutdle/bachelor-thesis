import boto3
import time
import configparser
from botocore.exceptions import ClientError

redshift_client = boto3.client('redshift', region_name='us-west-2')


def delete_udacity_cluster(config):
    """Delete Redshift cluster

    Args:
        config: configurations file
    Returns:
        response['Cluster']: return cluster dictionary information
    Raises:
        ClientError
    """
    try:
        response = redshift_client.delete_cluster(
            ClusterIdentifier='udacity-cluster',
            SkipFinalClusterSnapshot=True
        )
    except ClientError as e:
        print(f'ERROR: {e}')
        return None
    else:
        return response['Cluster']


def wait_for_deletion(cluster_id):
    """Wait for cluster deletion

    Args:
        cluster_id: Cluster identifier
    Returns:
        None
    Raises:
        Exception
    """
    while True:
        try:
            redshift_client.describe_clusters(ClusterIdentifier=cluster_id)
        except Exception as e:
            print('Cluster has been deleted')
            break
        else:
            time.sleep(30)


def main():
    """Cluster deletion"""

    config = configparser.ConfigParser()
    config.read('../dwh.cfg')

    cluster_info = delete_udacity_cluster(config)


    if cluster_info is not None:
        print('Cluster is being deleted')
        wait_for_deletion(cluster_info['ClusterIdentifier'])


if __name__ == '__main__':
    main()
