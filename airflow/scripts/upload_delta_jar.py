import boto3
import configparser
import json


def upload_jar(s3_client, bucket_name):
    """Upload all data files to S3.
    Args:
        s3_client: S3 Client
        bucket_name: Name of the bucket
    Returns:
        None
    """
    s3_client.upload_file('/opt/airflow/scripts/delta-core_2.13-1.1.0.jar',bucket_name,'delta-core_2.13-1.1.0.jar')

def main():
    """Main Script to setup the cluster and bucket
    Args:
        None
        
    Returns:
        None
    """
    config = configparser.ConfigParser()

    config.read('/opt/airflow/scripts/dl.cfg')
    
    s3_client = boto3.client(
        's3',
        region_name='ap-southeast-1',
        aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY']
    )
    upload_jar(s3_client,config['BUCKET']['JAR_BUCKET'])


if __name__ == '__main__':
    main()