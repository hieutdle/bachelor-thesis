import boto3
import configparser
import json


def upload_code(s3_client, bucket_name):
    """Upload ETL file to run on AWS.
    Args:
        s3_client: S3 Client
        bucket_name: Name of the bucket
    Returns:
        None
    """
    s3_client.upload_file('/opt/airflow/scripts/transform_code.py',bucket_name,'transform_code.py')

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
    upload_code(s3_client, config['BUCKET']['CODE_BUCKET'])


if __name__ == '__main__':
    main()