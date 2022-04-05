import boto3
import configparser
import json


def upload_files(s3_client, bucket_name):
    """Upload all data files to S3.
    Args:
        s3_client: S3 Client
        bucket_name: Name of the bucket
    Returns:
        None
    """
    datasets= ['allergies',
          'careplans',
          'conditions',
          'devices',
          'encounters',
          'imaging_studies',
          'immunizations',
          'medications',
          'observations',
          'organizations',
          'patients',
          'payer_transitions',
          'payers',
          'procedures',
          'providers',
          'supplies'
         ]
    for dataset in datasets:
        s3_client.upload_file('/opt/airflow/dataset/%s.csv'%dataset,bucket_name,'%s.csv'%dataset)

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
    upload_files(s3_client,config['BUCKET']['DATA_BUCKET'])


if __name__ == '__main__':
    main()