
import os
import configparser
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator


default_args = {
    'owner': 'hieu',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('aws_s3_to_gcp_gcs',
          default_args=default_args,
          description='Extract files from AWS S3 to GCP GCS',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


s3_to_gcs_op = S3ToGCSOperator(
    task_id="s3_to_gcs",
    bucket="hieu-raw-datalake",
    aws_conn_id='aws_credentials',
    gcp_conn_id="gcp_credentials",
    dest_gcs="gs://raw_data_lake/",
    replace=False,
    dag=dag
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# DAG dependencies

start_operator >> s3_to_gcs_op >> end_operator

