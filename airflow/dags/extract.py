
import os
import configparser
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator

raw_data_lake = 'raw_data_lake'
optimized_data_lake = 'optimized_data_lake'
scripts = 'scripts_data_lake'

default_args = {
    'owner': 'hieu',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('extract',
          default_args=default_args,
          description='Extract',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_raw_data_lake = GCSCreateBucketOperator(
    task_id="create_raw_data_lake",
    bucket_name=raw_data_lake,
    storage_class="REGIONAL",
    location="ASIA-SOUTHEAST1",
    gcp_conn_id="gcp_credentials",
    dag = dag
)

create_optimized_data_lake = GCSCreateBucketOperator(
    task_id="create_optimized_data_lake",
    bucket_name=optimized_data_lake,
    storage_class="REGIONAL",
    location="ASIA-SOUTHEAST1",
    gcp_conn_id="gcp_credentials",
    dag = dag
)

create_scripts_bucket = GCSCreateBucketOperator(
    task_id="create_scripts_bucket",
    bucket_name=scripts,
    storage_class="REGIONAL",
    location="ASIA-SOUTHEAST1",
    gcp_conn_id="gcp_credentials",
    dag = dag
)

s3_to_gcs_data = S3ToGCSOperator(
    task_id="s3_to_gcs_data",
    bucket="hieu-raw-datalake",
    aws_conn_id='aws_credentials',
    gcp_conn_id="gcp_credentials",
    dest_gcs="gs://raw_data_lake/",
    replace=False,
    dag=dag
)

s3_to_gcs_scripts = S3ToGCSOperator(
    task_id="s3_to_gcs_scripts",
    bucket="hieu-scripts",
    aws_conn_id='aws_credentials',
    gcp_conn_id="gcp_credentials",
    dest_gcs="gs://scripts_data_lake/",
    replace=False,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# DAG dependencies

start_operator >> create_raw_data_lake >> s3_to_gcs_data >> end_operator
start_operator >> create_optimized_data_lake >> end_operator
start_operator >> create_scripts_bucket >> s3_to_gcs_scripts >> end_operator

