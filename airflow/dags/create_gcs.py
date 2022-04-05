
import os
import configparser
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator

raw_data_lake = 'raw_data_lake'
optimized_data_lake = 'optimized_data_lake'

default_args = {
    'owner': 'hieu',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('create_gcs_buckets_dag',
          default_args=default_args,
          description='Create GCS Buckets',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


create_raw_data_lake = GCSCreateBucketOperator(
    task_id="create_raw_data_lake",
    bucket_name=raw_data_lake,
    storage_class="MULTI_REGIONAL",
    location="ASIA",
    gcp_conn_id="gcp_credentials",
    dag = dag
)

create_optimized_data_lake = GCSCreateBucketOperator(
    task_id="create_optimized_data_lake",
    bucket_name=optimized_data_lake,
    storage_class="MULTI_REGIONAL",
    location="ASIA",
    gcp_conn_id="gcp_credentials",
    dag = dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# DAG dependencies

start_operator >> create_raw_data_lake >> end_operator
start_operator >> create_optimized_data_lake >> end_operator

