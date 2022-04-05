
import os
import configparser
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteBucketOperator
from airflow.operators.bash import BashOperator

raw_datalake_bucket_name = 'hieu-raw-datalake'

default_args = {
    'owner': 'hieu',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('delete_all_dag',
          default_args=default_args,
          description='Delete data in S3',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

delete_raw_datalake = S3DeleteBucketOperator(
    task_id='Delete_raw_datalake',
    bucket_name=raw_datalake_bucket_name,
    aws_conn_id='aws_credentials',
    force_delete=True,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# DAG dependencies

start_operator >> delete_raw_datalake >> end_operator
