
import os
import configparser
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.operators.bash import BashOperator

raw_datalake_bucket_name = 'hieu-raw-datalake'

default_args = {
    'owner': 'hieu',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('extract_datalake_dag',
          default_args=default_args,
          description='Extract data into S3',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_raw_datalake = S3CreateBucketOperator(
    task_id='Create_raw_datalake',
    bucket_name=raw_datalake_bucket_name,
    aws_conn_id='aws_credentials',
    region_name='ap-southeast-1',
    dag = dag
)

upload_synthea_data = BashOperator(
    task_id='Upload_synthea_data',
    bash_command='python /opt/airflow/scripts/upload_synthea_data.py',
    dag = dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# DAG dependencies

start_operator >> create_raw_datalake >> upload_synthea_data >> end_operator
