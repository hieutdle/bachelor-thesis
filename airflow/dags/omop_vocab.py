
import os
import configparser
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


scripts_data_lake = 'scripts_data_lake'
raw_data_lake = 'raw_data_lake'


default_args = {
    'owner': 'hieu',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('omop_vocab',
          default_args=default_args,
          description='Set up OMOP Vocabulary',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

upload_vocab_file = LocalFilesystemToGCSOperator(
    task_id="upload_vocab_file",
    dst="omop_vocab/",
    bucket=raw_data_lake,
    src="/opt/airflow/scripts/omop_vocab/*.csv",
    gcp_conn_id="gcp_credentials",
    dag = dag
)

upload_transform_code = LocalFilesystemToGCSOperator(
    task_id="upload_transform_code",
    dst="transform_vocab.py",
    bucket=scripts_data_lake,
    src="/opt/airflow/scripts/transform_vocab.py",
    gcp_conn_id="gcp_credentials",
    dag = dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# DAG dependencies

start_operator >> upload_vocab_file >> upload_transform_code >> end_operator