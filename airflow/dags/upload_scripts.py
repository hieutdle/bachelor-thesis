
import os
import configparser
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


scripts_data_lake = 'scripts_data_lake'


default_args = {
    'owner': 'hieu',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('upload_scripts',
          default_args=default_args,
          description='Upload Scripts for Data Lake',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


create_scripts_data_lake = GCSCreateBucketOperator(
    task_id="create_scripts_data_lake",
    bucket_name=scripts_data_lake,
    storage_class="MULTI_REGIONAL",
    location="ASIA",
    gcp_conn_id="gcp_credentials",
    dag = dag
)

upload_python_file = LocalFilesystemToGCSOperator(
    task_id="upload_python_file",
    dst="transform_code.py",
    bucket=scripts_data_lake,
    src="/opt/airflow/scripts/transform_code.py",
    gcp_conn_id="gcp_credentials",
    dag = dag
)

upload_sh_file = LocalFilesystemToGCSOperator(
    task_id="upload_sh_file",
    dst="pip-install.sh",
    bucket=scripts_data_lake,
    src="/opt/airflow/scripts/pip-install.sh",
    gcp_conn_id="gcp_credentials",
    dag = dag
)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# DAG dependencies

start_operator >> create_scripts_data_lake 
create_scripts_data_lake >> upload_python_file
create_scripts_data_lake >> upload_sh_file
upload_python_file >> end_operator
upload_sh_file >> end_operator
