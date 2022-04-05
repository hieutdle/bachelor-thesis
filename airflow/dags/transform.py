import os
import configparser
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator
from numpy import diag

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

dag = DAG('transform',
          default_args=default_args,
          description='Transform',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
    task_id='create_dataproc_cluster',
    project_id='bachelor-thesis-344103',
    cluster_name='transform-data-lake',
    num_workers=2,
    region='asia-southeast1',
    zone='asia-southeast1-b',
    init_actions_uris=['gs://scripts_data_lake/pip-install.sh'],
    image_version='2.0',
    master_machine_type='n1-standard-4',
    worker_machine_type='n1-standard-4',
    gcp_conn_id ='gcp_credentials',
    dag = dag
)


run_dataproc_pyspark = dataproc_operator.DataprocSubmitPySparkJobOperator(
    task_id='run_dataproc_pyspark',
    region='asia-southeast1',
    main='gs://scripts_data_lake/transform_code.py',
    cluster_name='transform-data-lake',
    dataproc_jars=['file:///usr/lib/delta/jars/delta-core_2.12-1.0.0.jar'],
    gcp_conn_id ='gcp_credentials',
    dag = dag
    )

# Delete Cloud Dataproc cluster.
delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
    task_id='delete_dataproc_cluster',
    project_id ='bachelor-thesis-344103',
    region='asia-southeast1',
    cluster_name='transform-data-lake',
    # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
    # even if the Dataproc job fails.
    trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
    gcp_conn_id ='gcp_credentials',
    dag = dag)
    
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Define DAG dependencies.
start_operator >> create_dataproc_cluster >> run_dataproc_pyspark >> delete_dataproc_cluster >> end_operator