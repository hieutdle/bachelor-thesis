
import os
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

raw_datalake_bucket_name = 'hieu-raw-datalake'
spark_script_bucket_name = 'hieu-spark-script'
optimized_datalake_bucket_name = 'hieu-optimized-datalake'

default_args = {
    'owner': 'hieu',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

SPARK_ETL_STEPS = [
    {
        'Name': 'Setup Debugging',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['state-pusher-script']
        }
    },
    {
        'Name': 'Setup - copy code files',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['aws', 's3', 'cp', 's3://' + spark_script_bucket_name, '/home/hadoop/', '--recursive']
        }
    },
    {
        'Name': 'Transform Data Lake',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', 's3://hieu-spark-script/transform_code.py', 's3a://' + raw_datalake_bucket_name,
                     's3a://' + optimized_datalake_bucket_name]
        }
    }
]

JOB_FLOW_OVERRIDES = {
    "Name": "hieu-spark-emr-cluster",
    "LogUri": "s3://hieulogfile",
    "ReleaseLabel": "emr-6.1.0",
    "Applications": [
        {
            "Name": "Spark"
        },
        {
            "Name": "Hadoop"
        }
    ],
    "Configurations": [
        {
        "Classification": "spark-env",
        "Configurations": [
            {
            "Classification": "export",
            "Properties": {
                "PYSPARK_PYTHON": "/usr/bin/python3"
            }
            }
        ]
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "ON_DEMAND", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    "VisibleToAllUsers": True,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "HieuEmrRole",
}

dag = DAG('transform_datalake_dag',
          default_args=default_args,
          description='Transform data lake in S3',
          schedule_interval='@monthly',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


create_cluster = EmrCreateJobFlowOperator(
    task_id='Create_EMR_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_credentials',
    emr_conn_id='emr_default',
    dag=dag
)

create_optimized_datalake_bucket = S3CreateBucketOperator(
    task_id='Create_optimized_datalake_bucket',
    bucket_name=optimized_datalake_bucket_name,
    aws_conn_id='aws_credentials',
    region_name='ap-southeast-1',
    dag=dag
)


create_spark_script_bucket = S3CreateBucketOperator(
    task_id='Create_spark_script_bucket',
    bucket_name=spark_script_bucket_name,
    aws_conn_id='aws_credentials',
    region_name='ap-southeast-1',
    dag=dag
)

upload_transform_code = BashOperator(
    task_id='Upload_transform_code',
    bash_command='python /opt/airflow/scripts/upload_transform_code.py',
    dag=dag
)

add_jobflow_steps = EmrAddStepsOperator(
    task_id='Add_jobflow_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_cluster', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=SPARK_ETL_STEPS,
    dag=dag
)

check_transform_processing = EmrStepSensor(
    task_id='Watch_transform_processing_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[2] }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

delete_cluster = EmrTerminateJobFlowOperator(
    task_id='Delete_EMR_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_cluster', key='return_value') }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# DAG dependencies

start_operator >> create_optimized_datalake_bucket >> create_cluster
start_operator >> create_spark_script_bucket >> upload_transform_code >> create_cluster
create_cluster >> add_jobflow_steps >> check_transform_processing >> delete_cluster >> end_operator