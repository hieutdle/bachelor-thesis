import os
import configparser
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow import models
from airflow.utils import trigger_rule
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
raw_data_warehouse = 'raw_data_warehouse'
optimized_data_warehouse = 'optimized_data_warehouse'

default_args = {
    'owner': 'hieu',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('load',
          default_args=default_args,
          description='Load',
          schedule_interval='0 0 1 1 *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


create_raw_datawarehouse = BigQueryCreateEmptyDatasetOperator(
    dataset_id=raw_data_warehouse,
    project_id='bachelor-thesis-344103',
    gcp_conn_id='gcp_credentials',
    task_id='create_raw_datawarehouse',
    location = 'asia-southeast1',
    dag=dag)

create_optimized_datawarehouse = BigQueryCreateEmptyDatasetOperator(
    dataset_id=optimized_data_warehouse,
    project_id='bachelor-thesis-344103',
    gcp_conn_id='gcp_credentials',
    task_id='create_optimized_datawarehouse',
    location = 'asia-southeast1',
    dag=dag)

create_allergies_table = BigQueryCreateExternalTableOperator(
    task_id="create_allergies_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": raw_data_warehouse,
            "tableId": "allergies"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/allergies/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_careplans_table = BigQueryCreateExternalTableOperator(
    task_id="create_careplans_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": raw_data_warehouse,
            "tableId": "careplans"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/careplans/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_conditions_table = BigQueryCreateExternalTableOperator(
    task_id="create_conditions_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": raw_data_warehouse,
            "tableId": "conditions"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/conditions/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_devices_table = BigQueryCreateExternalTableOperator(
    task_id="create_devices_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": raw_data_warehouse,
            "tableId": "devices"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/devices/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_encounters_table = BigQueryCreateExternalTableOperator(
    task_id="create_encounters_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": raw_data_warehouse,
            "tableId": "encounters"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/encounters/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_imaging_studies_table = BigQueryCreateExternalTableOperator(
    task_id="create_imaging_studies_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": raw_data_warehouse,
            "tableId": "imaging_studies"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/imaging_studies/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_immunizations_table = BigQueryCreateExternalTableOperator(
    task_id="create_immunizations_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": raw_data_warehouse,
            "tableId": "immunizations"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/immunizations/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_medications_table = BigQueryCreateExternalTableOperator(
    task_id="create_medications_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": raw_data_warehouse,
            "tableId": "medications"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/medications/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_observations_table = BigQueryCreateExternalTableOperator(
    task_id="create_observations_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": raw_data_warehouse,
            "tableId": "observations"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/observations/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_organizations_table = BigQueryCreateExternalTableOperator(
    task_id="create_organizations_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": raw_data_warehouse,
            "tableId": "organizations"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/organizations/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_patients_table = BigQueryCreateExternalTableOperator(
    task_id="create_patients_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": raw_data_warehouse,
            "tableId": "patients"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/patients/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_payer_transitions_table = BigQueryCreateExternalTableOperator(
    task_id="create_payer_transitions_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": raw_data_warehouse,
            "tableId": "payer_transitions"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/payer_transitions/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_payers_table = BigQueryCreateExternalTableOperator(
    task_id="create_payers_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": raw_data_warehouse,
            "tableId": "payers"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/payers/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_procedures_table = BigQueryCreateExternalTableOperator(
    task_id="create_procedures_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": raw_data_warehouse,
            "tableId": "procedures"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/procedures/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_providers_table = BigQueryCreateExternalTableOperator(
    task_id="create_providers_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": raw_data_warehouse,
            "tableId": "providers"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/providers/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_relationship_table = BigQueryCreateExternalTableOperator(
    task_id="create_relationship_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": optimized_data_warehouse,
            "tableId": "relationship"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/RELATIONSHIP/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_supplies_table = BigQueryCreateExternalTableOperator(
    task_id="create_supplies_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": raw_data_warehouse,
            "tableId": "supplies"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/supplies/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_concept_table = BigQueryCreateExternalTableOperator(
    task_id="create_concept_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": optimized_data_warehouse,
            "tableId": "concept"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/CONCEPT/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_vocabulary_table = BigQueryCreateExternalTableOperator(
    task_id="create_vocabulary_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": optimized_data_warehouse,
            "tableId": "vocabulary"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/VOCABULARY/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_concept_ancestor_table = BigQueryCreateExternalTableOperator(
    task_id="create_concept_ancestor_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": optimized_data_warehouse,
            "tableId": "concept_ancestor"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/CONCEPT_ANCESTOR/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_concept_class_table = BigQueryCreateExternalTableOperator(
    task_id="create_concept_class_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": optimized_data_warehouse,
            "tableId": "concept_class"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/CONCEPT_CLASS/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_concept_relationship_table = BigQueryCreateExternalTableOperator(
    task_id="create_concept_relationship_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": optimized_data_warehouse,
            "tableId": "concept_relationship"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/CONCEPT_RELATIONSHIP/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_concept_synonym_table = BigQueryCreateExternalTableOperator(
    task_id="create_concept_synonym_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": optimized_data_warehouse,
            "tableId": "concept_synonym"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/CONCEPT_SYNONYM/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_domain_table = BigQueryCreateExternalTableOperator(
    task_id="create_domain_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": optimized_data_warehouse,
            "tableId": "domain"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/DOMAIN/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

create_drug_strength_table = BigQueryCreateExternalTableOperator(
    task_id="create_drug_strength_table",
    table_resource={
        "tableReference": {
            "projectId": 'bachelor-thesis-344103',
            "datasetId": optimized_data_warehouse,
            "tableId": "drug_strength"
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": ['gs://optimized_data_lake/DRUG_STRENGTH/*.parquet']
        },
    },
    bigquery_conn_id='gcp_credentials',
    dag = dag
)

check_raw_dw_quality = BigQueryCheckOperator(
    task_id = "check_raw_dw_quality",
    sql = "SELECT COUNT(*) FROM raw_data_warehouse.allergies",
    use_legacy_sql=False,
    location='asia-southeast1',
    bigquery_conn_id= 'gcp_credentials',
    dag = dag
)

check_optimized_dw_quality = BigQueryCheckOperator(
    task_id = "check_optimized_dw_quality",
    sql = "SELECT COUNT(*) FROM optimized_data_warehouse.relationship",
    use_legacy_sql=False,
    location='asia-southeast1',
    bigquery_conn_id= 'gcp_credentials',
    dag = dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Define DAG dependencies.
start_operator >> create_raw_datawarehouse
start_operator >> create_optimized_datawarehouse 
create_raw_datawarehouse >> create_allergies_table >> check_raw_dw_quality
create_raw_datawarehouse >> create_careplans_table >> check_raw_dw_quality
create_raw_datawarehouse >> create_conditions_table >> check_raw_dw_quality
create_raw_datawarehouse >> create_devices_table >> check_raw_dw_quality
create_raw_datawarehouse >> create_encounters_table >> check_raw_dw_quality
create_raw_datawarehouse >> create_imaging_studies_table >> check_raw_dw_quality
create_raw_datawarehouse >> create_immunizations_table >> check_raw_dw_quality
create_raw_datawarehouse >> create_medications_table >> check_raw_dw_quality
create_raw_datawarehouse >> create_observations_table >> check_raw_dw_quality
create_raw_datawarehouse >> create_organizations_table >> check_raw_dw_quality
create_raw_datawarehouse >> create_patients_table >> check_raw_dw_quality
create_raw_datawarehouse >> create_payer_transitions_table >> check_raw_dw_quality
create_raw_datawarehouse >> create_payers_table >> check_raw_dw_quality
create_raw_datawarehouse >> create_procedures_table >> check_raw_dw_quality
create_raw_datawarehouse >> create_providers_table >> check_raw_dw_quality
create_raw_datawarehouse >> create_supplies_table >> check_raw_dw_quality
create_optimized_datawarehouse >> create_concept_table >> check_optimized_dw_quality
create_optimized_datawarehouse >> create_concept_ancestor_table >> check_optimized_dw_quality
create_optimized_datawarehouse >> create_concept_class_table >> check_optimized_dw_quality
create_optimized_datawarehouse >> create_concept_relationship_table >> check_optimized_dw_quality
create_optimized_datawarehouse >> create_concept_synonym_table >> check_optimized_dw_quality
create_optimized_datawarehouse >> create_domain_table >> check_optimized_dw_quality
create_optimized_datawarehouse >> create_drug_strength_table >> check_optimized_dw_quality
create_optimized_datawarehouse >> create_relationship_table >> check_optimized_dw_quality
create_optimized_datawarehouse >> create_vocabulary_table  >> check_optimized_dw_quality
check_raw_dw_quality >> end_operator
check_optimized_dw_quality >> end_operator
