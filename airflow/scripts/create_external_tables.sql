CREATE OR REPLACE EXTERNAL TABLE raw_data_warehouse.allergies
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/allergies/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE raw_data_warehouse.careplans
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/careplans/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE raw_data_warehouse.conditions
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/conditions/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE raw_data_warehouse.devices
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/devices/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE raw_data_warehouse.encounters
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/encounters/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE raw_data_warehouse.imaging_studies
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/imaging_studies/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE raw_data_warehouse.immunizations
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/immunizations/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE raw_data_warehouse.medications
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/medications/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE raw_data_warehouse.observations
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/observations/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE raw_data_warehouse.organizations
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/organizations/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE raw_data_warehouse.patients
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/patients/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE raw_data_warehouse.payer_transitions
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/payer_transitions/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE raw_data_warehouse.payers
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/payers/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE raw_data_warehouse.procedures
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/procedures/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE raw_data_warehouse.providers
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/providers/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE raw_data_warehouse.supplies
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/supplies/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE transformed_data_warehouse.concept
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/CONCEPT/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE transformed_data_warehouse.concept_ancestor
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/CONCEPT_ANCESTOR/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE transformed_data_warehouse.concept_class
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/CONCEPT_CLASS/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE transformed_data_warehouse.concept_relationship
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/CONCEPT_RELATIONSHIP/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE transformed_data_warehouse.concept_synonym
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/CONCEPT_SYNONYM/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE transformed_data_warehouse.domain
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/DOMAIN/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE transformed_data_warehouse.drug_strength
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/DRUG_STRENGTH/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE transformed_data_warehouse.relationship
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/RELATIONSHIP/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE transformed_data_warehouse.vocabulary
OPTIONS (
  format = 'Parquet',
  uris = ['gs://optimized_data_lake_2/VOCABULARY/*.parquet']
);
