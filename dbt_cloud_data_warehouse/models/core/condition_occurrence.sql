{{ config(materialized='table')}}

select
  row_number()over(order by p.person_id) as condition_occurrence_id,
  p.person_id,
  coalesce(srctostdvm.target_concept_id,0) AS condition_concept_id,
  CAST(c.start AS TIMESTAMP) as condition_start_date,
  CAST(c.start AS TIMESTAMP) as condition_start_datetime,
  CAST(c.stop AS TIMESTAMP) as condition_end_date,
  CAST(c.stop AS TIMESTAMP) as condition_end_datetime,
  32020 as condition_type_concept_id,
  0 as condition_status_concept_id,
  null as stop_reason,
  0 as provider_id,
  fv.visit_occurrence_id_new AS visit_occurrence_id,
  0 as visit_detail_id,
  c.code as condition_source_value,
  coalesce(srctosrcvm.source_concept_id,0) as condition_source_concept_id,
  NULL as condition_status_source_value
from {{source('raw_data_warehouse','conditions')}} c
inner join {{ref('source_to_standard_vocab_map')}} srctostdvm
on srctostdvm.source_code             = c.code
 and srctostdvm.target_domain_id        = 'Condition'
 and srctostdvm.source_vocabulary_id    = 'SNOMED'
 and srctostdvm.target_standard_concept = 'S'
 and (srctostdvm.target_invalid_reason IS NULL OR srctostdvm.target_invalid_reason = '')
left join {{ref('source_to_source_vocab_map')}} srctosrcvm
  on srctosrcvm.source_code             = c.code
 and srctosrcvm.source_vocabulary_id    = 'SNOMED'
left join {{ref('final_visit_ids')}} fv
  on fv.encounter_id = c.encounter
inner join {{ref('person')}} p
  on c.patient = p.person_source_value