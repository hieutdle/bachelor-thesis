

  create or replace table `bachelor-thesis-344103`.`optimized_data_warehouse`.`procedure_occurrence`
  
  
  OPTIONS()
  as (
    

SELECT
  row_number()over(order by p.person_id) as procedure_occurrence_id,
  p.person_id,
  coalesce(srctostdvm.target_concept_id, CAST(0 AS STRING)) as procedure_concept_id,
  CAST(pr.date as TIMESTAMP) as procedure_date,
  CAST(pr.date as TIMESTAMP) as procedure_datetime,
  38000275 as procedure_type_concept_id,
  0 as modifier_concept_id,
  null as quantity,
  0 as provider_id,
  fv.visit_occurrence_id_new AS visit_occurrence_id,
  fv.visit_occurrence_id_new + 1000000 as visit_detail_id,
  pr.code as procedure_source_value,
  coalesce(srctosrcvm.source_concept_id,CAST(0 AS STRING)) as procedure_source_concept_id,
  NULL as modifier_source_value
from `bachelor-thesis-344103`.`raw_data_warehouse`.`procedures` pr
inner join `bachelor-thesis-344103`.`optimized_data_warehouse`.`source_to_standard_vocab_map` srctostdvm
  on srctostdvm.source_code             = CAST(pr.code AS STRING)
 and srctostdvm.target_domain_id        = 'Procedure'
 and srctostdvm.source_vocabulary_id    = 'SNOMED'
 and srctostdvm.target_standard_concept = 'S'
 and (srctostdvm.target_invalid_reason IS NULL OR srctostdvm.target_invalid_reason = '')
left join `bachelor-thesis-344103`.`optimized_data_warehouse`.`source_to_source_vocab_map` srctosrcvm
  on srctosrcvm.source_code             = CAST(pr.code AS STRING)
 and srctosrcvm.source_vocabulary_id    = 'SNOMED'
left join `bachelor-thesis-344103`.`optimized_data_warehouse`.`final_visit_ids` fv
  on fv.encounter_id = pr.encounter
inner join  `bachelor-thesis-344103`.`optimized_data_warehouse`.`person` p
  on p.person_source_value    = pr.patient
  );
  