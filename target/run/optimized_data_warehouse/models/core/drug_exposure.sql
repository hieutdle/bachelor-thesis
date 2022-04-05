

  create or replace table `bachelor-thesis-344103`.`optimized_data_warehouse`.`drug_exposure`
  
  
  OPTIONS()
  as (
    

with tmp as (
    select
        p.person_id,
        coalesce(srctostdvm.target_concept_id,CAST(0 AS STRING)) as drug_concept_id,
        CAST(c.start AS TIMESTAMP) AS drug_exposure_start_date,
        CAST(c.start AS TIMESTAMP) AS drug_exposure_start_datetime,
        coalesce(CAST(c.stop AS TIMESTAMP),CAST(c.start AS TIMESTAMP)) AS drug_exposure_end_date,
        coalesce(CAST(c.stop AS TIMESTAMP),CAST(c.start AS TIMESTAMP)) AS drug_exposure_end_datetime,
        CAST(c.stop AS TIMESTAMP) AS verbatim_end_date,
        581452 as drug_type_concept_id,
        null as  stop_reason,
        0 as refills,
        0 as quantity,
        coalesce(date_diff(CAST(c.stop AS TIMESTAMP),CAST(c.start AS TIMESTAMP),DAY),0) as days_supply,
        null as  sig,
        0 as route_concept_id,
        0 as lot_number,
        0 as provider_id,
        fv.visit_occurrence_id_new AS visit_occurrence_id,
        0 as visit_detail_id,
        c.code AS drug_source_value,
        coalesce(srctosrcvm.source_concept_id,CAST(0 AS STRING)) AS drug_source_concept_id,
        null as  route_source_value,
        null as  dose_unit_source_value
        from `bachelor-thesis-344103`.`raw_data_warehouse`.`conditions` c
        join `bachelor-thesis-344103`.`optimized_data_warehouse`.`source_to_standard_vocab_map`   srctostdvm
        on srctostdvm.source_code             = CAST(c.code as STRING)
        and srctostdvm.target_domain_id        = 'Drug'
        and srctostdvm.source_vocabulary_id    = 'RxNorm'
        and srctostdvm.target_standard_concept = 'S'
        and (srctostdvm.target_invalid_reason IS NULL OR srctostdvm.target_invalid_reason = '')
        left join `bachelor-thesis-344103`.`optimized_data_warehouse`.`source_to_source_vocab_map` srctosrcvm
        on srctosrcvm.source_code             = CAST(c.code AS STRING)
        and srctosrcvm.source_vocabulary_id    = 'RxNorm'
        left join `bachelor-thesis-344103`.`optimized_data_warehouse`.`final_visit_ids` fv
        on fv.encounter_id = c.encounter
        join `bachelor-thesis-344103`.`optimized_data_warehouse`.`person` p
        on p.person_source_value              = c.patient
        union all
        select
        p.person_id,
        coalesce(srctostdvm.target_concept_id,CAST(0 AS STRING)) as drug_concept_id,
        CAST(m.start AS TIMESTAMP),
        CAST(m.start AS TIMESTAMP),
        coalesce(CAST(m.stop AS TIMESTAMP),CAST(m.start AS TIMESTAMP)),
        coalesce(CAST(m.stop AS TIMESTAMP),CAST(m.start AS TIMESTAMP)),
        CAST(m.stop AS TIMESTAMP),
        38000177,
        null,
        0,
        0,
        coalesce(date_diff(CAST(m.stop AS TIMESTAMP),CAST(m.start AS TIMESTAMP),DAY),0),
        null,
        0,
        0,
        0,
        fv.visit_occurrence_id_new AS visit_occurrence_id,
        0,
        m.code,
        coalesce(srctosrcvm.source_concept_id,CAST(0 AS STRING)),
        null,
        null 
        from `bachelor-thesis-344103`.`raw_data_warehouse`.`medications` m
        join `bachelor-thesis-344103`.`optimized_data_warehouse`.`source_to_standard_vocab_map`   srctostdvm
        on srctostdvm.source_code             = CAST(m.code AS STRING)
        and srctostdvm.target_domain_id        = 'Drug'
        and srctostdvm.source_vocabulary_id    = 'RxNorm'
        and srctostdvm.target_standard_concept = 'S'
        and (srctostdvm.target_invalid_reason IS  NULL OR srctostdvm.target_invalid_reason = '')
        left join `bachelor-thesis-344103`.`optimized_data_warehouse`.`source_to_source_vocab_map` srctosrcvm
        on srctosrcvm.source_code             = CAST(m.code AS STRING)
        and srctosrcvm.source_vocabulary_id    = 'RxNorm'
        left join `bachelor-thesis-344103`.`optimized_data_warehouse`.`final_visit_ids` fv
        on fv.encounter_id = m.encounter
        join `bachelor-thesis-344103`.`optimized_data_warehouse`.`person` p
        on p.person_source_value              = m.patient
        union all
        select
        p.person_id,
        coalesce(srctostdvm.target_concept_id,CAST(0 AS STRING)) as drug_concept_id,
        CAST(i.date as TIMESTAMP),
        CAST(i.date as TIMESTAMP),
        CAST(i.date as TIMESTAMP),
        CAST(i.date as TIMESTAMP),
        CAST(i.date as TIMESTAMP),
        581452,
        null,
        0,
        0,
        0,
        null,
        0,
        0,
        0,
        fv.visit_occurrence_id_new AS visit_occurrence_id,
        0,
        i.code,
        coalesce(srctosrcvm.source_concept_id,CAST(0 AS STRING)),
        null,
        null
        from `bachelor-thesis-344103`.`raw_data_warehouse`.`immunizations` i
        left join `bachelor-thesis-344103`.`optimized_data_warehouse`.`source_to_standard_vocab_map`   srctostdvm
        on srctostdvm.source_code             = CAST(i.code AS STRING)
        and srctostdvm.target_domain_id        = 'Drug'
        and srctostdvm.source_vocabulary_id    = 'CVX'
        and srctostdvm.target_standard_concept = 'S'
        and (srctostdvm.target_invalid_reason IS NULL OR srctostdvm.target_invalid_reason = '')
        left join `bachelor-thesis-344103`.`optimized_data_warehouse`.`source_to_source_vocab_map` srctosrcvm
        on srctosrcvm.source_code             = CAST(i.code AS STRING)
        and srctosrcvm.source_vocabulary_id    = 'CVX'
        left join `bachelor-thesis-344103`.`optimized_data_warehouse`.`final_visit_ids` fv
        on fv.encounter_id = i.encounter
        join `bachelor-thesis-344103`.`optimized_data_warehouse`.`person` p
        on p.person_source_value              = i.patient
)
SELECT  row_number()over(order by person_id) AS drug_exposure_id,
        person_id,
        drug_concept_id,
        drug_exposure_start_date,
        drug_exposure_start_datetime,
        drug_exposure_end_date,
        drug_exposure_end_datetime,
        verbatim_end_date,
        drug_type_concept_id,
        stop_reason,
        refills,
        quantity,
        days_supply,
        sig,
        route_concept_id,
        lot_number,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        drug_source_value,
        drug_source_concept_id,
        route_source_value,
        dose_unit_source_value
from tmp
  );
  