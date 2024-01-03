{{ config(materialized='table')}}

with tmp as (
    select
        p.person_id,
        coalesce(srctostdvm.target_concept_id,0) AS measurement_concept_id,
        CAST(pr.date AS TIMESTAMP) AS measurement_date,
        CAST(pr.date AS TIMESTAMP) AS measurement_datetime,
        CAST(pr.date AS TIMESTAMP) AS measurement_time,
        5001 AS measurement_type_concept_id,
        0 as operator_concept_id,
        null as value_as_number,
        0 as value_as_concept_id,
        0 as unit_concept_id,
        null as range_low,
        null as range_high,
        0 as provider_id,
        fv.visit_occurrence_id_new AS visit_occurrence_id,
        0 as visit_detail_id,
        pr.code AS measurement_source_value,
        coalesce(srctosrcvm.source_concept_id,0) AS measurement_source_concept_id,
        null as unit_source_value,
        null as value_source_value
        from {{source('raw_data_warehouse','procedures')}} pr
        inner join {{ref('source_to_standard_vocab_map')}}  srctostdvm
        on srctostdvm.source_code             = pr.code
        and srctostdvm.target_domain_id        = 'Measurement'
        and srctostdvm.source_vocabulary_id    = 'SNOMED'
        and srctostdvm.target_standard_concept = 'S'
        and (srctostdvm.target_invalid_reason IS NULL or srctostdvm.target_invalid_reason = '')
        left join {{ref('source_to_source_vocab_map')}} srctosrcvm
        on srctosrcvm.source_code             = pr.code
        and srctosrcvm.source_vocabulary_id    = 'SNOMED'
        left join {{ref('final_visit_ids')}} fv
        on fv.encounter_id = pr.encounter
        join {{ref('person')}} p
        on p.person_source_value = pr.patient
        union all
        select
        p.person_id,
        coalesce(srctostdvm.target_concept_id,0) AS target_concept_id,
        CAST(o.date as TIMESTAMP),
        CAST(o.date as TIMESTAMP),
        CAST(o.date as TIMESTAMP),
        5001,
        0,
        SAFE_CAST(o.value AS FLOAT64) as value_as_number,
        coalesce(srcmap2.target_concept_id,0) AS value_as_concept_id,
        coalesce(srcmap1.target_concept_id,0) AS unit_concept_id,
        null ,
        null ,
        0 as provider_id,
        fv.visit_occurrence_id_new AS visit_occurrence_id,
        0 as visit_detail_id,
        o.code AS measurement_source_value,
        coalesce(srctosrcvm.source_concept_id,0) AS measurement_source_concept_id,
        o.units AS unit_source_value,
        o.value AS value_source_value
        from {{source('raw_data_warehouse','observations')}} o
        inner join {{ref('source_to_standard_vocab_map')}}  srctostdvm
        on srctostdvm.source_code             = o.code
        and srctostdvm.target_domain_id        = 'Measurement'
        and srctostdvm.source_vocabulary_id    = 'LOINC'
        and srctostdvm.target_standard_concept = 'S'
        and (srctostdvm.target_invalid_reason IS NULL OR srctostdvm.target_invalid_reason = '')
        left join {{ref('source_to_standard_vocab_map')}}  srcmap1
        on srcmap1.source_code                = o.units
        and srcmap1.target_vocabulary_id       = 'UCUM'
        and srcmap1.target_standard_concept    = 'S'
        and (srcmap1.target_invalid_reason IS NULL OR srcmap1.target_invalid_reason = '')
        left join {{ref('source_to_standard_vocab_map')}}  srcmap2
        on srcmap2.source_code                = o.value
        and srcmap2.target_domain_id           = 'Meas value'
        and srcmap2.target_standard_concept    = 'S'
        and (srcmap2.target_invalid_reason IS NULL OR srcmap2.target_invalid_reason = '')
        left join {{ref('source_to_source_vocab_map')}} srctosrcvm
        on srctosrcvm.source_code             = o.code
        and srctosrcvm.source_vocabulary_id    = 'LOINC'
        left join {{ref('final_visit_ids')}} fv
        on fv.encounter_id                    = o.encounter
        inner join {{ref('person')}} p
        on p.person_source_value              = o.patient
)
SELECT  row_number()over(order by person_id) AS measurement_id,
        person_id,
        measurement_concept_id,
        measurement_date,
        measurement_datetime,
        measurement_time,
        measurement_type_concept_id,
        operator_concept_id,
        value_as_number,
        value_as_concept_id,
        unit_concept_id,
        range_low,
        range_high,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        measurement_source_value,
        measurement_source_concept_id,
        unit_source_value,
        value_source_value
from tmp