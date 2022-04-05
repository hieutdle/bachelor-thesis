

with vo_cte as (
    select *
    from `bachelor-thesis-344103`.`optimized_data_warehouse`.`all_visits` av
    join `bachelor-thesis-344103`.`optimized_data_warehouse`.`person` p on av.patient = p.person_source_value
    where av.visit_occurrence_id in (
        select distinct visit_occurrence_id_new
        from
        `bachelor-thesis-344103`.`optimized_data_warehouse`.`final_visit_ids`
    )
)
select
  visit_occurrence_id,
  person_id,
  case
    lower(encounterclass)
    when 'ambulatory' then 9202
    when 'emergency' then 9203
    when 'inpatient' then 9201
    when 'wellness' then 9202
    when 'urgentcare' then 9203
    when 'outpatient' then 9202
    else 0
  end as visit_concept_id,
  visit_start_date,
  visit_start_date as visit_start_datetime,
  visit_end_date,
  visit_end_date as visit_end_datetime,
  44818517 as visit_type_concept_id,
  0 as provider_id,
  null as care_site_id,
  encounter_id as visit_source_value,
  0 as visit_source_concept_id,
  0 as admitted_from_concept_id,
  NULL as admitted_from_source_value,
  0 as discharged_to_concept_id,
  NULL as discharged_to_source_value,
  lag(visit_occurrence_id) over(
    partition by person_id
    order by
      visit_start_date
  ) as preceding_visit_occurrence_id	
from vo_cte