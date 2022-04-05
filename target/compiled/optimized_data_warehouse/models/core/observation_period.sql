

with p as (
    SELECT
        person_id,
        person_source_value
    FROM
      `bachelor-thesis-344103`.`optimized_data_warehouse`.`person`
),

e as (
    SELECT
      patient,
      CAST(start as TIMESTAMP) as start,
      CAST(stop as TIMESTAMP) as stop,
    FROM
      `bachelor-thesis-344103`.`raw_data_warehouse`.`encounters` e
),

final as (
    SELECT
      p.person_id,
      MIN(e.start) AS start_date,
      MAX(e.stop) AS end_date
    FROM
        p
        INNER JOIN e ON p.person_source_value = e.patient
    GROUP BY
      p.person_id
)

SELECT
  ROW_NUMBER() OVER(
    ORDER BY
      person_id
  ) as observation_period_id,
  person_id as person_id,
  start_date,
  end_date,
  44814724 AS period_type_concept_id
from final