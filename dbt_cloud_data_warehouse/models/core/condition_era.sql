{{ config(materialized='table')}}

WITH ConditionTarget AS(
    SELECT
        co.condition_occurrence_id, 
        co.person_id, 
        co.condition_concept_id, 
        co.condition_start_datetime, 
        COALESCE(co.condition_end_datetime, date_add(condition_start_datetime,INTERVAL 1 DAY)) AS condition_end_datetime
    FROM {{ref('condition_occurrence')}} co
),
EndDates AS (
    SELECT
        person_id, 
        condition_concept_id, 
        date_sub(event_date,INTERVAL 30 DAY) AS end_date
    FROM
    (
        SELECT
            person_id, 
            condition_concept_id, 
            event_date, 
            event_type, 
            MAX(start_ordinal) OVER (PARTITION BY person_id, condition_concept_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal , 
            ROW_NUMBER() OVER (PARTITION BY person_id, condition_concept_id ORDER BY event_date, event_type) AS overall_ord 
        FROM
        (
            SELECT
                person_id, 
                condition_concept_id, 
                condition_start_datetime AS event_date, 
                -1 AS event_type, 
                ROW_NUMBER() OVER (PARTITION BY person_id, condition_concept_id ORDER BY condition_start_datetime) AS start_ordinal
            FROM ConditionTarget
            UNION ALL
            SELECT
                person_id, 
                condition_concept_id, 
                date_add(condition_end_datetime,INTERVAL 30 DAY), 
                1 AS event_type, 
                NULL
            FROM ConditionTarget
        ) RAWDATA
    ) e
    WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
ConditionEnds AS (
    SELECT
        c.person_id, 
        c.condition_concept_id, 
        c.condition_start_datetime, 
        MIN(e.end_date) AS era_end_datetime
    FROM ConditionTarget c
    INNER JOIN EndDates e ON c.person_id = e.person_id AND c.condition_concept_id = e.condition_concept_id AND e.end_date >= c.condition_start_datetime
    GROUP BY
        c.condition_occurrence_id, 
        c.person_id, 
        c.condition_concept_id, 
        c.condition_start_datetime
)
SELECT
    row_number()over(order by person_id) AS condition_era_id, 
    person_id, 
    condition_concept_id, 
    MIN(condition_start_datetime) AS condition_era_start_datetime, 
    era_end_datetime AS condition_era_end_datetime, 
    COUNT(*) AS condition_occurrence_count
FROM ConditionEnds
GROUP BY person_id, condition_concept_id, era_end_datetime
