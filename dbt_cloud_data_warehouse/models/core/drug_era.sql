{{ config(materialized='table')}}

WITH PreDrugTarget AS(
    SELECT
        d.drug_exposure_id, 
        d.person_id, 
        c.concept_id AS ingredient_concept_id, 
        d.drug_exposure_start_datetime AS drug_exposure_start_datetime, 
        d.days_supply AS days_supply, 
        COALESCE(
            IFNULL(drug_exposure_end_datetime, NULL),
            IFNULL(date_add(drug_exposure_start_datetime,INTERVAL cast(days_supply as int) DAY), drug_exposure_start_datetime),
            date_add(drug_exposure_start_datetime,INTERVAL 1 DAY)
        ) AS drug_exposure_end_datetime
    FROM {{ref('drug_exposure')}} d
        INNER JOIN {{source('transformed_data_warehouse','concept_ancestor')}} ca ON ca.descendant_concept_id = d.drug_concept_id
        INNER JOIN {{source('transformed_data_warehouse','concept')}} c ON ca.ancestor_concept_id = c.concept_id
        WHERE c.vocabulary_id = 'RxNorm' 
        AND c.concept_class_id = 'Ingredient'
        AND d.drug_concept_id != 0 
        AND coalesce(d.days_supply,0) >= 0
), 
SubExposureEndDates AS (
    SELECT person_id, ingredient_concept_id, event_date AS end_datetime
    FROM
    (
        SELECT person_id, ingredient_concept_id, event_date, event_type,
        MAX(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type ROWS unbounded preceding) AS start_ordinal,
        ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type) AS overall_ord
        FROM (
            SELECT person_id, ingredient_concept_id, drug_exposure_start_datetime AS event_date,
            -1 AS event_type,
            ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY drug_exposure_start_datetime) AS start_ordinal
            FROM PreDrugTarget
            UNION ALL
            SELECT person_id, ingredient_concept_id, drug_exposure_end_datetime, 1 AS event_type, NULL
            FROM PreDrugTarget
        ) RAWDATA
    ) e
    WHERE (2 * e.start_ordinal) - e.overall_ord = 0
), 
DrugExposureEnds AS(
    SELECT
        dt.person_id, 
        dt.ingredient_concept_id as drug_concept_id, 
        dt.drug_exposure_start_datetime, 
        MIN(e.end_datetime) AS drug_sub_exposure_end_datetime
    FROM PreDrugTarget dt
    INNER JOIN SubExposureEndDates e ON dt.person_id = e.person_id AND dt.ingredient_concept_id = e.ingredient_concept_id AND e.end_datetime >= dt.drug_exposure_start_datetime
    GROUP BY
            dt.drug_exposure_id, 
            dt.person_id, 
            dt.ingredient_concept_id, 
            dt.drug_exposure_start_datetime
), 
SubExposures AS (
    SELECT ROW_NUMBER() OVER (PARTITION BY person_id, drug_concept_id, drug_sub_exposure_end_datetime ORDER BY person_id) as row_number, 
    person_id, drug_concept_id, MIN(drug_exposure_start_datetime) AS drug_sub_exposure_start_datetime, drug_sub_exposure_end_datetime, COUNT(*) AS drug_exposure_count
    FROM DrugExposureEnds
    GROUP BY person_id, drug_concept_id, drug_sub_exposure_end_datetime
    ORDER BY person_id, drug_concept_id
), 
FinalTarget AS(
    SELECT row_number, person_id, drug_concept_id as ingredient_concept_id, drug_sub_exposure_start_datetime, drug_sub_exposure_end_datetime, drug_exposure_count, 
    date_diff(drug_sub_exposure_end_datetime,drug_sub_exposure_start_datetime,day) AS days_exposed
    FROM SubExposures
), 
EndDates AS (
    SELECT person_id, ingredient_concept_id, date_sub(event_date,INTERVAL 30 DAY) AS end_datetime
    FROM
    (
        SELECT person_id, ingredient_concept_id, event_date, event_type,
        MAX(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal,
        ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type) AS overall_ord
        FROM (
            SELECT person_id, ingredient_concept_id, drug_sub_exposure_start_datetime AS event_date,
            -1 AS event_type,
            ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY drug_sub_exposure_start_datetime) AS start_ordinal
            FROM FinalTarget
            UNION ALL
            SELECT person_id, ingredient_concept_id, date_add(drug_sub_exposure_end_datetime,INTERVAL 30 DAY), 1 AS event_type, NULL
            FROM FinalTarget
        ) RAWDATA
    ) e
    WHERE (2 * e.start_ordinal) - e.overall_ord = 0
 
), 
DrugEraEnds AS
(
    SELECT
        ft.person_id, 
        ft.ingredient_concept_id, 
        ft.drug_sub_exposure_start_datetime, 
        MIN(e.end_datetime) AS drug_era_end_datetime, 
        drug_exposure_count, 
        days_exposed
    FROM FinalTarget ft
    JOIN EndDates e ON ft.person_id = e.person_id AND ft.ingredient_concept_id = e.ingredient_concept_id AND e.end_datetime >= ft.drug_sub_exposure_start_datetime
    GROUP BY
            ft.person_id, 
            ft.ingredient_concept_id, 
            ft.drug_sub_exposure_start_datetime, 
            drug_exposure_count, 
            days_exposed
)
SELECT
    row_number()over(order by person_id) AS drug_era_id, 
    person_id, 
    ingredient_concept_id as drug_concept_id, 
    CAST(MIN(drug_sub_exposure_start_datetime) AS DATE) AS drug_era_start_date, 
    CAST(drug_era_end_datetime AS DATE) as drug_era_end_date, 
    SUM(drug_exposure_count) AS drug_exposure_count,
    date_diff(date_add(drug_era_end_datetime,INTERVAL cast(-(date_diff(drug_era_end_datetime,MIN(drug_sub_exposure_start_datetime),DAY)-SUM(days_exposed)) as int) DAY),TIMESTAMP '1970-01-01',DAY) as gap_days
FROM DrugEraEnds dee
GROUP BY person_id, drug_concept_id, drug_era_end_datetime