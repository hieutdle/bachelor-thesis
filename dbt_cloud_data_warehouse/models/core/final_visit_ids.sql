{{ config(materialized='table')}}

SELECT encounter_id, VISIT_OCCURRENCE_ID_NEW
FROM(
	SELECT *, ROW_NUMBER () OVER (PARTITION BY encounter_id ORDER BY PRIORITY) AS RN
	FROM (
		SELECT *,
			CASE
				WHEN encounterclass in ('emergency','urgent')
					THEN (
						CASE
							WHEN VISIT_TYPE = 'inpatient' AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
								THEN 1
							WHEN VISIT_TYPE in ('emergency','urgent') AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
								THEN 2
							ELSE 99
						END
					)
				WHEN encounterclass in ('ambulatory', 'wellness', 'outpatient')
					THEN (
						CASE
							WHEN VISIT_TYPE = 'inpatient' AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
								THEN  1
							WHEN VISIT_TYPE in ('ambulatory', 'wellness', 'outpatient') AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
								THEN 2
							ELSE 99
						END
					)
				WHEN encounterclass = 'inpatient' AND VISIT_TYPE = 'inpatient' AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
					THEN 1
				ELSE 99
			END AS PRIORITY
	FROM {{ ref('assign_all_visit_ids') }}
	) T1
) T2
WHERE RN=1