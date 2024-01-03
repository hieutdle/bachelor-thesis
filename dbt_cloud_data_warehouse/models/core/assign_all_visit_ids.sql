{{ config(materialized='table')}}

SELECT  E.id AS encounter_id,
		E.patient as person_source_value,
		CAST(E.start AS TIMESTAMP) AS date_service,
		CAST(E.stop AS TIMESTAMP) AS date_service_end,
		E.encounterclass,
		AV.encounterclass AS VISIT_TYPE,
		AV.VISIT_START_DATE,
		AV.VISIT_END_DATE,
		AV.VISIT_OCCURRENCE_ID,
		CASE
			WHEN E.encounterclass = 'inpatient' and AV.encounterclass = 'inpatient'
				THEN VISIT_OCCURRENCE_ID
			WHEN E.encounterclass in ('emergency','urgent')
				THEN (
					CASE
						WHEN AV.encounterclass = 'inpatient' AND CAST(E.start as TIMESTAMP)> AV.VISIT_START_DATE
							THEN VISIT_OCCURRENCE_ID
						WHEN AV.encounterclass in ('emergency','urgent') AND CAST(E.start as TIMESTAMP) = AV.VISIT_START_DATE
							THEN VISIT_OCCURRENCE_ID
						ELSE NULL
					END
				)
			WHEN E.encounterclass in ('ambulatory', 'wellness', 'outpatient')
				THEN (
					CASE
						WHEN AV.encounterclass = 'inpatient' AND CAST(E.start as TIMESTAMP) >= AV.VISIT_START_DATE
							THEN VISIT_OCCURRENCE_ID
						WHEN AV.encounterclass in ('ambulatory', 'wellness', 'outpatient')
							THEN VISIT_OCCURRENCE_ID
						ELSE NULL
					END
				)
			ELSE NULL
		END AS VISIT_OCCURRENCE_ID_NEW
FROM {{source('raw_data_warehouse','encounters')}} e
JOIN {{ ref('all_visits') }} AV
	ON E.patient = AV.patient
	AND CAST(E.start as TIMESTAMP) >= AV.VISIT_START_DATE
	AND CAST(E.start as TIMESTAMP) <= AV.VISIT_END_DATE