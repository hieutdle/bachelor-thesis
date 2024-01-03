{{ config(materialized='view')}}

SELECT T2.encounter_id,
    T2.patient,
	T2.encounterclass,
	T2.VISIT_START_DATE,
	T2.VISIT_END_DATE
FROM (
	SELECT MIN(encounter_id) encounter_id,
	    patient,
		encounterclass,
		VISIT_START_DATE,
		MAX(VISIT_END_DATE) AS VISIT_END_DATE
	FROM (
		SELECT CL1.id encounter_id,
			CL1.patient,
			CL1.encounterclass,
			CAST(CL1.start AS TIMESTAMP) VISIT_START_DATE,
			CAST(CL2.stop AS TIMESTAMP) VISIT_END_DATE
		FROM {{source('raw_data_warehouse','encounters')}} CL1
		JOIN {{source('raw_data_warehouse','encounters')}} CL2
			ON CL1.patient = CL2.patient
			AND CL1.start = CL2.start
			AND CL1.encounterclass = CL2.encounterclass
		WHERE CL1.encounterclass in ('emergency','urgent')
	) T1
	GROUP BY patient, encounterclass, VISIT_START_DATE
) T2