

WITH CTE_END_DATES AS (
	SELECT patient, encounterclass, DATE_SUB(EVENT_DATE, INTERVAL 1 DAY) AS END_DATE
	FROM (
		SELECT patient, encounterclass, EVENT_DATE, EVENT_TYPE,
			MAX(START_ORDINAL) OVER (PARTITION BY patient, encounterclass ORDER BY EVENT_DATE, EVENT_TYPE ROWS UNBOUNDED PRECEDING) AS START_ORDINAL,
			ROW_NUMBER() OVER (PARTITION BY patient, encounterclass ORDER BY EVENT_DATE, EVENT_TYPE) AS OVERALL_ORD
		FROM (
			SELECT patient, encounterclass, CAST(start AS TIMESTAMP) AS EVENT_DATE, -1 AS EVENT_TYPE,
			       ROW_NUMBER () OVER (PARTITION BY patient, encounterclass ORDER BY CAST(start AS TIMESTAMP), CAST(stop AS TIMESTAMP)) AS START_ORDINAL
			FROM `bachelor-thesis-344103`.`raw_data_warehouse`.`encounters`
			WHERE encounterclass = 'inpatient'
			UNION ALL
			SELECT patient, encounterclass, DATE_ADD(CAST(start AS TIMESTAMP), INTERVAL 1 DAY), 1 AS EVENT_TYPE, NULL
			FROM `bachelor-thesis-344103`.`raw_data_warehouse`.`encounters`
			WHERE encounterclass = 'inpatient'
		) RAWDATA
	) E
	WHERE (2 * E.START_ORDINAL - E.OVERALL_ORD = 0)
),
CTE_VISIT_ENDS AS (
	SELECT MIN(V.id) encounter_id,
	    V.patient,
		V.encounterclass,
		CAST(V.start AS TIMESTAMP) VISIT_START_DATE,
		MIN(E.END_DATE) AS VISIT_END_DATE
	FROM `bachelor-thesis-344103`.`raw_data_warehouse`.`encounters` V
		JOIN CTE_END_DATES E
			ON V.patient = E.patient
			AND V.encounterclass = E.encounterclass
			AND E.END_DATE >= CAST(V.start AS TIMESTAMP)
	GROUP BY V.patient,V.encounterclass,CAST(V.start AS TIMESTAMP)
)
SELECT T2.encounter_id,
    T2.patient,
	T2.encounterclass,
	T2.VISIT_START_DATE,
	T2.VISIT_END_DATE
FROM (
	SELECT
	    encounter_id,
	    patient,
		encounterclass,
		MIN(VISIT_START_DATE) AS VISIT_START_DATE,
		VISIT_END_DATE
	FROM CTE_VISIT_ENDS
	GROUP BY encounter_id, patient, encounterclass, VISIT_END_DATE
) T2