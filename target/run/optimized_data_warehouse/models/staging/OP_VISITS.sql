

  create or replace view `bachelor-thesis-344103`.`optimized_data_warehouse`.`OP_VISITS`
  OPTIONS()
  as 

WITH CTE_VISITS_DISTINCT AS (
	SELECT MIN(id) encounter_id,
	               patient,
				   encounterclass,
					CAST(start AS TIMESTAMP) VISIT_START_DATE,
					CAST(stop AS TIMESTAMP) VISIT_END_DATE
	FROM `bachelor-thesis-344103`.`raw_data_warehouse`.`encounters`
	WHERE encounterclass in ('ambulatory', 'wellness', 'outpatient')
	GROUP BY patient,encounterclass,start,stop
)
SELECT MIN(encounter_id) encounter_id,
       patient,
		encounterclass,
		VISIT_START_DATE,
		MAX(VISIT_END_DATE) AS VISIT_END_DATE
FROM CTE_VISITS_DISTINCT
GROUP BY patient, encounterclass, VISIT_START_DATE;

