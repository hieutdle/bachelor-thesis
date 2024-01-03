{{ config(materialized='table')}}

SELECT *, row_number()over(order by patient) as visit_occurrence_id
FROM
(
	SELECT * FROM {{ ref('ER_VISITS') }}
	UNION ALL
	SELECT * FROM {{ ref('IP_VISITS') }}
	UNION ALL
	SELECT * FROM {{ ref('OP_VISITS') }}
) T1