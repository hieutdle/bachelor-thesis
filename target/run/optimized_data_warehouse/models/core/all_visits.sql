

  create or replace table `bachelor-thesis-344103`.`optimized_data_warehouse`.`all_visits`
  
  
  OPTIONS()
  as (
    

SELECT *, row_number()over(order by patient) as visit_occurrence_id
FROM
(
	SELECT * FROM `bachelor-thesis-344103`.`optimized_data_warehouse`.`ER_VISITS`
	UNION ALL
	SELECT * FROM `bachelor-thesis-344103`.`optimized_data_warehouse`.`IP_VISITS`
	UNION ALL
	SELECT * FROM `bachelor-thesis-344103`.`optimized_data_warehouse`.`OP_VISITS`
) T1
  );
  