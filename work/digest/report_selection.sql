select  max(validation_id) as v2 , max (q2.report_date) as date_2, v1, date_1, q2.report_region_name
from flk.validations as q2
right join 
	(select max(validation_id) as v1, max(report_date) as date_1, report_region_id
	from flk.validations
	where report_region_id is not null 
	  and report_region_name is not null 
	  and report_errors is null 
	  and report_date >= current_date - 7         /*'2023-11-02'::date -  '7 days'::interval*/
	group by report_region_id)  as q1 
on q1.report_region_id = q2.report_region_id
where q2.validation_id < v1 
 and q2.report_date <> date_1 
 and date_1 - 4  >= q2.report_date  
 and q2.report_date>= date_1 - 10
 and report_errors is null
group by v1, q2.report_region_name, date_1
order by report_region_name

WITH SELECT_FLAG(FLAG_) AS (VALUES(1)), -- значение: 0 - последняя дата, 1 - предпоследняя дата
DATA_SELECT AS (
	SELECT
		MAX(VALIDATION_ID) AS V2,
		MAX (Q2.REPORT_DATE) AS DATE_2,
		V1,
		DATE_1,
		Q2.REPORT_REGION_NAME
	FROM
		FLK.VALIDATIONS AS Q2
		RIGHT JOIN (
			SELECT
				MAX(VALIDATION_ID) AS V1,
				MAX(REPORT_DATE) AS DATE_1,
				REPORT_REGION_ID
			FROM
				FLK.VALIDATIONS
			WHERE
				REPORT_REGION_ID IS NOT NULL
				AND REPORT_REGION_NAME IS NOT NULL
				AND REPORT_ERRORS IS NULL
				AND REPORT_DATE >= CURRENT_DATE - 7
			GROUP BY
				REPORT_REGION_ID
		) AS Q1
		ON Q1.REPORT_REGION_ID = Q2.REPORT_REGION_ID
	WHERE
		Q2.VALIDATION_ID < V1
		AND Q2.REPORT_DATE <> DATE_1
		AND DATE_1 - 4 >= Q2.REPORT_DATE
		AND Q2.REPORT_DATE>= DATE_1 - 10
		AND REPORT_ERRORS IS NULL
	GROUP BY
		V1,
		Q2.REPORT_REGION_NAME,
		DATE_1
	ORDER BY
		REPORT_REGION_NAME
)
SELECT
	CASE
		WHEN FLAG_ = 0 THEN
			V1
		ELSE
			V2
	END AS C_ID,
	CASE
		WHEN FLAG_ = 0 THEN
			DATE_1
		ELSE
			DATE_2
	END AS C_DATE,
	REPORT_REGION_NAME AS C_REG
FROM
	DATA_SELECT,
	SELECT_FLAG;