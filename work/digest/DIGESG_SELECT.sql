﻿WITH
-- ВЫБОР ПЕРИОДА ВЫГРУЗКИ: 0 - ОТЧЁТНЫЙ, 1 - КОНТРОЛЬНЫЙ
SELECT_FLAG(FLAG_) AS (VALUES(0)),
-- ГРАНИЦЫ ПЕРИОДОВ: КОНТРОЛЬНЫЙ, ОТЧЁТНЫЙ
BORDERS(I, D) AS (VALUES (0, '2024-02-12'), (1, '2024-02-18'), (2, '2024-02-19'), (3, '2024-02-25')),
-- КОЛИЧЕСТВО ('НЕ МЕНЕЕ') ДНЕЙ С РАСПИСАНИЕМ В ЗАКРЫТОМ ПЕРИОДЕ (0), МАКСИМАЛЬНЫЙ ВРЕМЕННОЙ ЛАГ ('НЕ БОЛЕЕ') МЕЖДУ ДАТОЙ ОТЧЁТА И ПЕРВЫМ ДНЁМ С РАСПИСАНИЕМ (1)
LIMITS(I, VAL) AS (VALUES (0, 1), (1, 15)),

T_BIG AS(
SELECT
V.VALIDATION_ID,
V.REPORT_REGION_NAME,
V.REPORT_DATE,
CASE
	WHEN LEFT(RR.DATE_REPORT, 10) ~ '^[0-3][0-9].[0-1][0-9].[2][0][2][3-9]$' THEN TO_DATE(RR.DATE_REPORT, 'dd.mm.yyyy')
	WHEN LEFT(RR.DATE_REPORT, 10) ~ '^[2][0][2][3-9]-[0-1][0-9]-[0-3][0-9]$' THEN TO_DATE(RR.DATE_REPORT, 'yyyy-mm-dd')
	ELSE TO_DATE('1900-01-01', 'yyyy-mm-dd')
END AS DATE_SLOT
FROM FLK.REPORT_ROWS RR
INNER JOIN
FLK.VALIDATIONS V
ON RR.VALIDATION_ID = V.VALIDATION_ID
WHERE
V.REPORT_ERRORS IS NULL
AND V.REPORT_REGION_NAME IS NOT NULL
AND RR.MO_DEPT_NAME = 'Органы исполнительной власти субъектов Российской Федерации, осуществляющие функции в области здравоохранения'
AND RR.SP_DEPART_TYPE_NAME = 'Амбулаторный'
AND V.REPORT_DATE BETWEEN CAST((SELECT D FROM BORDERS WHERE I = 0) AS DATE) AND CAST((SELECT D FROM BORDERS WHERE I = 3) AS DATE)
),

T_CNT_LIMIT AS(
SELECT
T_C.VALIDATION_ID,
T_C.REPORT_REGION_NAME,
T_C.REPORT_DATE,
T_C.DATE_SLOT,
T_C.SLOTS_DAYS_COUNT,
T_C.MAX_SLOT_LAG
FROM
	(SELECT
	TB_1.VALIDATION_ID,
	TB_1.REPORT_REGION_NAME,
	TB_1.REPORT_DATE,
	TB_1.DATE_SLOT,
	MAX(TB_1.SLOTS_DAYS_RANK) OVER(PARTITION BY TB_1.VALIDATION_ID) AS SLOTS_DAYS_COUNT,
	TB_1.REPORT_DATE - TB_1.MIN_DATE_SLOT AS MAX_SLOT_LAG
	FROM
		(SELECT
		TB.VALIDATION_ID,
		TB.REPORT_REGION_NAME,
		TB.REPORT_DATE,
		TB.DATE_SLOT,
		DENSE_RANK() OVER(PARTITION BY TB.VALIDATION_ID ORDER BY TB.DATE_SLOT) AS SLOTS_DAYS_RANK,
		MIN(TB.DATE_SLOT) OVER(PARTITION BY TB.VALIDATION_ID) AS MIN_DATE_SLOT
		FROM T_BIG TB WHERE TB.DATE_SLOT < TB.REPORT_DATE AND TB.DATE_SLOT <> '1900-01-01') AS TB_1) AS T_C
WHERE
T_C.SLOTS_DAYS_COUNT >= (SELECT VAL FROM LIMITS WHERE I = 0)
AND T_C.MAX_SLOT_LAG <= (SELECT VAL FROM LIMITS WHERE I = 1)
),

T_SELECT_ID AS(
SELECT
T_S.REPORT_REGION_NAME AS C_REG,
CASE
WHEN FLAG_ = 0 THEN T_S.MAX_ID_0
ELSE T_S.MAX_ID_1
END AS C_ID,
CASE
WHEN FLAG_ = 0 THEN T_S.MAX_DATE_0
ELSE T_S.MAX_DATE_1
END AS C_DATE
FROM
	(SELECT DISTINCT
	T_CNT_LIMIT.REPORT_REGION_NAME,
	MAX(T_CNT_LIMIT.REPORT_DATE) OVER (PARTITION BY T_CNT_LIMIT.REPORT_REGION_NAME) MAX_DATE_0,
	MAX(T_CNT_LIMIT.VALIDATION_ID) OVER (PARTITION BY T_CNT_LIMIT.REPORT_REGION_NAME) MAX_ID_0,
	T_CNT_LIMIT_1.MAX_DATE_1,
	T_CNT_LIMIT_1.MAX_ID_1
	FROM T_CNT_LIMIT
	INNER JOIN
	(SELECT
	 DISTINCT
	 T_CNT_LIMIT.REPORT_REGION_NAME,
	 MAX(T_CNT_LIMIT.REPORT_DATE) OVER (PARTITION BY T_CNT_LIMIT.REPORT_REGION_NAME) MAX_DATE_1,
	 MAX(T_CNT_LIMIT.VALIDATION_ID) OVER (PARTITION BY T_CNT_LIMIT.REPORT_REGION_NAME) MAX_ID_1
	 FROM T_CNT_LIMIT
	 WHERE T_CNT_LIMIT.REPORT_DATE BETWEEN CAST((SELECT D FROM BORDERS WHERE I = 0) AS DATE) AND CAST((SELECT D FROM BORDERS WHERE I = 1) AS DATE)) AS T_CNT_LIMIT_1
	ON T_CNT_LIMIT.REPORT_REGION_NAME = T_CNT_LIMIT_1.REPORT_REGION_NAME
	WHERE T_CNT_LIMIT.REPORT_DATE BETWEEN CAST((SELECT D FROM BORDERS WHERE I = 2) AS DATE) AND CAST((SELECT D FROM BORDERS WHERE I = 3) AS DATE)) AS T_S, SELECT_FLAG
)

SELECT DISTINCT
TCL.VALIDATION_ID,
TCL.REPORT_REGION_NAME,
TCL.REPORT_DATE,
TCL.MAX_SLOT_LAG,
TCL.SLOTS_DAYS_COUNT
FROM
T_CNT_LIMIT TCL
RIGHT JOIN
T_SELECT_ID TS
ON
TCL.VALIDATION_ID = TS.C_ID
AND TCL.REPORT_REGION_NAME = TS.C_REG
AND TCL.REPORT_DATE = TS.C_DATE;