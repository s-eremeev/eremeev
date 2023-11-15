-- эксперименты, РАБОЧИЙ ВАРИАНТ
WITH SELECT_FLAG(FLAG_) AS (VALUES(0)), -- выбор расчётной даты для каждого региона: 0 - последняя дата, 1 - предпоследняя дата
BORDER_DAYS(I, DAYS) AS (VALUES (0, 7), (1, 4), (2, 10)), -- "отступы": max дней от даты дайджеста до даты последнего отчёта (7), границы интервала для даты предпоследнего отчёта (от 4 до 10)
T_MAIN AS
 -- Отбор данных за 14 дней
(
	SELECT
		C_ID,
		C_REG,
		C_DATE,
		MO_OID,
		MO_SHORT_NAME,
		SP_OID,
		SP_NAME,
		MP_DOLGNOST,
		MP_FIO,
		C_DATE_REP,
		SLOTS_TYPE,
		C_SLOTS_C,
		C_SLOTS_B,
		C_STAVKA,
		SLOT_LENGTH,
		VISITS_ABSENCE
	FROM
 -- Отбор из основной таблицы
		(
			SELECT
				C_ID,
				C_REG,
				C_DATE,
				MO_OID,
				MO_SHORT_NAME,
				SP_OID,
				SP_NAME,
				MP_DOLGNOST,
				MP_FIO,
				SLOTS_TYPE,
				CAST(SLOTS_CREATED AS DECIMAL(10,
				0)) AS C_SLOTS_C,
				CAST(SLOTS_BOOKED AS DECIMAL(10,
				0)) AS C_SLOTS_B,
				CASE
					WHEN LEFT(DATE_REPORT, 10) ~ '^[0-3][0-9].[0-1][0-9].[2][0][2][3-9]$' THEN
						TO_DATE(DATE_REPORT, 'dd.mm.yyyy')
					WHEN LEFT(DATE_REPORT, 10) ~ '^[2][0][2][3-9]-[0-1][0-9]-[0-3][0-9]$' THEN
						TO_DATE(DATE_REPORT, 'yyyy-mm-dd')
					ELSE
						TO_DATE('1900-01-01', 'yyyy-mm-dd')
				END AS C_DATE_REP,
				CASE
					WHEN REPLACE(MP_STAVKA, ',', '.') ~ '^[0-9]+.[0-9]+$' THEN
						CAST(REPLACE(MP_STAVKA, ',', '.') AS DECIMAL(10, 3))
					ELSE
						0
				END AS C_STAVKA,
				CASE
					WHEN REPORT_ROWS.SLOT_LENGTH ~ '^[0-9]+.[0-9]+$' THEN
						CAST(REPORT_ROWS.SLOT_LENGTH AS DECIMAL(10, 0))
					ELSE
						0
				END AS SLOT_LENGTH, -- поля, которых у Морева нет
				CASE
					WHEN REPORT_ROWS.VISITS_ABSENCE ~ '^[0-9]+$' THEN
						CAST(REPORT_ROWS.VISITS_ABSENCE AS INT)
					ELSE
						0
				END AS VISITS_ABSENCE
			FROM
				FLK.REPORT_ROWS
				RIGHT JOIN -- фильтрует основную таблицу по отобранным ниже ID
				(
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
						(
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
										FLK.VALIDATIONS,
										BORDER_DAYS
									WHERE
										REPORT_REGION_ID IS NOT NULL
										AND REPORT_REGION_NAME IS NOT NULL
										AND REPORT_ERRORS IS NULL
										AND REPORT_DATE >= CURRENT_DATE - (
											SELECT
												DAYS
											FROM
												BORDER_DAYS
											WHERE
												I = 0
										)
									GROUP BY
										REPORT_REGION_ID
								) AS Q1
								ON Q1.REPORT_REGION_ID = Q2.REPORT_REGION_ID
							WHERE
								Q2.VALIDATION_ID < V1
								AND Q2.REPORT_DATE <> DATE_1
								AND DATE_1 - (
									SELECT
										DAYS
									FROM
										BORDER_DAYS
									WHERE
										I = 1
								) >= Q2.REPORT_DATE
								AND Q2.REPORT_DATE>= DATE_1 - (
									SELECT
										DAYS
									FROM
										BORDER_DAYS
									WHERE
										I = 2
								)
								AND REPORT_ERRORS IS NULL
							GROUP BY
								V1,
								Q2.REPORT_REGION_NAME,
								DATE_1
							ORDER BY
								REPORT_REGION_NAME
						)AS ACTUAL_DATA,
						SELECT_FLAG,
						BORDER_DAYS
				) AS T_ID_REG
				ON T_ID_REG.C_ID = FLK.REPORT_ROWS.VALIDATION_ID
			WHERE
				SP_DEPART_TYPE_NAME = 'Амбулаторный'
				AND MO_DEPT_NAME = 'Органы исполнительной власти субъектов Российской Федерации, осуществляющие функции в области здравоохранения'
				AND (LEFT(DATE_REPORT, 10) ~ '^[2][0][2][3-9]-[0-1][0-9]-[0-3][0-9]$'
				OR LEFT(DATE_REPORT, 10) ~ '^[0-3][0-9].[0-1][0-9].[2][0][2][3-9]$')
				AND SLOTS_CREATED ~ '^[0-9]+$'
				AND SLOTS_BOOKED ~ '^[0-9]+$'
		) AS T_MAIN_TMP
	WHERE
		C_DATE_REP <= C_DATE
		AND C_DATE_REP > C_DATE-14
)
 -- Собственно формирование выгрузки из ГЛАВНОЙ ТАБЛИЦЫ с приджойниванием столбцов
SELECT
	C_ID,
	C_DATE,
	C_REG_0,
	MO_OID_0,
	MO_SHORT_NAME_0,
	SP_OID_0,
	SP_NAME_0,
	MP_DOLGNOST_0,
	C_SLOTS_ALL,
	C_SLOTS_BOOKED,
	C_KONK_SLOTS_ALL,
	C_KONK_SLOTS_BOOKED,
	C_NEKONK_SLOTS_ALL,
	C_NEKONK_SLOTS_BOOKED,
	C_STAVKA_SUM,
	C_STAVKA_DAYS_SUM
FROM
	(
		SELECT
			C_ID,
			C_DATE,
			C_REG AS C_REG_0,
			MO_OID AS MO_OID_0,
			MO_SHORT_NAME AS MO_SHORT_NAME_0,
			SP_OID AS SP_OID_0,
			SP_NAME AS SP_NAME_0,
			MP_DOLGNOST AS MP_DOLGNOST_0,
			SUM(C_SLOTS_C) AS C_SLOTS_ALL,
			SUM(C_SLOTS_B) AS C_SLOTS_BOOKED
		FROM
			T_MAIN
		GROUP BY
			C_ID,
			C_REG,
			C_DATE,
			MO_OID,
			MO_SHORT_NAME,
			SP_OID,
			SP_NAME,
			MP_DOLGNOST
	) AS T_SLOTS
 -- Приджойниваем конкурентные слоты всего и занятые
	FULL JOIN (
		SELECT
			C_REG AS C_REG_1,
			MO_OID AS MO_OID_1,
			MO_SHORT_NAME AS MO_SHORT_NAME_1,
			SP_OID AS SP_OID_1,
			SP_NAME AS SP_NAME_1,
			MP_DOLGNOST AS MP_DOLGNOST_1,
			SUM(C_SLOTS_C) AS C_KONK_SLOTS_ALL,
			SUM(C_SLOTS_B) AS C_KONK_SLOTS_BOOKED
		FROM
			T_MAIN
		WHERE
			LEFT(SLOTS_TYPE, 8) = 'Доступен'
		GROUP BY
			C_REG,
			MO_OID,
			MO_SHORT_NAME,
			SP_OID,
			SP_NAME,
			MP_DOLGNOST
	) AS T_KONK_SLOTS
	ON T_SLOTS.C_REG_0 = T_KONK_SLOTS.C_REG_1
	AND T_SLOTS.MO_OID_0 = T_KONK_SLOTS.MO_OID_1
	AND T_SLOTS.MO_SHORT_NAME_0 = T_KONK_SLOTS.MO_SHORT_NAME_1
	AND T_SLOTS.SP_OID_0 = T_KONK_SLOTS.SP_OID_1
	AND T_SLOTS.SP_NAME_0 = T_KONK_SLOTS.SP_NAME_1
	AND T_SLOTS.MP_DOLGNOST_0 = T_KONK_SLOTS.MP_DOLGNOST_1
 -- И также неконкурентные
	FULL JOIN (
		SELECT
			C_REG AS C_REG_2,
			MO_OID AS MO_OID_2,
			MO_SHORT_NAME AS MO_SHORT_NAME_2,
			SP_OID AS SP_OID_2,
			SP_NAME AS SP_NAME_2,
			MP_DOLGNOST AS MP_DOLGNOST_2,
			SUM(C_SLOTS_C) AS C_NEKONK_SLOTS_ALL,
			SUM(C_SLOTS_B) AS C_NEKONK_SLOTS_BOOKED
		FROM
			T_MAIN
		WHERE
			LEFT(SLOTS_TYPE, 10) = 'Недоступен'
			OR LEFT(SLOTS_TYPE, 11) = 'Не доступен'
		GROUP BY
			C_REG,
			MO_OID,
			MO_SHORT_NAME,
			SP_OID,
			SP_NAME,
			MP_DOLGNOST
	) AS T_NEKONK_SLOTS
	ON T_SLOTS.C_REG_0 = T_NEKONK_SLOTS.C_REG_2
	AND T_SLOTS.MO_OID_0 = T_NEKONK_SLOTS.MO_OID_2
	AND T_SLOTS.MO_SHORT_NAME_0 = T_NEKONK_SLOTS.MO_SHORT_NAME_2
	AND T_SLOTS.SP_OID_0 = T_NEKONK_SLOTS.SP_OID_2
	AND T_SLOTS.SP_NAME_0 = T_NEKONK_SLOTS.SP_NAME_2
	AND T_SLOTS.MP_DOLGNOST_0 = T_NEKONK_SLOTS.MP_DOLGNOST_2
 -- Наконец, отбираем уникальных врачей с уникальной ставкой, выводим по ним ставку и считаем ставкодни
	FULL JOIN (
		SELECT
			C_REG AS C_REG_3,
			MO_OID AS MO_OID_3,
			MO_SHORT_NAME AS MO_SHORT_NAME_3,
			SP_OID AS SP_OID_3,
			SP_NAME AS SP_NAME_3,
			MP_DOLGNOST AS MP_DOLGNOST_3,
			SUM(C_STAVKA) AS C_STAVKA_SUM,
			SUM(C_STAVKA_DAYS) AS C_STAVKA_DAYS_SUM
		FROM
			(
				SELECT
					C_REG,
					MO_OID,
					MO_SHORT_NAME,
					SP_OID,
					SP_NAME,
					MP_DOLGNOST,
					MP_FIO,
					C_STAVKA,
					C_STAVKA * C_DATE_REP_COUNT AS C_STAVKA_DAYS
				FROM
					(
						SELECT
							C_REG,
							MO_OID,
							MO_SHORT_NAME,
							SP_OID,
							SP_NAME,
							MP_DOLGNOST,
							MP_FIO,
							C_STAVKA,
							COUNT(C_DATE_REP) AS C_DATE_REP_COUNT
						FROM
							(
								SELECT
									DISTINCT C_REG,
									MO_OID,
									MO_SHORT_NAME,
									SP_OID,
									SP_NAME,
									MP_DOLGNOST,
									MP_FIO,
									C_DATE_REP,
									C_STAVKA
								FROM
									T_MAIN
							) AS T_UNIC_1
						GROUP BY
							C_REG,
							MO_OID,
							MO_SHORT_NAME,
							SP_OID,
							SP_NAME,
							MP_DOLGNOST,
							MP_FIO,
							C_STAVKA
					) AS T_UNIC_2
			) AS T_UNIC_3
		GROUP BY
			C_REG,
			MO_OID,
			MO_SHORT_NAME,
			SP_OID,
			SP_NAME,
			MP_DOLGNOST
	) AS T_SD
	ON T_SLOTS.C_REG_0 = T_SD.C_REG_3
	AND T_SLOTS.MO_OID_0 = T_SD.MO_OID_3
	AND T_SLOTS.MO_SHORT_NAME_0 = T_SD.MO_SHORT_NAME_3
	AND T_SLOTS.SP_OID_0 = T_SD.SP_OID_3
	AND T_SLOTS.SP_NAME_0 = T_SD.SP_NAME_3
	AND T_SLOTS.MP_DOLGNOST_0 = T_SD.MP_DOLGNOST_3;

-- А.Шатров + Я
WITH SELECT_FLAG(FLAG_) AS (VALUES(0)), -- значение: 0 - последняя дата, 1 - предпоследняя дата
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