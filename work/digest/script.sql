WITH
 -- настройки:
 -- выбор расчётной даты для каждого региона: 0 - последняя дата, 1 - предпоследняя дата:
SELECT_FLAG(FLAG_) AS (VALUES(1)),
 -- выбор даты дайджеста (выгрузки):
DIGEST_DATE(DATE_DIGEST) AS (VALUES('2023-12-21')),
 -- "отступы": max дней от даты дайджеста до даты последнего отчёта (7), границы интервала для даты предпоследнего отчёта (от 4 до 10) и 14 дней:
BORDER_DAYS(I, DAYS) AS (VALUES (0, 7), (1, 4), (2, 10), (3, 14)),
 -- продолжительность "короткого" слота, мин:
MIN_SLOT_LENGTH(MM) AS (VALUES(5)), T_MAIN AS
 -- Отбор данных за 14 дней
(
	SELECT
		DISTINCT C_ID,
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
				CASE
					WHEN SLOTS_CREATED ~ '[0-9]+$' THEN
						CAST(SLOTS_CREATED AS DECIMAL(10, 0))
					WHEN REPLACE(SLOTS_CREATED, ',', '.') ~ '[0-9]+.[0]+$' THEN
						CAST(REPLACE(SLOTS_CREATED, ',', '.') AS DECIMAL(10, 0))
					ELSE
						0
				END AS C_SLOTS_C,
				CASE
					WHEN SLOTS_BOOKED ~ '^[0-9]+$' THEN
						CAST(SLOTS_BOOKED AS DECIMAL(10, 0))
					WHEN REPLACE(SLOTS_BOOKED, ',', '.') ~ '[0-9]+.[0]+$' THEN
						CAST(REPLACE(SLOTS_BOOKED, ',', '.') AS DECIMAL(10, 0))
					ELSE
						0
				END AS C_SLOTS_B,
				CASE
					WHEN LEFT(DATE_REPORT, 10) ~ '^[0-3][0-9].[0-1][0-9].[2][0][2][3-9]$' THEN
						TO_DATE(DATE_REPORT, 'dd.mm.yyyy')
					WHEN LEFT(DATE_REPORT, 10) ~ '^[2][0][2][3-9]-[0-1][0-9]-[0-3][0-9]$' THEN
						TO_DATE(DATE_REPORT, 'yyyy-mm-dd')
					ELSE
						TO_DATE('1900-01-01', 'yyyy-mm-dd')
				END AS C_DATE_REP,
				CASE
					WHEN REPLACE(MP_STAVKA, ',', '') ~ '^[0]+$' THEN
						1.000
					WHEN REPLACE(MP_STAVKA, '.', '') ~ '^[0]+$' THEN
						1.000
					WHEN REPLACE(MP_STAVKA, ',', '.') ~ '^[0-9]+.[0-9]+$' THEN
						CAST(REPLACE(MP_STAVKA, ',', '.') AS DECIMAL(10, 3))
					WHEN REPLACE(MP_STAVKA, ',', '.') ~ '^.[0-9]+$' THEN
						CAST(REPLACE(CONCAT('0', MP_STAVKA), ',', '.') AS DECIMAL(10, 3))
					WHEN MP_STAVKA ~ '^[1-9][0-9]+$' THEN
						CAST(MP_STAVKA AS DECIMAL(10, 3))
					WHEN MP_STAVKA ~ '^[0]+$' THEN
						1.000
					WHEN MP_STAVKA IS NULL THEN
						1.000
					WHEN MP_STAVKA = '' THEN
						1.000
					ELSE
						1.000
				END AS C_STAVKA,
				CASE
					WHEN REPLACE (SLOT_LENGTH, ',', '.') ~ '^[0-9]+.[0-9]+$' THEN
						CAST(REPLACE (SLOT_LENGTH, ',', '.') AS DECIMAL(10, 0))
					WHEN SLOT_LENGTH ~ '^[0-9]+$' THEN
						CAST(SLOT_LENGTH AS DECIMAL(10, 0))
					ELSE
						0
				END AS SLOT_LENGTH,
				CASE
					WHEN VISITS_ABSENCE ~ '^[0-9]+$' THEN
						CAST(VISITS_ABSENCE AS DECIMAL(10, 0))
					WHEN REPLACE(VISITS_ABSENCE, ',', '.') ~ '^[0-9]+.[0-9]+$' THEN
						CAST(REPLACE(VISITS_ABSENCE, ',', '.') AS DECIMAL(10, 0))
					ELSE
						0
				END AS VISITS_ABSENCE
			FROM
				FLK.REPORT_ROWS
 -- начало таблицы с validation_id
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
										BORDER_DAYS,
										DIGEST_DATE
									WHERE
										REPORT_REGION_ID IS NOT NULL
										AND REPORT_REGION_NAME IS NOT NULL
										AND REPORT_ERRORS IS NULL
										AND REPORT_DATE >= CAST(DATE_DIGEST AS DATE) - (
											SELECT
												DAYS
											FROM
												BORDER_DAYS
											WHERE
												I = 0
										)
										AND REPORT_DATE < CAST(DATE_DIGEST AS DATE)
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
 -- конец таблицы с validation_id
				ON T_ID_REG.C_ID = FLK.REPORT_ROWS.VALIDATION_ID
			WHERE
				SP_DEPART_TYPE_NAME = 'Амбулаторный'
				AND MO_DEPT_NAME = 'Органы исполнительной власти субъектов Российской Федерации, осуществляющие функции в области здравоохранения'
 --and (left(date_report, 10) ~ '^[2][0][2][3-9]-[0-1][0-9]-[0-3][0-9]$' or left(date_report, 10) ~ '^[0-3][0-9].[0-1][0-9].[2][0][2][3-9]$')
 --and slots_created ~ '^[0-9]+$'
 --and slots_booked ~ '^[0-9]+$'
 --and (slot_length ~ '^[0-9]+.[0-9]+$' or slot_length ~ '^[0-9]+,[0-9]+$' or slot_length ~ '^[0-9]+$')
 --and (visits_absence ~ '^[0-9]+$' or visits_absence ~ '^[0-9]+.[0-9]+$')
		) AS T_MAIN_TMP,
		BORDER_DAYS
	WHERE
		C_DATE_REP < C_DATE
		AND C_DATE_REP >= C_DATE - (
			SELECT
				DAYS
			FROM
				BORDER_DAYS
			WHERE
				I = 3
		)
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
	COALESCE (C_SLOTS_ALL,
	0) AS C_SLOTS_ALL,
	COALESCE (C_SLOTS_BOOKED,
	0) AS C_SLOTS_BOOKED,
	COALESCE (C_KONK_SLOTS_ALL,
	0) AS C_KONK_SLOTS_ALL,
	COALESCE (C_KONK_SLOTS_BOOKED,
	0) AS C_KONK_SLOTS_BOOKED,
	COALESCE (C_NEKONK_SLOTS_ALL,
	0) AS C_NEKONK_SLOTS_ALL,
	COALESCE (C_NEKONK_SLOTS_BOOKED,
	0) AS C_NEKONK_SLOTS_BOOKED,
 --COALESCE (c_STAVKA_sum, 0) as c_STAVKA_sum,
	COALESCE (STF_SUM,
	0) AS C_STAVKA_SUM,
	COALESCE (C_STAVKA_DAYS_SUM,
	0) AS C_STAVKA_DAYS_SUM,
	COALESCE (ABSENCE_COUNT,
	0) AS ABSENCE_COUNT,
	COALESCE (SHORT_SLOTS_COUNT,
	0) AS SHORT_SLOTS_COUNT
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
	AND T_SLOTS.MP_DOLGNOST_0 = T_SD.MP_DOLGNOST_3
 -- А равно и число неявок
	FULL JOIN (
		SELECT
			C_REG AS C_REG_4,
			MO_OID AS MO_OID_4,
			MO_SHORT_NAME AS MO_SHORT_NAME_4,
			SP_OID AS SP_OID_4,
			SP_NAME AS SP_NAME_4,
			MP_DOLGNOST AS MP_DOLGNOST_4,
			SUM(VISITS_ABSENCE) AS ABSENCE_COUNT
		FROM
			T_MAIN
		GROUP BY
			C_REG,
			MO_OID,
			MO_SHORT_NAME,
			SP_OID,
			SP_NAME,
			MP_DOLGNOST
	) AS T_ABSENCE_COUNT
	ON T_SLOTS.C_REG_0 = T_ABSENCE_COUNT.C_REG_4
	AND T_SLOTS.MO_OID_0 = T_ABSENCE_COUNT.MO_OID_4
	AND T_SLOTS.MO_SHORT_NAME_0 = T_ABSENCE_COUNT.MO_SHORT_NAME_4
	AND T_SLOTS.SP_OID_0 = T_ABSENCE_COUNT.SP_OID_4
	AND T_SLOTS.SP_NAME_0 = T_ABSENCE_COUNT.SP_NAME_4
	AND T_SLOTS.MP_DOLGNOST_0 = T_ABSENCE_COUNT.MP_DOLGNOST_4
 -- Также количество слотов с длиной меньше 5 мин
	FULL JOIN (
		SELECT
			C_REG AS C_REG_5,
			MO_OID AS MO_OID_5,
			MO_SHORT_NAME AS MO_SHORT_NAME_5,
			SP_OID AS SP_OID_5,
			SP_NAME AS SP_NAME_5,
			MP_DOLGNOST AS MP_DOLGNOST_5,
			SUM(C_SLOTS_C) AS SHORT_SLOTS_COUNT
		FROM
			T_MAIN,
			MIN_SLOT_LENGTH
		WHERE
			SLOT_LENGTH < MM
		GROUP BY
			C_REG,
			MO_OID,
			MO_SHORT_NAME,
			SP_OID,
			SP_NAME,
			MP_DOLGNOST
	) AS T_SHORT_SLOTS_COUNT
	ON T_SLOTS.C_REG_0 = T_SHORT_SLOTS_COUNT.C_REG_5
	AND T_SLOTS.MO_OID_0 = T_SHORT_SLOTS_COUNT.MO_OID_5
	AND T_SLOTS.MO_SHORT_NAME_0 = T_SHORT_SLOTS_COUNT.MO_SHORT_NAME_5
	AND T_SLOTS.SP_OID_0 = T_SHORT_SLOTS_COUNT.SP_OID_5
	AND T_SLOTS.SP_NAME_0 = T_SHORT_SLOTS_COUNT.SP_NAME_5
	AND T_SLOTS.MP_DOLGNOST_0 = T_SHORT_SLOTS_COUNT.MP_DOLGNOST_5
 -- сумма ставок для уникальных людей
	FULL JOIN (
		SELECT
			C_REG AS C_REG_6,
			MO_OID AS MO_OID_6,
			MO_SHORT_NAME AS MO_SHORT_NAME_6,
			SP_OID AS SP_OID_6,
			SP_NAME AS SP_NAME_6,
			MP_DOLGNOST AS MP_DOLGNOST_6,
			SUM(C_STAVKA) AS STF_SUM
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
					C_STAVKA
				FROM
					T_MAIN
			) AS T_S
		GROUP BY
			C_REG_6,
			MO_OID_6,
			MO_SHORT_NAME_6,
			SP_OID_6,
			SP_NAME_6,
			MP_DOLGNOST_6
	) AS T_STF
	ON T_SLOTS.C_REG_0 = T_STF.C_REG_6
	AND T_SLOTS.MO_OID_0 = T_STF.MO_OID_6
	AND T_SLOTS.MO_SHORT_NAME_0 = T_STF.MO_SHORT_NAME_6
	AND T_SLOTS.SP_OID_0 = T_STF.SP_OID_6
	AND T_SLOTS.SP_NAME_0 = T_STF.SP_NAME_6
	AND T_SLOTS.MP_DOLGNOST_0 = T_STF.MP_DOLGNOST_6;