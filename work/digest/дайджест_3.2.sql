-- ВЫГРУЗКА
WITH -- настройки:
 -- выбор расчётной даты для каждого региона: 0 - последняя дата, 1 - предпоследняя дата:
SELECT_FLAG(FLAG_) AS (
                       VALUES
(1)), -- выбор даты дайджеста (выгрузки) - следующий день по окончании анализируемого периода (важно!):
DIGEST_DATE
(DATE_DIGEST) AS
(
                             VALUES
('2024-01-29')), -- "отступы": max дней от даты дайджеста до даты последнего отчёта (7), границы интервала для даты предпоследнего отчёта (от 4 до 10) и 14 дней:
BORDER_DAYS
(I, DAYS) AS
(
                         VALUES
(0,
                                 7),
(1,
                                      4),
(2,
                                           10),
(3,
                                                 14)), -- продолжительность "короткого" слота, мин:
MIN_SLOT_LENGTH
(MM) AS
(
                        VALUES
(5)), -- "основная таблица"
T_MAIN AS
(SELECT C_ID,
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
    (SELECT C_ID,
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
                 WHEN SLOTS_CREATED ~ '[0-9]+$' THEN CAST(SLOTS_CREATED AS DECIMAL(10, 0))
                 WHEN REPLACE(SLOTS_CREATED, ',', '.') ~ '[0-9]+.[0]+$' THEN CAST(REPLACE(SLOTS_CREATED, ',', '.') AS DECIMAL(10, 0))
                 ELSE 0
             END AS C_SLOTS_C,
        CASE
                 WHEN SLOTS_BOOKED ~ '^[0-9]+$' THEN CAST(SLOTS_BOOKED AS DECIMAL(10, 0))
                 WHEN REPLACE(SLOTS_BOOKED, ',', '.') ~ '[0-9]+.[0]+$' THEN CAST(REPLACE(SLOTS_BOOKED, ',', '.') AS DECIMAL(10, 0))
                 ELSE 0
             END AS C_SLOTS_B,
        CASE
                 WHEN LEFT(DATE_REPORT, 10) ~ '^[0-3][0-9].[0-1][0-9].[2][0][2][3-9]$' THEN TO_DATE(DATE_REPORT, 'dd.mm.yyyy')
                 WHEN LEFT(DATE_REPORT, 10) ~ '^[2][0][2][3-9]-[0-1][0-9]-[0-3][0-9]$' THEN TO_DATE(DATE_REPORT, 'yyyy-mm-dd')
                 ELSE TO_DATE('1900-01-01', 'yyyy-mm-dd')
             END AS C_DATE_REP,
        CASE
                 WHEN REPLACE(MP_STAVKA, ',', '') ~ '^[0]+$' THEN 1.000
                 WHEN REPLACE(MP_STAVKA, '.', '') ~ '^[0]+$' THEN 1.000
                 WHEN REPLACE(MP_STAVKA, ',', '.') ~ '^[0-9]+.[0-9]+$' THEN CAST(REPLACE(MP_STAVKA, ',', '.') AS DECIMAL(10, 3))
                 WHEN REPLACE(MP_STAVKA, ',', '.') ~ '^.[0-9]+$' THEN CAST(REPLACE(CONCAT('0', MP_STAVKA), ',', '.') AS DECIMAL(10, 3))
                 WHEN MP_STAVKA ~ '^[1-9][0-9]+$' THEN CAST(MP_STAVKA AS DECIMAL(10, 3))
                 WHEN MP_STAVKA ~ '^[0]+$' THEN 1.000
                 WHEN MP_STAVKA IS NULL THEN 1.000
                 WHEN MP_STAVKA = '' THEN 1.000
                 ELSE 1.000
             END AS C_STAVKA,
        CASE
                 WHEN
                      REPLACE (SLOT_LENGTH,
                               ',',
                               '.') ~ '^[0-9]+.[0-9]+$' THEN CAST
                        (REPLACE (SLOT_LENGTH, ',', '.') AS DECIMAL(10, 0))
                 WHEN SLOT_LENGTH ~ '^[0-9]+$' THEN CAST(SLOT_LENGTH AS DECIMAL(10, 0))
                 ELSE 0
             END AS SLOT_LENGTH,
        CASE
                 WHEN VISITS_ABSENCE ~ '^[0-9]+$' THEN CAST(VISITS_ABSENCE AS DECIMAL(10, 0))
                 WHEN REPLACE(VISITS_ABSENCE, ',', '.') ~ '^[0-9]+.[0]+$' THEN CAST(REPLACE(VISITS_ABSENCE, ',', '.') AS DECIMAL(10, 0))
                 ELSE 0
             END AS VISITS_ABSENCE
    FROM FLK.REPORT_ROWS
        RIGHT JOIN
        (SELECT CASE
                    WHEN FLAG_ = 0 THEN V1
                    ELSE V2
                END AS C_ID,
            CASE
                    WHEN FLAG_ = 0 THEN DATE_1
                    ELSE DATE_2
                END AS C_DATE,
            REPORT_REGION_NAME AS C_REG
        FROM
            (SELECT MAX(VALIDATION_ID) AS V2,
                MAX (Q2.REPORT_DATE) AS DATE_2,
                V1,
                DATE_1,
                Q2.REPORT_REGION_NAME
            FROM FLK.VALIDATIONS AS Q2
                RIGHT JOIN
                (SELECT MAX(VALIDATION_ID) AS V1,
                    MAX(REPORT_DATE) AS DATE_1,
                    REPORT_REGION_ID
                FROM FLK.VALIDATIONS,
                    DIGEST_DATE
                WHERE REPORT_REGION_ID IS NOT NULL
                    AND REPORT_REGION_NAME IS NOT NULL
                    AND REPORT_ERRORS IS NULL
                    AND REPORT_DATE >= CAST(DATE_DIGEST AS DATE) -
                   (SELECT DAYS
                    FROM BORDER_DAYS
                    WHERE I = 0 )
                    AND REPORT_DATE < CAST(DATE_DIGEST AS DATE)
                GROUP BY REPORT_REGION_ID) AS Q1 ON Q1.REPORT_REGION_ID = Q2.REPORT_REGION_ID
            WHERE Q2.VALIDATION_ID < V1
                AND Q2.REPORT_DATE <> DATE_1
                AND DATE_1 -
                (SELECT DAYS
                FROM BORDER_DAYS
                WHERE I = 1 ) > Q2.REPORT_DATE -- тут заменил >= на >, иначе периоды отчётов пересекались
                AND Q2.REPORT_DATE>= DATE_1 -
                (SELECT DAYS
                FROM BORDER_DAYS
                WHERE I = 2 )
                AND REPORT_ERRORS IS NULL
            GROUP BY V1,
                     Q2.REPORT_REGION_NAME,
                     DATE_1
            ORDER BY REPORT_REGION_NAME)AS ACTUAL_DATA,
            SELECT_FLAG) AS T_ID_REG ON T_ID_REG.C_ID = FLK.REPORT_ROWS.VALIDATION_ID
    WHERE SP_DEPART_TYPE_NAME = 'Амбулаторный'
        AND MO_DEPT_NAME = 'Органы исполнительной власти субъектов Российской Федерации, осуществляющие функции в области здравоохранения' ) AS T_MAIN_TMP
WHERE C_DATE_REP < C_DATE
    AND C_DATE_REP >= C_DATE -
       (SELECT DAYS
    FROM BORDER_DAYS
    WHERE I = 3 ) )
SELECT DISTINCT TM.C_ID,
    TM.C_DATE,
    TM.C_REG,
    TM.MO_OID,
    TM.MO_SHORT_NAME,
    TM.SP_OID,
    TM.SP_NAME,
    TM.MP_DOLGNOST,
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
    C_STAVKA_SUM,
    C_STAVKA_DAYS_SUM,
    COALESCE (C_ABSENCE_COUNT,
                                                                                0) AS C_ABSENCE_COUNT,
    COALESCE (C_SHORT_SLOTS_COUNT,
                                                                                         0) AS C_SHORT_SLOTS_COUNT
FROM T_MAIN TM
    LEFT JOIN
    (SELECT TM.C_ID,
        TM.C_REG,
        TM.C_DATE,
        TM.MO_OID,
        TM.MO_SHORT_NAME,
        TM.SP_OID,
        TM.SP_NAME,
        TM.MP_DOLGNOST,
        SUM(TM.C_SLOTS_C) AS C_SLOTS_ALL,
        SUM(TM.C_SLOTS_B) AS C_SLOTS_BOOKED
    FROM T_MAIN TM
    GROUP BY TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST) AS TM0 ON TM.C_REG = TM0.C_REG
        AND TM.MO_OID = TM0.MO_OID
        AND TM.MO_SHORT_NAME = TM0.MO_SHORT_NAME
        AND TM.SP_OID = TM0.SP_OID
        AND TM.SP_NAME = TM0.SP_NAME
        AND TM.MP_DOLGNOST = TM0.MP_DOLGNOST
    LEFT JOIN
    (SELECT TM.C_ID,
        TM.C_REG,
        TM.C_DATE,
        TM.MO_OID,
        TM.MO_SHORT_NAME,
        TM.SP_OID,
        TM.SP_NAME,
        TM.MP_DOLGNOST,
        SUM(TM.C_SLOTS_C) AS C_KONK_SLOTS_ALL,
        SUM(TM.C_SLOTS_B) AS C_KONK_SLOTS_BOOKED
    FROM T_MAIN TM
    WHERE LOWER(TM.SLOTS_TYPE) LIKE 'доступ%'
    OR LOWER(TM.SLOTS_TYPE) = 'да'
    GROUP BY TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST) AS TM1 ON TM.C_REG = TM1.C_REG
        AND TM.MO_OID = TM1.MO_OID
        AND TM.MO_SHORT_NAME = TM1.MO_SHORT_NAME
        AND TM.SP_OID = TM1.SP_OID
        AND TM.SP_NAME = TM1.SP_NAME
        AND TM.MP_DOLGNOST = TM1.MP_DOLGNOST
    LEFT JOIN
    (SELECT TM.C_ID,
        TM.C_REG,
        TM.C_DATE,
        TM.MO_OID,
        TM.MO_SHORT_NAME,
        TM.SP_OID,
        TM.SP_NAME,
        TM.MP_DOLGNOST,
        SUM(TM.C_SLOTS_C) AS C_NEKONK_SLOTS_ALL,
        SUM(TM.C_SLOTS_B) AS C_NEKONK_SLOTS_BOOKED
    FROM T_MAIN TM
    WHERE LOWER(TM.SLOTS_TYPE) LIKE 'не%доступ%'
    OR LOWER(TM.SLOTS_TYPE) = 'нет'
    GROUP BY TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST) AS TM2 ON TM.C_REG = TM2.C_REG
        AND TM.MO_OID = TM2.MO_OID
        AND TM.MO_SHORT_NAME = TM2.MO_SHORT_NAME
        AND TM.SP_OID = TM2.SP_OID
        AND TM.SP_NAME = TM2.SP_NAME
        AND TM.MP_DOLGNOST = TM2.MP_DOLGNOST
    LEFT JOIN
    (SELECT TS.C_REG,
        TS.MO_OID,
        TS.MO_SHORT_NAME,
        TS.SP_OID,
        TS.SP_NAME,
        TS.MP_DOLGNOST,
        SUM(TS.C_STAVKA) AS C_STAVKA_DAYS_SUM
    FROM
        (SELECT DISTINCT TM.C_REG,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST,
            TM.MP_FIO,
            TM.C_DATE_REP,
            TM.C_STAVKA
        FROM T_MAIN TM) TS
    GROUP BY TS.C_REG,
            TS.MO_OID,
            TS.MO_SHORT_NAME,
            TS.SP_OID,
            TS.SP_NAME,
            TS.MP_DOLGNOST) AS TM3 ON TM.C_REG = TM3.C_REG
        AND TM.MO_OID = TM3.MO_OID
        AND TM.MO_SHORT_NAME = TM3.MO_SHORT_NAME
        AND TM.SP_OID = TM3.SP_OID
        AND TM.SP_NAME = TM3.SP_NAME
        AND TM.MP_DOLGNOST = TM3.MP_DOLGNOST
    LEFT JOIN
    (SELECT TS.C_REG,
        TS.MO_OID,
        TS.MO_SHORT_NAME,
        TS.SP_OID,
        TS.SP_NAME,
        TS.MP_DOLGNOST,
        SUM(TS.C_STAVKA) AS C_STAVKA_SUM
    FROM
        (SELECT DISTINCT TM.C_REG,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST,
            TM.MP_FIO,
            TM.C_STAVKA
        FROM T_MAIN TM) TS
    GROUP BY TS.C_REG,
            TS.MO_OID,
            TS.MO_SHORT_NAME,
            TS.SP_OID,
            TS.SP_NAME,
            TS.MP_DOLGNOST) AS TM4 ON TM.C_REG = TM4.C_REG
        AND TM.MO_OID = TM4.MO_OID
        AND TM.MO_SHORT_NAME = TM4.MO_SHORT_NAME
        AND TM.SP_OID = TM4.SP_OID
        AND TM.SP_NAME = TM4.SP_NAME
        AND TM.MP_DOLGNOST = TM4.MP_DOLGNOST
    LEFT JOIN
    (SELECT TM.C_ID,
        TM.C_REG,
        TM.C_DATE,
        TM.MO_OID,
        TM.MO_SHORT_NAME,
        TM.SP_OID,
        TM.SP_NAME,
        TM.MP_DOLGNOST,
        SUM(TM.VISITS_ABSENCE) AS C_ABSENCE_COUNT
    FROM T_MAIN TM
    GROUP BY TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST) AS TM5 ON TM.C_REG = TM5.C_REG
        AND TM.MO_OID = TM5.MO_OID
        AND TM.MO_SHORT_NAME = TM5.MO_SHORT_NAME
        AND TM.SP_OID = TM5.SP_OID
        AND TM.SP_NAME = TM5.SP_NAME
        AND TM.MP_DOLGNOST = TM5.MP_DOLGNOST
    LEFT JOIN
    (SELECT TM.C_ID,
        TM.C_REG,
        TM.C_DATE,
        TM.MO_OID,
        TM.MO_SHORT_NAME,
        TM.SP_OID,
        TM.SP_NAME,
        TM.MP_DOLGNOST,
        SUM(TM.C_SLOTS_C) AS C_SHORT_SLOTS_COUNT
    FROM T_MAIN TM,
        MIN_SLOT_LENGTH
    WHERE SLOT_LENGTH < MM
    GROUP BY TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST) AS TM6 ON TM.C_REG = TM6.C_REG
        AND TM.MO_OID = TM6.MO_OID
        AND TM.MO_SHORT_NAME = TM6.MO_SHORT_NAME
        AND TM.SP_OID = TM6.SP_OID
        AND TM.SP_NAME = TM6.SP_NAME
        AND TM.MP_DOLGNOST = TM6.MP_DOLGNOST;
        
       
--------------------------------------------------------------------------------------------------------------------------
--ПРЕДВЫГРУЗКА
WITH
 -- настройки:
 -- выбор расчётной даты для каждого региона: 0 - последняя дата, 1 - предпоследняя дата:
SELECT_FLAG(FLAG_) AS (VALUES(1)),
 -- выбор даты дайджеста (выгрузки) - следующий день по окончании анализируемого периода (важно!):
DIGEST_DATE(DATE_DIGEST) AS (VALUES('2024-01-29')),
 -- "отступы": max дней от даты дайджеста до даты последнего отчёта (7), границы интервала для даты предпоследнего отчёта (от 4 до 10) и 14 дней:
BORDER_DAYS(I, DAYS) AS (VALUES (0, 7), (1, 4), (2, 10), (3, 14)),
 -- продолжительность "короткого" слота, мин:
MIN_SLOT_LENGTH(MM) AS (VALUES(5)),
 -- "основная таблица"
T_MAIN AS (
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
					WHEN REPLACE(VISITS_ABSENCE, ',', '.') ~ '^[0-9]+.[0]+$' THEN
						CAST(REPLACE(VISITS_ABSENCE, ',', '.') AS DECIMAL(10, 0))
					ELSE
						0
				END AS VISITS_ABSENCE
			FROM
				FLK.REPORT_ROWS
				RIGHT JOIN (
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
								) > Q2.REPORT_DATE -- тут заменил >= на >, иначе периоды отчётов пересекались
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
						SELECT_FLAG
				) AS T_ID_REG
				ON T_ID_REG.C_ID = FLK.REPORT_ROWS.VALIDATION_ID
			WHERE
				SP_DEPART_TYPE_NAME = 'Амбулаторный'
				AND MO_DEPT_NAME = 'Органы исполнительной власти субъектов Российской Федерации, осуществляющие функции в области здравоохранения'
		) AS T_MAIN_TMP
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
SELECT
    DISTINCT TM.C_DATE,
    TM.C_REG,
    TM.SLOTS_TYPE
FROM
    T_MAIN TM
ORDER BY
    TM.C_REG;


SELECT
    DISTINCT TM.C_DATE,
    TM.C_REG,
    TM.SLOTS_TYPE
    FROM
        T_MAIN TM
    WHERE
        TM.C_REG = 'Республика Татарстан (Татарстан)'
        AND TM.SLOTS_TYPE = ''
    ORDER BY
        TM.C_REG;



---------------------------------------------------------------------------------------------------------------
WITH
 -- настройки:
 -- выбор расчётной даты для каждого региона: 0 - последняя дата, 1 - предпоследняя дата:
SELECT_FLAG(FLAG_) AS (VALUES(0)),
 -- выбор даты дайджеста (выгрузки) - следующий день по окончании анализируемого периода (важно!):
DIGEST_DATE(DATE_DIGEST) AS (VALUES('2024-02-05')),
 -- "отступы": max дней от даты дайджеста до даты последнего отчёта (7), границы интервала для даты предпоследнего отчёта (от 4 до 10) и 14 дней:
BORDER_DAYS(I, DAYS) AS (VALUES (0, 7), (1, 4), (2, 10), (3, 14)),
 -- продолжительность "короткого" слота, мин:
MIN_SLOT_LENGTH(MM) AS (VALUES(5)),
 -- "основная таблица"
T_MAIN AS (
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
                    WHEN REPLACE(VISITS_ABSENCE, ',', '.') ~ '^[0-9]+.[0]+$' THEN
                        CAST(REPLACE(VISITS_ABSENCE, ',', '.') AS DECIMAL(10, 0))
                    ELSE
                        0
                END AS VISITS_ABSENCE
            FROM
                FLK.REPORT_ROWS
                RIGHT JOIN (
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
                                ) >= Q2.REPORT_DATE -- тут заменил >= на >, иначе периоды отчётов пересекались
                                AND Q2.REPORT_DATE>= DATE_1 - ( -- тут заменил >= на >, иначе периоды отчётов пересекались
                                    SELECT
                                        DAYS
                                    FROM
                                        BORDER_DAYS
                                    WHERE
                                        I = 2
                                )
                                -- ВСТАВЛЕНО
                                --AND Q2.REPORT_DATE < CAST(DATE_DIGEST AS DATE) - 7
                                AND Q2.REPORT_DATE < '2024-01-29'
                                --КОНЕЦ ВСТАВКИ
                                AND REPORT_ERRORS IS NULL
                            GROUP BY
                                V1,
                                Q2.REPORT_REGION_NAME,
                                DATE_1
                            ORDER BY
                                REPORT_REGION_NAME
                        )AS ACTUAL_DATA,
                        SELECT_FLAG
                ) AS T_ID_REG
                ON T_ID_REG.C_ID = FLK.REPORT_ROWS.VALIDATION_ID
            WHERE
                SP_DEPART_TYPE_NAME = 'Амбулаторный'
                AND MO_DEPT_NAME = 'Органы исполнительной власти субъектов Российской Федерации, осуществляющие функции в области здравоохранения'
        ) AS T_MAIN_TMP
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
SELECT
    DISTINCT TM.C_ID,
    TM.C_DATE,
    TM.C_REG,
    TM.MO_OID,
    TM.MO_SHORT_NAME,
    TM.SP_OID,
    TM.SP_NAME,
    TM.MP_DOLGNOST,
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
    C_STAVKA_SUM,
    C_STAVKA_DAYS_SUM,
    COALESCE (C_ABSENCE_COUNT,
    0) AS C_ABSENCE_COUNT,
    COALESCE (C_SHORT_SLOTS_COUNT,
    0) AS C_SHORT_SLOTS_COUNT
FROM
    T_MAIN TM
    LEFT JOIN (
        SELECT
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST,
            SUM(TM.C_SLOTS_C) AS C_SLOTS_ALL,
            SUM(TM.C_SLOTS_B) AS C_SLOTS_BOOKED
        FROM
            T_MAIN TM
        GROUP BY
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST
    ) AS TM0
    ON TM.C_REG = TM0.C_REG
    AND TM.MO_OID = TM0.MO_OID
    AND TM.MO_SHORT_NAME = TM0.MO_SHORT_NAME
    AND TM.SP_OID = TM0.SP_OID
    AND TM.SP_NAME = TM0.SP_NAME
    AND TM.MP_DOLGNOST = TM0.MP_DOLGNOST
    LEFT JOIN (
        SELECT
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST,
            SUM(TM.C_SLOTS_C) AS C_KONK_SLOTS_ALL,
            SUM(TM.C_SLOTS_B) AS C_KONK_SLOTS_BOOKED
        FROM
            T_MAIN TM
        WHERE
            LOWER(TM.SLOTS_TYPE) LIKE 'доступ%'
            OR LOWER(TM.SLOTS_TYPE) = 'да'
        GROUP BY
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST
    ) AS TM1
    ON TM.C_REG = TM1.C_REG
    AND TM.MO_OID = TM1.MO_OID
    AND TM.MO_SHORT_NAME = TM1.MO_SHORT_NAME
    AND TM.SP_OID = TM1.SP_OID
    AND TM.SP_NAME = TM1.SP_NAME
    AND TM.MP_DOLGNOST = TM1.MP_DOLGNOST
    LEFT JOIN (
        SELECT
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST,
            SUM(TM.C_SLOTS_C) AS C_NEKONK_SLOTS_ALL,
            SUM(TM.C_SLOTS_B) AS C_NEKONK_SLOTS_BOOKED
        FROM
            T_MAIN TM
        WHERE
            LOWER(TM.SLOTS_TYPE) LIKE 'не%доступ%'
            OR LOWER(TM.SLOTS_TYPE) = 'нет'
        GROUP BY
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST
    ) AS TM2
    ON TM.C_REG = TM2.C_REG
    AND TM.MO_OID = TM2.MO_OID
    AND TM.MO_SHORT_NAME = TM2.MO_SHORT_NAME
    AND TM.SP_OID = TM2.SP_OID
    AND TM.SP_NAME = TM2.SP_NAME
    AND TM.MP_DOLGNOST = TM2.MP_DOLGNOST
    LEFT JOIN (
        SELECT
            TS.C_REG,
            TS.MO_OID,
            TS.MO_SHORT_NAME,
            TS.SP_OID,
            TS.SP_NAME,
            TS.MP_DOLGNOST,
            SUM(TS.C_STAVKA) AS C_STAVKA_DAYS_SUM
        FROM
            (
                SELECT
                    DISTINCT TM.C_REG,
                    TM.MO_OID,
                    TM.MO_SHORT_NAME,
                    TM.SP_OID,
                    TM.SP_NAME,
                    TM.MP_DOLGNOST,
                    TM.MP_FIO,
                    TM.C_DATE_REP,
                    TM.C_STAVKA
                FROM
                    T_MAIN TM
            ) TS
        GROUP BY
            TS.C_REG,
            TS.MO_OID,
            TS.MO_SHORT_NAME,
            TS.SP_OID,
            TS.SP_NAME,
            TS.MP_DOLGNOST
    ) AS TM3
    ON TM.C_REG = TM3.C_REG
    AND TM.MO_OID = TM3.MO_OID
    AND TM.MO_SHORT_NAME = TM3.MO_SHORT_NAME
    AND TM.SP_OID = TM3.SP_OID
    AND TM.SP_NAME = TM3.SP_NAME
    AND TM.MP_DOLGNOST = TM3.MP_DOLGNOST
    LEFT JOIN (
        SELECT
            TS.C_REG,
            TS.MO_OID,
            TS.MO_SHORT_NAME,
            TS.SP_OID,
            TS.SP_NAME,
            TS.MP_DOLGNOST,
            SUM(TS.C_STAVKA) AS C_STAVKA_SUM
        FROM
            (
                SELECT
                    DISTINCT TM.C_REG,
                    TM.MO_OID,
                    TM.MO_SHORT_NAME,
                    TM.SP_OID,
                    TM.SP_NAME,
                    TM.MP_DOLGNOST,
                    TM.MP_FIO,
                    TM.C_STAVKA
                FROM
                    T_MAIN TM
            ) TS
        GROUP BY
            TS.C_REG,
            TS.MO_OID,
            TS.MO_SHORT_NAME,
            TS.SP_OID,
            TS.SP_NAME,
            TS.MP_DOLGNOST
    ) AS TM4
    ON TM.C_REG = TM4.C_REG
    AND TM.MO_OID = TM4.MO_OID
    AND TM.MO_SHORT_NAME = TM4.MO_SHORT_NAME
    AND TM.SP_OID = TM4.SP_OID
    AND TM.SP_NAME = TM4.SP_NAME
    AND TM.MP_DOLGNOST = TM4.MP_DOLGNOST
    LEFT JOIN (
        SELECT
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST,
            SUM(TM.VISITS_ABSENCE) AS C_ABSENCE_COUNT
        FROM
            T_MAIN TM
        GROUP BY
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST
    ) AS TM5
    ON TM.C_REG = TM5.C_REG
    AND TM.MO_OID = TM5.MO_OID
    AND TM.MO_SHORT_NAME = TM5.MO_SHORT_NAME
    AND TM.SP_OID = TM5.SP_OID
    AND TM.SP_NAME = TM5.SP_NAME
    AND TM.MP_DOLGNOST = TM5.MP_DOLGNOST
    LEFT JOIN (
        SELECT
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST,
            SUM(TM.C_SLOTS_C) AS C_SHORT_SLOTS_COUNT
        FROM
            T_MAIN TM,
            MIN_SLOT_LENGTH
        WHERE
            SLOT_LENGTH < MM
        GROUP BY
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST
    ) AS TM6
    ON TM.C_REG = TM6.C_REG
    AND TM.MO_OID = TM6.MO_OID
    AND TM.MO_SHORT_NAME = TM6.MO_SHORT_NAME
    AND TM.SP_OID = TM6.SP_OID
    AND TM.SP_NAME = TM6.SP_NAME
    AND TM.MP_DOLGNOST = TM6.MP_DOLGNOST;

-----------------------------------------------------------------------------------------------------------------------------------------------------
WITH
 -- настройки:
 -- выбор расчётной даты для каждого региона: 0 - последняя дата, 1 - предпоследняя дата:
SELECT_FLAG(FLAG_) AS (VALUES(0)),
 -- выбор даты дайджеста (выгрузки) - следующий день по окончании анализируемого периода (важно!):
DIGEST_DATE(DATE_DIGEST) AS (VALUES('2024-02-05')),
 -- "отступы": max дней от даты дайджеста до даты последнего отчёта (7), границы интервала для даты предпоследнего отчёта (от 4 до 10) и 14 дней:
BORDER_DAYS(I, DAYS) AS (VALUES (0, 7), (1, 4), (2, 10), (3, 14)),
 -- продолжительность "короткого" слота, мин:
MIN_SLOT_LENGTH(MM) AS (VALUES(5)),
 -- "основная таблица"
T_MAIN AS (
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
                    WHEN REPLACE(VISITS_ABSENCE, ',', '.') ~ '^[0-9]+.[0]+$' THEN
                        CAST(REPLACE(VISITS_ABSENCE, ',', '.') AS DECIMAL(10, 0))
                    ELSE
                        0
                END AS VISITS_ABSENCE
            FROM
                FLK.REPORT_ROWS
                RIGHT JOIN (
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
                                ) >= Q2.REPORT_DATE -- тут заменил >= на >, иначе периоды отчётов пересекались
                                AND Q2.REPORT_DATE >= DATE_1 - ( -- тут заменил >= на >, иначе периоды отчётов пересекались
                                    SELECT
                                        DAYS
                                    FROM
                                        BORDER_DAYS
                                    WHERE
                                        I = 2
                                )
                                -- ВСТАВЛЕНО
                                AND Q2.REPORT_DATE < '2024-01-29'
                                --КОНЕЦ ВСТАВКИ
                                AND REPORT_ERRORS IS NULL
                            GROUP BY
                                V1,
                                Q2.REPORT_REGION_NAME,
                                DATE_1
                            ORDER BY
                                REPORT_REGION_NAME
                        )AS ACTUAL_DATA,
                        SELECT_FLAG
                ) AS T_ID_REG
                ON T_ID_REG.C_ID = FLK.REPORT_ROWS.VALIDATION_ID
            WHERE
                SP_DEPART_TYPE_NAME = 'Амбулаторный'
                AND MO_DEPT_NAME = 'Органы исполнительной власти субъектов Российской Федерации, осуществляющие функции в области здравоохранения'
        ) AS T_MAIN_TMP
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
SELECT
    DISTINCT TM.C_DATE,
    TM.C_REG,
    TM.SLOTS_TYPE
FROM
    T_MAIN TM
ORDER BY
    TM.C_REG;





--ТРИВИАЛЬНАЯ ОТБИРАЛКА
WITH    SELECT_FLAG(FLAG_) AS (VALUES(0)),
        BORDERS(I, D) AS (VALUES (0, '2024-01-22'), (1, '2024-01-28'), (2, '2024-01-29'), (3, '2024-02-04'))
SELECT
    CASE
        WHEN FLAG_ = 0 THEN
            IDT.ID_LAST
        ELSE
            IDT.ID_BEFORE
    END AS C_ID,
    CASE
        WHEN FLAG_ = 0 THEN
            IDT.DATE_LAST
        ELSE
            IDT.DATE_BEFORE
    END AS C_DATE,
    IDT.REPORT_REGION_NAME AS C_REG
FROM
    (
        SELECT
            V.REPORT_REGION_NAME,
            MAX(V.VALIDATION_ID) AS ID_LAST,
            MAX(V.REPORT_DATE) AS DATE_LAST,
            V1.ID_BEFORE,
            V1.DATE_BEFORE
        FROM
            FLK.VALIDATIONS V
            RIGHT JOIN (
                SELECT
                    V.REPORT_REGION_NAME,
                    MAX(V.VALIDATION_ID) AS ID_BEFORE,
                    MAX(V.REPORT_DATE) AS DATE_BEFORE
                FROM
                    FLK.VALIDATIONS V
                --where v.report_date between '2024-01-22' and '2024-01-28'
                WHERE
                    V.REPORT_DATE BETWEEN CAST((
                        SELECT
                            D
                        FROM
                            BORDERS
                        WHERE
                            I = 0
                    ) AS DATE) AND CAST((
                        SELECT
                            D
                        FROM
                            BORDERS
                        WHERE
                            I = 1
                    ) AS DATE)
                GROUP BY
                    V.REPORT_REGION_NAME
            ) AS V1
            ON V.REPORT_REGION_NAME = V1.REPORT_REGION_NAME
        --where v.report_date between '2024-01-29' and '2024-02-04'
        WHERE
            V.REPORT_DATE BETWEEN CAST((
                SELECT
                    D
                FROM
                    BORDERS
                WHERE
                    I = 2
            ) AS DATE) AND CAST((
                SELECT
                    D
                FROM
                    BORDERS
                WHERE
                    I = 3
            ) AS DATE)
        GROUP BY
            V.REPORT_REGION_NAME,
            V1.ID_BEFORE,
            V1.DATE_BEFORE
    ) AS IDT,
    SELECT_FLAG;







------------------------------------------------------------------------------------------------------------------------------------
WITH
 -- настройки:
 -- выбор расчётного периода: 0 - последняя дата, 1 - предпоследняя дата:
SELECT_FLAG(FLAG_) AS (VALUES(0)),
 -- выбор границ недель:
BORDERS(I, D) AS (VALUES (0, '2024-01-22'), (1, '2024-01-28'), (2, '2024-01-29'), (3, '2024-02-04')),
 -- какие-то 14 дней
DAYS(DAYS) AS (VALUES(14)),
 -- продолжительность "короткого" слота, мин:
MIN_SLOT_LENGTH(MM) AS (VALUES(5)),
 -- "основная таблица"
T_MAIN AS (
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
                    WHEN REPLACE(VISITS_ABSENCE, ',', '.') ~ '^[0-9]+.[0]+$' THEN
                        CAST(REPLACE(VISITS_ABSENCE, ',', '.') AS DECIMAL(10, 0))
                    ELSE
                        0
                END AS VISITS_ABSENCE
            FROM
                FLK.REPORT_ROWS
                RIGHT JOIN (
                    SELECT
                        CASE
                            WHEN FLAG_ = 0 THEN
                                IDT.ID_LAST
                            ELSE
                                IDT.ID_BEFORE
                        END AS C_ID,
                        CASE
                            WHEN FLAG_ = 0 THEN
                                IDT.DATE_LAST
                            ELSE
                                IDT.DATE_BEFORE
                        END AS C_DATE,
                        IDT.REPORT_REGION_NAME AS C_REG
                    FROM
                        (
                            SELECT
                                V.REPORT_REGION_NAME,
                                MAX(V.VALIDATION_ID) AS ID_LAST,
                                MAX(V.REPORT_DATE) AS DATE_LAST,
                                V1.ID_BEFORE,
                                V1.DATE_BEFORE
                            FROM
                                FLK.VALIDATIONS V
                                RIGHT JOIN (
                                    SELECT
                                        V.REPORT_REGION_NAME,
                                        MAX(V.VALIDATION_ID) AS ID_BEFORE,
                                        MAX(V.REPORT_DATE) AS DATE_BEFORE
                                    FROM
                                        FLK.VALIDATIONS V
 --where v.report_date between '2024-01-22' and '2024-01-28'
                                    WHERE
                                        V.REPORT_DATE BETWEEN CAST((
                                            SELECT
                                                D
                                            FROM
                                                BORDERS
                                            WHERE
                                                I = 0
                                        ) AS DATE) AND CAST((
                                            SELECT
                                                D
                                            FROM
                                                BORDERS
                                            WHERE
                                                I = 1
                                        ) AS DATE)
                                    GROUP BY
                                        V.REPORT_REGION_NAME
                                ) AS V1
                                ON V.REPORT_REGION_NAME = V1.REPORT_REGION_NAME
 --where v.report_date between '2024-01-29' and '2024-02-04'
                            WHERE
                                V.REPORT_DATE BETWEEN CAST((
                                    SELECT
                                        D
                                    FROM
                                        BORDERS
                                    WHERE
                                        I = 2
                                ) AS DATE) AND CAST((
                                    SELECT
                                        D
                                    FROM
                                        BORDERS
                                    WHERE
                                        I = 3
                                ) AS DATE)
                            GROUP BY
                                V.REPORT_REGION_NAME,
                                V1.ID_BEFORE,
                                V1.DATE_BEFORE
                        ) AS IDT,
                        SELECT_FLAG
                ) AS T_ID_REG
                ON T_ID_REG.C_ID = FLK.REPORT_ROWS.VALIDATION_ID
            WHERE
                SP_DEPART_TYPE_NAME = 'Амбулаторный'
                AND MO_DEPT_NAME = 'Органы исполнительной власти субъектов Российской Федерации, осуществляющие функции в области здравоохранения'
        ) AS T_MAIN_TMP,
        DAYS
    WHERE
        C_DATE_REP < C_DATE
        AND C_DATE_REP >= C_DATE - DAYS
)

-- ПОЛНОСТЬЮ
--или это 1
SELECT
    DISTINCT TM.C_DATE,
    TM.C_REG,
    TM.SLOTS_TYPE
FROM
    T_MAIN TM
ORDER BY
    TM.C_REG;

--конец или этого 1

--или это 2
SELECT
    DISTINCT TM.C_ID,
    TM.C_DATE,
    TM.C_REG,
    TM.MO_OID,
    TM.MO_SHORT_NAME,
    TM.SP_OID,
    TM.SP_NAME,
    TM.MP_DOLGNOST,
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
    C_STAVKA_SUM,
    C_STAVKA_DAYS_SUM,
    COALESCE (C_ABSENCE_COUNT,
    0) AS C_ABSENCE_COUNT,
    COALESCE (C_SHORT_SLOTS_COUNT,
    0) AS C_SHORT_SLOTS_COUNT
FROM
    T_MAIN TM
    LEFT JOIN (
        SELECT
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST,
            SUM(TM.C_SLOTS_C) AS C_SLOTS_ALL,
            SUM(TM.C_SLOTS_B) AS C_SLOTS_BOOKED
        FROM
            T_MAIN TM
        GROUP BY
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST
    ) AS TM0
    ON TM.C_REG = TM0.C_REG
    AND TM.MO_OID = TM0.MO_OID
    AND TM.MO_SHORT_NAME = TM0.MO_SHORT_NAME
    AND TM.SP_OID = TM0.SP_OID
    AND TM.SP_NAME = TM0.SP_NAME
    AND TM.MP_DOLGNOST = TM0.MP_DOLGNOST
    LEFT JOIN (
        SELECT
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST,
            SUM(TM.C_SLOTS_C) AS C_KONK_SLOTS_ALL,
            SUM(TM.C_SLOTS_B) AS C_KONK_SLOTS_BOOKED
        FROM
            T_MAIN TM
        WHERE
            LOWER(TM.SLOTS_TYPE) LIKE 'доступ%'
            OR LOWER(TM.SLOTS_TYPE) = 'да'
        GROUP BY
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST
    ) AS TM1
    ON TM.C_REG = TM1.C_REG
    AND TM.MO_OID = TM1.MO_OID
    AND TM.MO_SHORT_NAME = TM1.MO_SHORT_NAME
    AND TM.SP_OID = TM1.SP_OID
    AND TM.SP_NAME = TM1.SP_NAME
    AND TM.MP_DOLGNOST = TM1.MP_DOLGNOST
    LEFT JOIN (
        SELECT
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST,
            SUM(TM.C_SLOTS_C) AS C_NEKONK_SLOTS_ALL,
            SUM(TM.C_SLOTS_B) AS C_NEKONK_SLOTS_BOOKED
        FROM
            T_MAIN TM
        WHERE
            LOWER(TM.SLOTS_TYPE) LIKE 'не%доступ%'
            OR LOWER(TM.SLOTS_TYPE) = 'нет'
        GROUP BY
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST
    ) AS TM2
    ON TM.C_REG = TM2.C_REG
    AND TM.MO_OID = TM2.MO_OID
    AND TM.MO_SHORT_NAME = TM2.MO_SHORT_NAME
    AND TM.SP_OID = TM2.SP_OID
    AND TM.SP_NAME = TM2.SP_NAME
    AND TM.MP_DOLGNOST = TM2.MP_DOLGNOST
    LEFT JOIN (
        SELECT
            TS.C_REG,
            TS.MO_OID,
            TS.MO_SHORT_NAME,
            TS.SP_OID,
            TS.SP_NAME,
            TS.MP_DOLGNOST,
            SUM(TS.C_STAVKA) AS C_STAVKA_DAYS_SUM
        FROM
            (
                SELECT
                    DISTINCT TM.C_REG,
                    TM.MO_OID,
                    TM.MO_SHORT_NAME,
                    TM.SP_OID,
                    TM.SP_NAME,
                    TM.MP_DOLGNOST,
                    TM.MP_FIO,
                    TM.C_DATE_REP,
                    TM.C_STAVKA
                FROM
                    T_MAIN TM
            ) TS
        GROUP BY
            TS.C_REG,
            TS.MO_OID,
            TS.MO_SHORT_NAME,
            TS.SP_OID,
            TS.SP_NAME,
            TS.MP_DOLGNOST
    ) AS TM3
    ON TM.C_REG = TM3.C_REG
    AND TM.MO_OID = TM3.MO_OID
    AND TM.MO_SHORT_NAME = TM3.MO_SHORT_NAME
    AND TM.SP_OID = TM3.SP_OID
    AND TM.SP_NAME = TM3.SP_NAME
    AND TM.MP_DOLGNOST = TM3.MP_DOLGNOST
    LEFT JOIN (
        SELECT
            TS.C_REG,
            TS.MO_OID,
            TS.MO_SHORT_NAME,
            TS.SP_OID,
            TS.SP_NAME,
            TS.MP_DOLGNOST,
            SUM(TS.C_STAVKA) AS C_STAVKA_SUM
        FROM
            (
                SELECT
                    DISTINCT TM.C_REG,
                    TM.MO_OID,
                    TM.MO_SHORT_NAME,
                    TM.SP_OID,
                    TM.SP_NAME,
                    TM.MP_DOLGNOST,
                    TM.MP_FIO,
                    TM.C_STAVKA
                FROM
                    T_MAIN TM
            ) TS
        GROUP BY
            TS.C_REG,
            TS.MO_OID,
            TS.MO_SHORT_NAME,
            TS.SP_OID,
            TS.SP_NAME,
            TS.MP_DOLGNOST
    ) AS TM4
    ON TM.C_REG = TM4.C_REG
    AND TM.MO_OID = TM4.MO_OID
    AND TM.MO_SHORT_NAME = TM4.MO_SHORT_NAME
    AND TM.SP_OID = TM4.SP_OID
    AND TM.SP_NAME = TM4.SP_NAME
    AND TM.MP_DOLGNOST = TM4.MP_DOLGNOST
    LEFT JOIN (
        SELECT
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST,
            SUM(TM.VISITS_ABSENCE) AS C_ABSENCE_COUNT
        FROM
            T_MAIN TM
        GROUP BY
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST
    ) AS TM5
    ON TM.C_REG = TM5.C_REG
    AND TM.MO_OID = TM5.MO_OID
    AND TM.MO_SHORT_NAME = TM5.MO_SHORT_NAME
    AND TM.SP_OID = TM5.SP_OID
    AND TM.SP_NAME = TM5.SP_NAME
    AND TM.MP_DOLGNOST = TM5.MP_DOLGNOST
    LEFT JOIN (
        SELECT
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST,
            SUM(TM.C_SLOTS_C) AS C_SHORT_SLOTS_COUNT
        FROM
            T_MAIN TM,
            MIN_SLOT_LENGTH
        WHERE
            SLOT_LENGTH < MM
        GROUP BY
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.MO_SHORT_NAME,
            TM.SP_OID,
            TM.SP_NAME,
            TM.MP_DOLGNOST
    ) AS TM6
    ON TM.C_REG = TM6.C_REG
    AND TM.MO_OID = TM6.MO_OID
    AND TM.MO_SHORT_NAME = TM6.MO_SHORT_NAME
    AND TM.SP_OID = TM6.SP_OID
    AND TM.SP_NAME = TM6.SP_NAME
    AND TM.MP_DOLGNOST = TM6.MP_DOLGNOST;

-- конец или это 2