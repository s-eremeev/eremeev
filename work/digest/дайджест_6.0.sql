WITH
 -- ПРОДОЛЖИТЕЛЬНОСТЬ "КОРОТКОГО" СЛОТА (НАСЛЕНИЕ ПРОШЛОГО)
MIN_SLOT_LENGTH(MM) AS (VALUES(5)), T_MAIN AS(
    SELECT
        T0.C_ID,
        T0.C_REG,
        T0.C_DATE,
        T0.MO_OID,
        T0.SP_OID,
        T0.MP_DOLGNOST,
        T0.MP_ID,
        T0.MP_FIO,
        T0.SLOTS_TYPE,
        T0.C_SLOTS_C,
        T0.C_SLOTS_B,
        T0.C_DATE_REP,
        T0.C_STAVKA,
        T0.SLOT_LENGTH,
        T0.VISITS_ABSENCE
    FROM
        (
            SELECT
                V.VALIDATION_ID AS C_ID,
                V.REPORT_REGION_NAME AS C_REG,
                V.REPORT_DATE AS C_DATE,
                REPLACE(RR.MO_OID,
                ' ',
                '') AS MO_OID,
                REPLACE(RR.SP_OID,
                ' ',
                '') AS SP_OID,
                RR.MP_DOLGNOST,
                RR.MP_ID,
                RR.MP_FIO,
                CASE
                    WHEN RR.SLOTS_TYPE IN (
                        SELECT
                            ORIGIN_SLOTS_TYPE
                        FROM
                            FLK.DIRECTORY_SLOTS
                        WHERE
                            SLOTS_ID = 1
                    ) THEN
                        'доступен'
                    WHEN RR.SLOTS_TYPE IN (
                        SELECT
                            ORIGIN_SLOTS_TYPE
                        FROM
                            FLK.DIRECTORY_SLOTS
                        WHERE
                            SLOTS_ID = 2
                    ) THEN
                        'недоступен'
                    ELSE
                        'нераспознан'
                END AS SLOTS_TYPE,
                CASE
                    WHEN RR.SLOTS_CREATED ~ '[0-9]+$' THEN
                        CAST(RR.SLOTS_CREATED AS DECIMAL(10, 0))
                    WHEN REPLACE(RR.SLOTS_CREATED, ',', '.') ~ '[0-9]+.[0]+$' THEN
                        CAST(REPLACE(RR.SLOTS_CREATED, ',', '.') AS DECIMAL(10, 0))
                    ELSE
                        0
                END AS C_SLOTS_C,
                CASE
                    WHEN RR.SLOTS_BOOKED ~ '^[0-9]+$' THEN
                        CAST(RR.SLOTS_BOOKED AS DECIMAL(10, 0))
                    WHEN REPLACE(RR.SLOTS_BOOKED, ',', '.') ~ '[0-9]+.[0]+$' THEN
                        CAST(REPLACE(RR.SLOTS_BOOKED, ',', '.') AS DECIMAL(10, 0))
                    ELSE
                        0
                END AS C_SLOTS_B,
                CASE
                    WHEN LEFT(RR.DATE_REPORT, 10) ~ '^[0-3][0-9].[0-1][0-9].[2][0][2][3-9]$' THEN
                        TO_DATE(RR.DATE_REPORT, 'dd.mm.yyyy')
                    WHEN LEFT(RR.DATE_REPORT, 10) ~ '^[2][0][2][3-9]-[0-1][0-9]-[0-3][0-9]$' THEN
                        TO_DATE(RR.DATE_REPORT, 'yyyy-mm-dd')
                    ELSE
                        TO_DATE('1900-01-01', 'yyyy-mm-dd')
                END AS C_DATE_REP,
                CASE
                    WHEN REPLACE(RR.MP_STAVKA, ',', '') ~ '^[0]+$' THEN
                        1.000
                    WHEN REPLACE(RR.MP_STAVKA, '.', '') ~ '^[0]+$' THEN
                        1.000
                    WHEN REPLACE(RR.MP_STAVKA, ',', '.') ~ '^[0-9]+.[0-9]+$' THEN
                        CAST(REPLACE(RR.MP_STAVKA, ',', '.') AS DECIMAL(10, 3))
                    WHEN REPLACE(RR.MP_STAVKA, ',', '.') ~ '^.[0-9]+$' THEN
                        CAST(REPLACE(CONCAT('0', RR.MP_STAVKA), ',', '.') AS DECIMAL(10, 3))
                    WHEN RR.MP_STAVKA ~ '^[1-9][0-9]+$' THEN
                        CAST(RR.MP_STAVKA AS DECIMAL(10, 3))
                    WHEN RR.MP_STAVKA ~ '^[0]+[1-9]+$' THEN
                        CAST(REPLACE(RR.MP_STAVKA, '0', '') AS DECIMAL(10, 3))
                    WHEN RR.MP_STAVKA ~ '^[0-9]+$' THEN
                        CAST(RR.MP_STAVKA AS DECIMAL(10, 3))
                    WHEN RR.MP_STAVKA ~ '^[0]+$' THEN
                        1.000
                    WHEN RR.MP_STAVKA IS NULL THEN
                        1.000
                    WHEN RR.MP_STAVKA = '' THEN
                        1.000
                    ELSE
                        1.000
                END AS C_STAVKA,
                CASE
                    WHEN REPLACE (RR.SLOT_LENGTH, ',', '.') ~ '^[0-9]+.[0-9]+$' THEN
                        CAST(REPLACE (RR.SLOT_LENGTH, ',', '.') AS DECIMAL(10, 0))
                    WHEN RR.SLOT_LENGTH ~ '^[0-9]+$' THEN
                        CAST(RR.SLOT_LENGTH AS DECIMAL(10, 0))
                    ELSE
                        0
                END AS SLOT_LENGTH,
                CASE
                    WHEN RR.VISITS_ABSENCE ~ '^[0-9]+$' THEN
                        CAST(RR.VISITS_ABSENCE AS DECIMAL(10, 0))
                    WHEN REPLACE(RR.VISITS_ABSENCE, ',', '.') ~ '^[0-9]+.[0]+$' THEN
                        CAST(REPLACE(RR.VISITS_ABSENCE, ',', '.') AS DECIMAL(10, 0))
                    ELSE
                        0
                END AS VISITS_ABSENCE
            FROM
                FLK.REPORT_ROWS RR
                INNER JOIN FLK.VALIDATIONS V
                ON RR.VALIDATION_ID = V.VALIDATION_ID
            WHERE
                V.REPORT_ERRORS IS NULL
                AND RR.MO_DEPT_NAME = 'Органы исполнительной власти субъектов Российской Федерации, осуществляющие функции в области здравоохранения'
                AND RR.SP_DEPART_TYPE_NAME = 'Амбулаторный'
                AND V.REPORT_REGION_NAME IS NOT NULL
                AND V.VALIDATION_ID IN ( 11098, 11101, 11102, 11103, 11104, 11105, 11106, 11107, 11108, 11111, 11125, 11136, 11140, 11143, 11144, 11145, 11146, 11147, 11148, 11149, 11150, 11151, 11152, 11153, 11154, 11155, 11156, 11157, 11158, 11159, 11160, 11161, 11162, 11163, 11164, 11165, 11166, 11167, 11172, 11179, 11184, 11287, 11288, 11289, 11290, 11292, 11294, 11296, 11297, 11298, 11286, 11299, 11280, 11281, 11282, 11283, 11285, 11300, 11284, 11301 )
        ) AS T0
    WHERE
        T0.C_DATE_REP < T0.C_DATE
        AND T0.C_DATE_REP >= T0.C_DATE - 14
)
 --SELECT
 --*
 --FROM
 --T_MAIN
 --WHERE
 --MO_OID = '1.2.643.5.1.13.13.12.2.33.3018';
 --РАСЧЁТНАЯ ЧАСТЬ
SELECT
    DISTINCT TM.C_ID,
    TM.C_DATE,
    TM.C_REG,
    TM.MO_OID,
    TM.SP_OID,
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
            TM.SP_OID,
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
            TM.SP_OID,
            TM.MP_DOLGNOST
    ) AS TM0
    ON TM.C_REG = TM0.C_REG
    AND TM.MO_OID = TM0.MO_OID
    AND TM.SP_OID = TM0.SP_OID
    AND TM.MP_DOLGNOST = TM0.MP_DOLGNOST
    LEFT JOIN (
        SELECT
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.SP_OID,
            TM.MP_DOLGNOST,
            SUM(TM.C_SLOTS_C) AS C_KONK_SLOTS_ALL,
            SUM(TM.C_SLOTS_B) AS C_KONK_SLOTS_BOOKED
        FROM
            T_MAIN TM
        WHERE
            TM.SLOTS_TYPE = 'доступен'
        GROUP BY
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.SP_OID,
            TM.MP_DOLGNOST
    ) AS TM1
    ON TM.C_REG = TM1.C_REG
    AND TM.MO_OID = TM1.MO_OID
    AND TM.SP_OID = TM1.SP_OID
    AND TM.MP_DOLGNOST = TM1.MP_DOLGNOST
    LEFT JOIN (
        SELECT
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.SP_OID,
            TM.MP_DOLGNOST,
            SUM(TM.C_SLOTS_C) AS C_NEKONK_SLOTS_ALL,
            SUM(TM.C_SLOTS_B) AS C_NEKONK_SLOTS_BOOKED
        FROM
            T_MAIN TM
        WHERE
            TM.SLOTS_TYPE = 'недоступен'
        GROUP BY
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.SP_OID,
            TM.MP_DOLGNOST
    ) AS TM2
    ON TM.C_REG = TM2.C_REG
    AND TM.MO_OID = TM2.MO_OID
    AND TM.SP_OID = TM2.SP_OID
    AND TM.MP_DOLGNOST = TM2.MP_DOLGNOST
    LEFT JOIN (
        SELECT
            TS.C_REG,
            TS.MO_OID,
            TS.SP_OID,
            TS.MP_DOLGNOST,
            SUM(TS.C_STAVKA) AS C_STAVKA_DAYS_SUM
        FROM
            (
                SELECT
                    DISTINCT TM.C_REG,
                    TM.MO_OID,
                    TM.SP_OID,
                    TM.MP_DOLGNOST,
                    MP_ID,
                    TM.MP_FIO,
                    TM.C_DATE_REP,
                    TM.C_STAVKA
                FROM
                    T_MAIN TM
            ) TS
        GROUP BY
            TS.C_REG,
            TS.MO_OID,
            TS.SP_OID,
            TS.MP_DOLGNOST
    ) AS TM3
    ON TM.C_REG = TM3.C_REG
    AND TM.MO_OID = TM3.MO_OID
    AND TM.SP_OID = TM3.SP_OID
    AND TM.MP_DOLGNOST = TM3.MP_DOLGNOST
    LEFT JOIN (
        SELECT
            TS.C_REG,
            TS.MO_OID,
            TS.SP_OID,
            TS.MP_DOLGNOST,
            SUM(TS.C_STAVKA) AS C_STAVKA_SUM
        FROM
            (
                SELECT
                    DISTINCT TM.C_REG,
                    TM.MO_OID,
                    TM.SP_OID,
                    TM.MP_DOLGNOST,
                    MP_ID,
                    TM.MP_FIO,
                    TM.C_STAVKA
                FROM
                    T_MAIN TM
            ) TS
        GROUP BY
            TS.C_REG,
            TS.MO_OID,
            TS.SP_OID,
            TS.MP_DOLGNOST
    ) AS TM4
    ON TM.C_REG = TM4.C_REG
    AND TM.MO_OID = TM4.MO_OID
    AND TM.SP_OID = TM4.SP_OID
    AND TM.MP_DOLGNOST = TM4.MP_DOLGNOST
    LEFT JOIN (
        SELECT
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.SP_OID,
            TM.MP_DOLGNOST,
            SUM(TM.VISITS_ABSENCE) AS C_ABSENCE_COUNT
        FROM
            T_MAIN TM
        GROUP BY
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.SP_OID,
            TM.MP_DOLGNOST
    ) AS TM5
    ON TM.C_REG = TM5.C_REG
    AND TM.MO_OID = TM5.MO_OID
    AND TM.SP_OID = TM5.SP_OID
    AND TM.MP_DOLGNOST = TM5.MP_DOLGNOST
    LEFT JOIN (
        SELECT
            TM.C_ID,
            TM.C_REG,
            TM.C_DATE,
            TM.MO_OID,
            TM.SP_OID,
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
            TM.SP_OID,
            TM.MP_DOLGNOST
    ) AS TM6
    ON TM.C_REG = TM6.C_REG
    AND TM.MO_OID = TM6.MO_OID
    AND TM.SP_OID = TM6.SP_OID
    AND TM.MP_DOLGNOST = TM6.MP_DOLGNOST;