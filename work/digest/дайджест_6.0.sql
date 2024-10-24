WITH
 -- ВЫБОР ПЕРИОДА ВЫГРУЗКИ: 0 - ОТЧЁТНЫЙ, 1 - КОНТРОЛЬНЫЙ
SELECT_FLAG(FLAG_) AS (VALUES(0)),
 -- ГРАНИЦЫ ПЕРИОДОВ: КОНТРОЛЬНЫЙ, ОТЧЁТНЫЙ (А НЕ НАОБОРОТ!)
BORDERS(I, D) AS (VALUES (0, '2024-02-05'), (1, '2024-02-11'), (2, '2024-02-12'), (3, '2024-02-18')),
 -- КОЛИЧЕСТВО ДНЕЙ С РАСПИСАНИЕМ В ЗАКРЫТОМ ПЕРИОДЕ (0), МАКСИМАЛЬНЫЙ ВРЕМЕННОЙ ЛАГ МЕЖДУ ДАТОЙ ОТЧЁТА И ПЕРВЫМ ДНЁМ С РАСПИСАНИЕМ (1)
LIMITS(I, VAL) AS (VALUES (0, 0), (1, 15)),
 -- СКОЛЬКО ДНЕЙ БРАТЬ ДЛЯ РАСЧЁТОВ:
SLONS_DAYS_COUNT(DAYS) AS (VALUES(14)),
 -- ПРОДОЛЖИТЕЛЬНОСТЬ "КОРОТКОГО СЛОТА", МИН:
MIN_SLOT_LENGTH(MM) AS (VALUES(5)), T_BIG AS(
    SELECT
        V.VALIDATION_ID,
        V.REPORT_REGION_NAME,
        V.REPORT_DATE,
        REPLACE(RR.MO_OID,
        ' ',
        '') AS MO_OID,
        REPLACE(RR.SP_OID,
        ' ',
        '') AS SP_OID,
        RR.MP_DOLGNOST,
        RR.MP_ID,
        REPLACE(LOWER(RR.MP_FIO),
        '  ',
        ' ') AS MP_FIO,
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
                'не распознан'
        END AS SLOTS_TYPE,
        CASE
            WHEN RR.SLOTS_CREATED ~ '[0-9]+$' THEN
                CAST(RR.SLOTS_CREATED AS DECIMAL(10, 0))
            WHEN REPLACE(RR.SLOTS_CREATED, ',', '.') ~ '[0-9]+.[0]+$' THEN
                CAST(REPLACE(RR.SLOTS_CREATED, ',', '.') AS DECIMAL(10, 0))
            ELSE
                0
        END AS SLOTS_C,
        CASE
            WHEN RR.SLOTS_BOOKED ~ '^[0-9]+$' THEN
                CAST(RR.SLOTS_BOOKED AS DECIMAL(10, 0))
            WHEN REPLACE(RR.SLOTS_BOOKED, ',', '.') ~ '[0-9]+.[0]+$' THEN
                CAST(REPLACE(RR.SLOTS_BOOKED, ',', '.') AS DECIMAL(10, 0))
            ELSE
                0
        END AS SLOTS_B,
        CASE
            WHEN LEFT(RR.DATE_REPORT, 10) ~ '^[0-3][0-9].[0-1][0-9].[2][0][2][3-9]$' THEN
                TO_DATE(RR.DATE_REPORT, 'dd.mm.yyyy')
            WHEN LEFT(RR.DATE_REPORT, 10) ~ '^[2][0][2][3-9]-[0-1][0-9]-[0-3][0-9]$' THEN
                TO_DATE(RR.DATE_REPORT, 'yyyy-mm-dd')
            ELSE
                TO_DATE('1900-01-01', 'yyyy-mm-dd')
        END AS DATE_SLOT,
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
        END AS STAVKA,
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
        AND V.REPORT_REGION_NAME IS NOT NULL
        AND RR.MO_DEPT_NAME = 'Органы исполнительной власти субъектов Российской Федерации, осуществляющие функции в области здравоохранения'
        AND RR.SP_DEPART_TYPE_NAME = 'Амбулаторный'
        AND V.REPORT_DATE BETWEEN CAST((
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
                I = 3
        ) AS DATE)
), T_CNT_LIMIT AS(
    SELECT
        T_C.VALIDATION_ID,
        T_C.REPORT_REGION_NAME,
        T_C.REPORT_DATE,
        T_C.MO_OID,
        T_C.SP_OID,
        T_C.MP_DOLGNOST,
        T_C.MP_ID,
        T_C.MP_FIO,
        T_C.SLOTS_TYPE,
        T_C.SLOTS_C,
        T_C.SLOTS_B,
        T_C.DATE_SLOT,
        T_C.STAVKA,
        T_C.SLOT_LENGTH,
        T_C.VISITS_ABSENCE
    FROM
        (
            SELECT
                TB_1.VALIDATION_ID,
                TB_1.REPORT_REGION_NAME,
                TB_1.REPORT_DATE,
                TB_1.MO_OID,
                TB_1.SP_OID,
                TB_1.MP_DOLGNOST,
                TB_1.MP_ID,
                TB_1.MP_FIO,
                TB_1.SLOTS_TYPE,
                TB_1.SLOTS_C,
                TB_1.SLOTS_B,
                TB_1.DATE_SLOT,
                TB_1.STAVKA,
                TB_1.SLOT_LENGTH,
                TB_1.VISITS_ABSENCE,
                MAX(TB_1.SLOTS_DAYS_RANK) OVER(PARTITION BY TB_1.VALIDATION_ID) AS SLOTS_DAYS_COUNT,
                TB_1.REPORT_DATE - TB_1.MIN_DATE_SLOT AS MAX_SLOT_LAG
            FROM
                (
                    SELECT
                        TB.VALIDATION_ID,
                        TB.REPORT_REGION_NAME,
                        TB.REPORT_DATE,
                        TB.MO_OID,
                        TB.SP_OID,
                        TB.MP_DOLGNOST,
                        TB.MP_ID,
                        TB.MP_FIO,
                        TB.SLOTS_TYPE,
                        TB.SLOTS_C,
                        TB.SLOTS_B,
                        TB.DATE_SLOT,
                        TB.STAVKA,
                        TB.SLOT_LENGTH,
                        TB.VISITS_ABSENCE,
                        DENSE_RANK() OVER(PARTITION BY TB.VALIDATION_ID ORDER BY TB.DATE_SLOT) AS SLOTS_DAYS_RANK,
                        MIN(TB.DATE_SLOT) OVER(PARTITION BY TB.VALIDATION_ID) AS MIN_DATE_SLOT
                    FROM
                        T_BIG TB,
                        SLONS_DAYS_COUNT
                    WHERE
                        TB.DATE_SLOT < TB.REPORT_DATE
                        AND TB.DATE_SLOT >= TB.REPORT_DATE - DAYS
                ) AS TB_1
        ) AS T_C
    WHERE
        T_C.SLOTS_DAYS_COUNT >= (
            SELECT
                VAL
            FROM
                LIMITS
            WHERE
                I = 0
        )
        AND T_C.MAX_SLOT_LAG <= (
            SELECT
                VAL
            FROM
                LIMITS
            WHERE
                I = 1
        )
), T_SELECT_ID AS(
    SELECT
        T_S.REPORT_REGION_NAME AS C_REG,
        CASE
            WHEN FLAG_ = 0 THEN
                T_S.MAX_ID_0
            ELSE
                T_S.MAX_ID_1
        END AS C_ID,
        CASE
            WHEN FLAG_ = 0 THEN
                T_S.MAX_DATE_0
            ELSE
                T_S.MAX_DATE_1
        END AS C_DATE
    FROM
        (
            SELECT
                DISTINCT T_CNT_LIMIT.REPORT_REGION_NAME,
                MAX(T_CNT_LIMIT.REPORT_DATE) OVER (PARTITION BY T_CNT_LIMIT.REPORT_REGION_NAME) MAX_DATE_0,
                MAX(T_CNT_LIMIT.VALIDATION_ID) OVER (PARTITION BY T_CNT_LIMIT.REPORT_REGION_NAME) MAX_ID_0,
                T_CNT_LIMIT_1.MAX_DATE_1,
                T_CNT_LIMIT_1.MAX_ID_1
            FROM
                T_CNT_LIMIT
                INNER JOIN (
                    SELECT
                        DISTINCT T_CNT_LIMIT.REPORT_REGION_NAME,
                        MAX(T_CNT_LIMIT.REPORT_DATE) OVER (PARTITION BY T_CNT_LIMIT.REPORT_REGION_NAME) MAX_DATE_1,
                        MAX(T_CNT_LIMIT.VALIDATION_ID) OVER (PARTITION BY T_CNT_LIMIT.REPORT_REGION_NAME) MAX_ID_1
                    FROM
                        T_CNT_LIMIT
                    WHERE
                        T_CNT_LIMIT.REPORT_DATE BETWEEN CAST((
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
                ) AS T_CNT_LIMIT_1
                ON T_CNT_LIMIT.REPORT_REGION_NAME = T_CNT_LIMIT_1.REPORT_REGION_NAME
            WHERE
                T_CNT_LIMIT.REPORT_DATE BETWEEN CAST((
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
        ) AS T_S,
        SELECT_FLAG
), T_MAIN AS(
    SELECT
        T_SL.VALIDATION_ID AS C_ID,
        T_SL.REPORT_REGION_NAME AS C_REG,
        T_SL.REPORT_DATE AS C_DATE,
        T_SL.MO_OID AS MO_OID,
        T_SL.SP_OID AS SP_OID,
        T_SL.MP_DOLGNOST AS MP_DOLGNOST,
        T_SL.MP_ID AS MP_ID,
        T_SL.MP_FIO AS MP_FIO,
        T_SL.DATE_SLOT AS C_DATE_REP,
        T_SL.SLOTS_TYPE AS SLOTS_TYPE,
        T_SL.SLOTS_C AS C_SLOTS_C,
        T_SL.SLOTS_B AS C_SLOTS_B,
        T_SL.STAVKA AS C_STAVKA,
        T_SL.SLOT_LENGTH AS SLOT_LENGTH,
        T_SL.VISITS_ABSENCE AS VISITS_ABSENCE
    FROM
        T_CNT_LIMIT T_SL
        RIGHT JOIN T_SELECT_ID T_SI
        ON T_SL.VALIDATION_ID = T_SI.C_ID
        AND T_SL.REPORT_REGION_NAME = T_SI.C_REG
        AND T_SL.REPORT_DATE = T_SI.C_DATE
)
 -- МЕСТО, ГДЕ МОЖНО ПИСАТЬ "ТЕХНИЧЕСКИЕ" ЗАПРОСЫ К СТЕ
 -- SELECT
 -- *
 --DISTINCT VALIDATION_ID
 -- FROM
 -- T_MAIN;
 --T_CNT
 --WHERE VALIDATION_ID = 11111
 --ORDER BY DATE_SLOT;
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
                    TM.MP_ID,
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
                    TM.MP_ID,
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


    --v_6.0
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
 --RR.MO_SHORT_NAME,
                REPLACE(RR.SP_OID,
                ' ',
                '') AS SP_OID,
 --RR.SP_NAME,
                RR.MP_DOLGNOST,
                RR.MP_ID,
                REPLACE(LOWER(RR.MP_FIO),
                '  ',
                ' ') AS MP_FIO,
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
 --AND V.REPORT_DATE BETWEEN CAST((SELECT D FROM BORDERS WHERE I = 0) AS DATE) AND CAST((SELECT D FROM BORDERS WHERE I = 3) AS DATE)
                AND V.VALIDATION_ID IN ( 11192, 11186, 11258, 11259, 11188, 11220, 11171, 11219, 11260, 11233, 11214, 11195, 11215, 11169, 11210, 11229, 11263, 11216, 11217, 11234, 11218, 11178, 11265, 11176, 11235, 11266, 11267, 11268, 11232, 11256, 11269, 11177, 11209, 11250, 11251, 11252, 11253, 11230, 11356, 11255, 11236, 11270, 11237, 11238, 11239, 11211, 11173, 11240, 11241, 11246, 11254, 11170, 11212, 11242, 11231, 11273, 11203 )
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
 -- 	DIR.NAMESHORT,
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

-- 	LEFT JOIN (
-- 		SELECT
-- 			directory_frmo.oid AS OID,
-- 			directory_frmo.nameShort AS NAMESHORT
-- 		FROM
-- 		FLK.DIRECTORY_FRMO) AS DIR
-- 	ON DIR.OID = TM.MO_OID;