WITH fn AS
  (SELECT v.validation_id
   FROM flk.validations v
   WHERE v.validation_filename = 'I38_SLOTS_78_17_10_2023_20231017_1152500.xlsx')
SELECT 'flk_01' AS flk,
       vd.flk_01_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_01_error_lines <> '{}'
UNION
SELECT 'flk_02' AS flk,
       vd.flk_02_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_02_error_lines <> '{}'
UNION
SELECT 'flk_03' AS flk,
       vd.flk_03_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_03_error_lines <> '{}'
UNION
SELECT 'flk_04' AS flk,
       vd.flk_04_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_04_error_lines <> '{}'
UNION
SELECT 'flk_05' AS flk,
       vd.flk_05_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_05_error_lines <> '{}'
UNION
SELECT 'flk_06' AS flk,
       vd.flk_06_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_06_error_lines <> '{}'
UNION
SELECT 'flk_07' AS flk,
       vd.flk_07_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_07_error_lines <> '{}'
UNION
SELECT 'flk_08' AS flk,
       vd.flk_08_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_08_error_lines <> '{}'
UNION
SELECT 'flk_09' AS flk,
       vd.flk_09_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_09_error_lines <> '{}'
UNION
SELECT 'flk_10' AS flk,
       vd.flk_10_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_10_error_lines <> '{}'
UNION
SELECT 'flk_11' AS flk,
       vd.flk_11_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_11_error_lines <> '{}'
UNION
SELECT 'flk_12' AS flk,
       vd.flk_12_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_12_error_lines <> '{}'
UNION
SELECT 'flk_13' AS flk,
       vd.flk_13_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_13_error_lines <> '{}'
UNION
SELECT 'flk_14' AS flk,
       vd.flk_14_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_14_error_lines <> '{}'
UNION
SELECT 'flk_15' AS flk,
       vd.flk_15_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_15_error_lines <> '{}'
UNION
SELECT 'flk_16' AS flk,
       vd.flk_16_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_16_error_lines <> '{}'
UNION
SELECT 'flk_17' AS flk,
       vd.flk_17_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_17_error_lines <> '{}'
UNION
SELECT 'flk_18' AS flk,
       vd.flk_18_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_18_error_lines <> '{}'
UNION
SELECT 'flk_19' AS flk,
       vd.flk_19_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_19_error_lines <> '{}'
UNION
SELECT 'flk_20' AS flk,
       vd.flk_20_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_20_error_lines <> '{}'
UNION
SELECT 'flk_21' AS flk,
       vd.flk_21_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_21_error_lines <> '{}'
UNION
SELECT 'flk_22' AS flk,
       vd.flk_22_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_22_error_lines <> '{}'
UNION
SELECT 'flk_23' AS flk,
       vd.flk_23_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_23_error_lines <> '{}'
UNION
SELECT 'flk_24' AS flk,
       vd.flk_24_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_24_error_lines <> '{}'
UNION
SELECT 'flk_25' AS flk,
       vd.flk_25_error_lines AS flk_lines
FROM flk.validation_details vd
WHERE vd.validation_id in
    (SELECT *
     FROM fn)
  AND vd.flk_25_error_lines <> '{}';