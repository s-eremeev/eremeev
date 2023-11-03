SELECT *
FROM flk.report_rows rr
WHERE rr.excel_row_number in (12798,
                              12888,
                              14768,
                              14772)
  AND rr.validation_id =
    (SELECT v.validation_id
     FROM flk.validations v
     WHERE v.validation_filename = 'I38_SLOTS_68_2023_09_04_20230904_092432.csv')