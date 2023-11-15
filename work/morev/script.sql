with t_MAIN as
	(select c_ID, c_REG, c_DATE, mo_oid, mo_short_name, sp_oid, sp_name, mp_dolgnost, mp_fio, c_DATE_REP, slots_type, c_SLOTS_C, c_SLOTS_B, c_STAVKA from
		(select c_ID, c_REG, c_DATE, mo_oid, mo_short_name, sp_oid, sp_name, mp_dolgnost, mp_fio, slots_type,
		 		cast(slots_created as decimal(10, 0)) as c_SLOTS_C, cast(slots_booked as decimal(10, 0)) as c_SLOTS_B,
				case
					when left(date_report, 10) ~ '^[0-3][0-9].[0-1][0-9].[2][0][2][3-9]$' then to_date(date_report, 'dd.mm.yyyy')
					when left(date_report, 10) ~ '^[2][0][2][3-9]-[0-1][0-9]-[0-3][0-9]$' then to_date(date_report, 'yyyy-mm-dd')
					else to_date('1900-01-01', 'yyyy-mm-dd')
				end as c_DATE_REP,
				case
					when replace(mp_stavka, ',', '.') ~ '^[0-9]+.[0-9]+$' then cast(replace(mp_stavka, ',', '.') as decimal(10, 3))
					else 0
				end as c_STAVKA
		from flk.report_rows right join
			(select validation_id as c_ID, report_region_name as c_REG, report_date as c_DATE from
				(select *, max(report_date) over (partition by report_region_name) as c_MAX_DATE from
				 	(select * from
						(select * from
							(select *, max(validation_timestamp) over (partition by report_region_name, report_date) as c_MAX_TIME from
								(select * from flk.validations
								where report_errors is null and validation_id is not null and report_region_name is not null) as t_NOERR
							) as t_MAX_TIME
						where validation_timestamp = c_MAX_TIME) as t_MAX_TIME_selected
				 	where report_date <= to_date('2023-11-01', 'yyyy-mm-dd') - 1 and report_date >= to_date('2023-11-01', 'yyyy-mm-dd') - 7) as t_RANGE_DATE
				) as t_MAX_DATE
			where report_date = c_MAX_DATE) as t_ID_REG
		on t_ID_REG.c_ID = flk.report_rows.validation_id
		where sp_depart_type_name = 'Амбулаторный'
		and mo_dept_name = 'Органы исполнительной власти субъектов Российской Федерации, осуществляющие функции в области здравоохранения'
		and (left(date_report, 10) ~ '^[2][0][2][3-9]-[0-1][0-9]-[0-3][0-9]$' or left(date_report, 10) ~ '^[0-3][0-9].[0-1][0-9].[2][0][2][3-9]$')
		and slots_created ~ '^[0-9]+$' and slots_booked ~ '^[0-9]+$'
		) as t_MAIN_tmp
	where c_DATE_REP <= c_DATE and c_DATE_REP > c_DATE-14)

select c_ID, c_DATE, c_REG_0, mo_oid_0, mo_short_name_0, sp_oid_0, sp_name_0, mp_dolgnost_0,
		c_SLOTS_ALL, c_SLOTS_BOOKED, c_KONK_SLOTS_ALL, c_KONK_SLOTS_BOOKED, c_NEKONK_SLOTS_ALL, c_NEKONK_SLOTS_BOOKED, c_STAVKA, c_STAVKA_DAYS_sum from
	(select c_ID, c_DATE, c_REG as c_REG_0, mo_oid as mo_oid_0, mo_short_name as mo_short_name_0, sp_oid as sp_oid_0, sp_name as sp_name_0, mp_dolgnost as mp_dolgnost_0,
	sum(c_SLOTS_C) as c_SLOTS_ALL, sum(c_SLOTS_B) as c_SLOTS_BOOKED from t_MAIN
	group by c_ID, c_REG, c_DATE, mo_oid, mo_short_name, sp_oid, sp_name, mp_dolgnost) as t_SLOTS

	full join
		(select c_REG as c_REG_1, mo_oid as mo_oid_1, mo_short_name as mo_short_name_1, sp_oid as sp_oid_1, sp_name as sp_name_1, mp_dolgnost as mp_dolgnost_1,
		sum(c_SLOTS_C) as c_KONK_SLOTS_ALL, sum(c_SLOTS_B) as c_KONK_SLOTS_BOOKED from t_MAIN
		where left(slots_type, 8) = 'Доступен'
		group by c_REG, mo_oid, mo_short_name, sp_oid, sp_name, mp_dolgnost) as t_KONK_SLOTS
	on t_SLOTS.c_REG_0 = t_KONK_SLOTS.c_REG_1
	and t_SLOTS.mo_oid_0 = t_KONK_SLOTS.mo_oid_1
	and t_SLOTS.mo_short_name_0 = t_KONK_SLOTS.mo_short_name_1
	and t_SLOTS.sp_oid_0 = t_KONK_SLOTS.sp_oid_1
	and t_SLOTS.sp_name_0 = t_KONK_SLOTS.sp_name_1
	and t_SLOTS.mp_dolgnost_0 = t_KONK_SLOTS.mp_dolgnost_1

	full join
		(select c_REG as c_REG_2, mo_oid as mo_oid_2, mo_short_name as mo_short_name_2, sp_oid as sp_oid_2, sp_name as sp_name_2, mp_dolgnost as mp_dolgnost_2,
		sum(c_SLOTS_C) as c_NEKONK_SLOTS_ALL, sum(c_SLOTS_B) as c_NEKONK_SLOTS_BOOKED from t_MAIN
		where left(slots_type, 10) = 'Недоступен' or left(slots_type, 11) = 'Не доступен'
		group by c_REG, mo_oid, mo_short_name, sp_oid, sp_name, mp_dolgnost) as t_NEKONK_SLOTS
	on t_SLOTS.c_REG_0 = t_NEKONK_SLOTS.c_REG_2
	and t_SLOTS.mo_oid_0 = t_NEKONK_SLOTS.mo_oid_2
	and t_SLOTS.mo_short_name_0 = t_NEKONK_SLOTS.mo_short_name_2
	and t_SLOTS.sp_oid_0 = t_NEKONK_SLOTS.sp_oid_2
	and t_SLOTS.sp_name_0 = t_NEKONK_SLOTS.sp_name_2
	and t_SLOTS.mp_dolgnost_0 = t_NEKONK_SLOTS.mp_dolgnost_2

	full join
		(select c_REG as c_REG_3, mo_oid as mo_oid_3, mo_short_name as mo_short_name_3, sp_oid as sp_oid_3, sp_name as sp_name_3, mp_dolgnost as mp_dolgnost_3,
		 		c_STAVKA, sum(c_STAVKA_DAYS) as c_STAVKA_DAYS_sum from
			(select c_REG, mo_oid, mo_short_name, sp_oid, sp_name, mp_dolgnost, mp_fio, c_STAVKA, c_STAVKA * c_DATE_REP_count as c_STAVKA_DAYS from
				(select c_REG, mo_oid, mo_short_name, sp_oid, sp_name, mp_dolgnost, mp_fio, c_STAVKA, count(c_DATE_REP) as c_DATE_REP_count from
					(select distinct c_REG, mo_oid, mo_short_name, sp_oid, sp_name, mp_dolgnost, mp_fio, c_DATE_REP, c_STAVKA from t_MAIN) as t_UNIC_1
				group by c_REG, mo_oid, mo_short_name, sp_oid, sp_name, mp_dolgnost, mp_fio, c_STAVKA) as t_UNIC_2
			) as t_UNIC_3
		group by c_REG, mo_oid, mo_short_name, sp_oid, sp_name, mp_dolgnost, c_STAVKA) as t_SD
	on t_SLOTS.c_REG_0 = t_SD.c_REG_3
	and t_SLOTS.mo_oid_0 = t_SD.mo_oid_3
	and t_SLOTS.mo_short_name_0 = t_SD.mo_short_name_3
	and t_SLOTS.sp_oid_0 = t_SD.sp_oid_3
	and t_SLOTS.sp_name_0 = t_SD.sp_name_3
	and t_SLOTS.mp_dolgnost_0 = t_SD.mp_dolgnost_3