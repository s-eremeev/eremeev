with
	select_flag(flag_) as (values(0)), 							-- выбор расчётной даты для каждого региона: 0 - последняя дата, 1 - предпоследняя дата
	border_days(i, days) as (values (0, 7), (1, 4), (2, 10)), 	-- "отступы": max дней от даты дайджеста до даты последнего отчёта (7), границы интервала для даты предпоследнего отчёта (от 4 до 10)
	min_slot_length(mm) as (values(5)),							-- продолжительность "короткого" слота, мин
	t_MAIN as
	-- Отбор данных за 14 дней
	(select c_ID, c_REG, c_DATE, mo_oid, mo_short_name, sp_oid, sp_name, mp_dolgnost, mp_fio, c_DATE_REP, slots_type, c_SLOTS_C, c_SLOTS_B, c_STAVKA, slot_length,  visits_absence from
		-- Отбор из основной таблицы
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
				end as c_STAVKA,
		 		case
		 			when report_rows.slot_length ~ '^[0-9]+.[0-9]+$' then cast(report_rows.slot_length as decimal(10, 0))
		 			else 0
		 		end as slot_length, -- поля, которых у Морева нет
		 		case
		 			when report_rows.visits_absence ~ '^[0-9]+$' then cast(report_rows.visits_absence as int)
		 			else 0
		 		end as visits_absence
		from flk.report_rows
		 
		 right join		-- фильтрует основную таблицу по отобранным ниже ID
			 (select
				case
					when flag_ = 0 then v1
					else v2
				end as c_ID,
				case
					when flag_ = 0 then date_1
					else date_2
				end as c_DATE,
				report_region_name as c_REG
			from
			(select  max(validation_id) as v2 , max (q2.report_date) as date_2, v1, date_1, q2.report_region_name
				from flk.validations as q2
				right join 
					(select max(validation_id) as v1, max(report_date) as date_1, report_region_id
					from flk.validations, border_days
					where report_region_id is not null 
					  and report_region_name is not null 
					  and report_errors is null 
					  and report_date >= current_date - (select days from border_days where i = 0)
					group by report_region_id)  as q1 
				on q1.report_region_id = q2.report_region_id
				where q2.validation_id < v1 
				 and q2.report_date <> date_1 
				 and date_1 - (select days from border_days where i = 1)  >= q2.report_date  
				 and q2.report_date>= date_1 - (select days from border_days where i = 2)
				 and report_errors is null
				group by v1, q2.report_region_name, date_1
				order by report_region_name)as actual_data, select_flag, border_days) as t_ID_REG
				on t_ID_REG.c_ID = flk.report_rows.validation_id
		 
		where sp_depart_type_name = 'Амбулаторный'
		and mo_dept_name = 'Органы исполнительной власти субъектов Российской Федерации, осуществляющие функции в области здравоохранения'
		and (left(date_report, 10) ~ '^[2][0][2][3-9]-[0-1][0-9]-[0-3][0-9]$' or left(date_report, 10) ~ '^[0-3][0-9].[0-1][0-9].[2][0][2][3-9]$')
		and slots_created ~ '^[0-9]+$' and slots_booked ~ '^[0-9]+$'
		) as t_MAIN_tmp
	where c_DATE_REP <= c_DATE and c_DATE_REP > c_DATE-14)

-- Собственно формирование выгрузки из ГЛАВНОЙ ТАБЛИЦЫ с приджойниванием столбцов
select c_ID, c_DATE, c_REG_0, mo_oid_0, mo_short_name_0, sp_oid_0, sp_name_0, mp_dolgnost_0,
		COALESCE (c_SLOTS_ALL, 0) as c_SLOTS_ALL,
		COALESCE (c_SLOTS_BOOKED, 0) as c_SLOTS_BOOKED,
		COALESCE (c_KONK_SLOTS_ALL, 0) as c_KONK_SLOTS_ALL,
		COALESCE (c_KONK_SLOTS_BOOKED, 0) as c_KONK_SLOTS_BOOKED,
		COALESCE (c_NEKONK_SLOTS_ALL, 0) as c_NEKONK_SLOTS_ALL,
		COALESCE (c_NEKONK_SLOTS_BOOKED, 0) as c_NEKONK_SLOTS_BOOKED,
		COALESCE (c_STAVKA_sum, 0) as c_STAVKA_sum,
		COALESCE (c_STAVKA_DAYS_sum, 0) as c_STAVKA_DAYS_sum,
		COALESCE (ABSENCE_COUNT, 0) as ABSENCE_COUNT,
		COALESCE (SHORT_SLOTS_COUNT, 0) as SHORT_SLOTS_COUNT
	from
		(select c_ID, c_DATE, c_REG as c_REG_0, mo_oid as mo_oid_0, mo_short_name as mo_short_name_0, sp_oid as sp_oid_0, sp_name as sp_name_0, mp_dolgnost as mp_dolgnost_0,
		sum(c_SLOTS_C) as c_SLOTS_ALL, sum(c_SLOTS_B) as c_SLOTS_BOOKED from t_MAIN
		group by c_ID, c_REG, c_DATE, mo_oid, mo_short_name, sp_oid, sp_name, mp_dolgnost) as t_SLOTS

	-- Приджойниваем конкурентные слоты всего и занятые
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

	-- И также неконкурентные
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

	-- Наконец, отбираем уникальных врачей с уникальной ставкой, выводим по ним ставку и считаем ставкодни
	full join
		(select c_REG as c_REG_3, mo_oid as mo_oid_3, mo_short_name as mo_short_name_3, sp_oid as sp_oid_3, sp_name as sp_name_3, mp_dolgnost as mp_dolgnost_3,
		 		sum(c_STAVKA) as c_STAVKA_sum, sum(c_STAVKA_DAYS) as c_STAVKA_DAYS_sum from
			(select c_REG, mo_oid, mo_short_name, sp_oid, sp_name, mp_dolgnost, mp_fio, c_STAVKA, c_STAVKA * c_DATE_REP_count as c_STAVKA_DAYS from
				(select c_REG, mo_oid, mo_short_name, sp_oid, sp_name, mp_dolgnost, mp_fio, c_STAVKA, count(c_DATE_REP) as c_DATE_REP_count from
					(select distinct c_REG, mo_oid, mo_short_name, sp_oid, sp_name, mp_dolgnost, mp_fio, c_DATE_REP, c_STAVKA from t_MAIN) as t_UNIC_1
				group by c_REG, mo_oid, mo_short_name, sp_oid, sp_name, mp_dolgnost, mp_fio, c_STAVKA) as t_UNIC_2
			) as t_UNIC_3
		group by c_REG, mo_oid, mo_short_name, sp_oid, sp_name, mp_dolgnost) as t_SD
	on t_SLOTS.c_REG_0 = t_SD.c_REG_3
	and t_SLOTS.mo_oid_0 = t_SD.mo_oid_3
	and t_SLOTS.mo_short_name_0 = t_SD.mo_short_name_3
	and t_SLOTS.sp_oid_0 = t_SD.sp_oid_3
	and t_SLOTS.sp_name_0 = t_SD.sp_name_3
	and t_SLOTS.mp_dolgnost_0 = t_SD.mp_dolgnost_3
	
	-- А равно и число неявок
	full join
		(select c_REG as c_REG_4, mo_oid as mo_oid_4, mo_short_name as mo_short_name_4, sp_oid as sp_oid_4, sp_name as sp_name_4, mp_dolgnost as mp_dolgnost_4,
		sum(visits_absence) as ABSENCE_COUNT from t_MAIN
		group by c_REG, mo_oid, mo_short_name, sp_oid, sp_name, mp_dolgnost) as t_ABSENCE_COUNT
	on t_SLOTS.c_REG_0 = t_ABSENCE_COUNT.c_REG_4
	and t_SLOTS.mo_oid_0 = t_ABSENCE_COUNT.mo_oid_4
	and t_SLOTS.mo_short_name_0 = t_ABSENCE_COUNT.mo_short_name_4
	and t_SLOTS.sp_oid_0 = t_ABSENCE_COUNT.sp_oid_4
	and t_SLOTS.sp_name_0 = t_ABSENCE_COUNT.sp_name_4
	and t_SLOTS.mp_dolgnost_0 = t_ABSENCE_COUNT.mp_dolgnost_4
	
		-- Также количество слотов с длиной меньше 5 мин
	full join
		(select c_REG as c_REG_5, mo_oid as mo_oid_5, mo_short_name as mo_short_name_5, sp_oid as sp_oid_5, sp_name as sp_name_5, mp_dolgnost as mp_dolgnost_5,
		count(slot_length) as SHORT_SLOTS_COUNT from t_MAIN, min_slot_length
		where slot_length < mm
		group by c_REG, mo_oid, mo_short_name, sp_oid, sp_name, mp_dolgnost) as t_SHORT_SLOTS_COUNT
	on t_SLOTS.c_REG_0 = t_SHORT_SLOTS_COUNT.c_REG_5
	and t_SLOTS.mo_oid_0 = t_SHORT_SLOTS_COUNT.mo_oid_5
	and t_SLOTS.mo_short_name_0 = t_SHORT_SLOTS_COUNT.mo_short_name_5
	and t_SLOTS.sp_oid_0 = t_SHORT_SLOTS_COUNT.sp_oid_5
	and t_SLOTS.sp_name_0 = t_SHORT_SLOTS_COUNT.sp_name_5
	and t_SLOTS.mp_dolgnost_0 = t_SHORT_SLOTS_COUNT.mp_dolgnost_5
				
	-- для тестирования, потом убрать
	--where mo_oid_0 = '1.2.643.5.1.13.13.12.2.28.2675'
	--and sp_oid_0 = '1.2.643.5.1.13.13.12.2.28.2675.0.237138'
	--and mp_dolgnost_0 = 'врач-терапевт участковый';
	-- убрать до этого места;
	
	
	
	
-- ОСНОВНАЯ ТАБЛИЦА
with
	select_flag(flag_) as (values(0)), 							-- выбор расчётной даты для каждого региона: 0 - последняя дата, 1 - предпоследняя дата
	border_days(i, days) as (values (0, 7), (1, 4), (2, 10)), 	-- "отступы": max дней от даты дайджеста до даты последнего отчёта (7), границы интервала для даты предпоследнего отчёта (от 4 до 10)
	min_slot_length(mm) as (values(5)),							-- продолжительность "короткого" слота, мин
	t_MAIN as
	-- Отбор данных за 14 дней
	(select c_ID, c_REG, c_DATE, mo_oid, mo_short_name, sp_oid, sp_name, mp_dolgnost, mp_fio, c_DATE_REP, slots_type, c_SLOTS_C, c_SLOTS_B, c_STAVKA, slot_length,  visits_absence from
		-- Отбор из основной таблицы
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
				end as c_STAVKA,
		 		case
		 			when report_rows.slot_length ~ '^[0-9]+.[0-9]+$' then cast(report_rows.slot_length as decimal(10, 0))
		 			else 0
		 		end as slot_length, -- поля, которых у Морева нет
		 		case
		 			when report_rows.visits_absence ~ '^[0-9]+$' then cast(report_rows.visits_absence as int)
		 			else 0
		 		end as visits_absence
		from flk.report_rows
		 
		 right join		-- фильтрует основную таблицу по отобранным ниже ID
			 (select
				case
					when flag_ = 0 then v1
					else v2
				end as c_ID,
				case
					when flag_ = 0 then date_1
					else date_2
				end as c_DATE,
				report_region_name as c_REG
			from
			(select  max(validation_id) as v2 , max (q2.report_date) as date_2, v1, date_1, q2.report_region_name
				from flk.validations as q2
				right join 
					(select max(validation_id) as v1, max(report_date) as date_1, report_region_id
					from flk.validations, border_days
					where report_region_id is not null 
					  and report_region_name is not null 
					  and report_errors is null 
					  and report_date >= current_date - (select days from border_days where i = 0)
					group by report_region_id)  as q1 
				on q1.report_region_id = q2.report_region_id
				where q2.validation_id < v1 
				 and q2.report_date <> date_1 
				 and date_1 - (select days from border_days where i = 1)  >= q2.report_date  
				 and q2.report_date>= date_1 - (select days from border_days where i = 2)
				 and report_errors is null
				group by v1, q2.report_region_name, date_1
				order by report_region_name)as actual_data, select_flag, border_days) as t_ID_REG
				on t_ID_REG.c_ID = flk.report_rows.validation_id
		 
		where sp_depart_type_name = 'Амбулаторный'
		and mo_dept_name = 'Органы исполнительной власти субъектов Российской Федерации, осуществляющие функции в области здравоохранения'
		and (left(date_report, 10) ~ '^[2][0][2][3-9]-[0-1][0-9]-[0-3][0-9]$' or left(date_report, 10) ~ '^[0-3][0-9].[0-1][0-9].[2][0][2][3-9]$')
		and slots_created ~ '^[0-9]+$' and slots_booked ~ '^[0-9]+$'
		) as t_MAIN_tmp
	where c_DATE_REP <= c_DATE and c_DATE_REP > c_DATE-14)
select *
from t_MAIN
where c_reg = 'Амурская область';