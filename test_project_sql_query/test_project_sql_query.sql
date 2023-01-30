--Напишите SQL-запрос, выводящий 5 шаблонов, которые чаще всего применяются юзерами 2 и более раза подряд в течение одной сессии.

with upshot as
	(select *,
	count (value) over (partition by val.user_id) as cnt
	from (
		select res.user_id, res.value, res.value - lead (res.value) over (partition by res.user_id) as delta_value
		from (
			with tbl as
				(select *,
				lag (l.event_time) over (partition by l.user_id order by l.event_time) as lag_time
				from logs_5020 l)
			select *,
			cast ((JulianDay(tbl.event_time) - JulianDay(tbl.lag_time)) * 24 * 60 * 60 As Integer) as delta
			from tbl
			where tbl.event = '0' and delta < 300
		) as res
	) as val 
	where val.delta_value = 0)
select distinct upshot.value as 'шаблон', upshot.cnt as 'использовался подряд в течение сессии, раз'
from upshot
order by upshot.cnt desc
limit 5;