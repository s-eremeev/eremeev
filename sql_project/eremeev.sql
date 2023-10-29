-- 1. В каких городах больше одного аэропорта?
select apt.city ->> lang() as "город", apt.cnt as "количество аэропортов",
	string_agg (apt.airport_name ->> lang(), ', '
order by apt.airport_name ->> lang()
) as "аэропроты"
from
(select distinct a.city,
	count (a.airport_code) over (partition by a.city) as cnt,
	a.airport_name
from airports_data a)
as apt
where apt.cnt > 1
group by apt.city ->> lang
(), apt.cnt;


-- 2. В каких аэропортах есть рейсы, выполняемые самолетом с максимальной дальностью перелета?
	select ad.airport_name ->> lang() as "название аэропорта", 'аэропрот вылета' as "статус аэропорта"
	from airports_data ad
	where ad.airport_code in
	(select f.departure_airport
	from flights f
	where f.aircraft_code = 
		(select ad.aircraft_code
	from aircrafts_data ad
	where ad."range" = (select max(ad2."range")
	from aircrafts_data ad2)))
union all
	select ad.airport_name ->> lang() as "название аэропорта", 'аэропрот прибытия' as "статус аэропрота"
	from airports_data ad
	where ad.airport_code in
	(select f.arrival_airport
	from flights f
	where f.aircraft_code = 
		(select ad.aircraft_code
	from aircrafts_data ad
	where ad."range" = (select max(ad2."range")
	from aircrafts_data ad2)));

-- 3. Вывести 10 рейсов с максимальным временем задержки вылета.
select res."date" as "дата рейса", res.flight_no as "номер рейса", res.t as "время задержки вылета"
from
	(select f.scheduled_departure::date as "date", f.flight_no, coalesce (f.actual_departure - f.scheduled_departure, '0') as t,
		rank () over (order by coalesce (f.actual_departure - f.scheduled_departure, '0') desc)
	from flights f
	order by rank
	limit 10)
	as res;

-- 4. Были ли брони, по которым не были получены посадочные талоны?
select distinct res.status as "выдача посадочных талонов",
	count (*) over (partition by res.status) as "количество бронирований"
from
	(select distinct b.book_ref,
		case	when	count (t.ticket_no) over (partition by b.book_ref) <> 0
			and count (t.ticket_no) over (partition by b.book_ref) = count (bp.boarding_no) over (partition by b.book_ref)
					then 'выданы по всем билетам'
		when	count (t.ticket_no) over (partition by b.book_ref) <> 0
			and count (t.ticket_no) over (partition by b.book_ref) <> count (bp.boarding_no) over (partition by b.book_ref)
			and count (bp.boarding_no) over (partition by b.book_ref) <> 0
					then 'выданы не по всем билетам'
		else		'не выданы по всем билетам'
end "status"
	from bookings b
		join tickets t on t.book_ref = b.book_ref
		left join boarding_passes bp  using (ticket_no)) as res;

-- 5. Найдите количество свободных мест для каждого рейса, их % отношение к общему количеству мест в самолете.
-- Добавьте столбец с накопительным итогом - суммарное накопление количества вывезенных пассажиров из каждого аэропорта на каждый
-- день. Т.е. в этом столбце должна отражаться накопительная сумма - сколько человек уже вылетело из данного аэропорта на этом или
-- более ранних рейсах в течении дня.
select ad.airport_name ->> lang() as "аэропрот вылета",
	f.actual_departure::date as "дата вылета",
	f.flight_no as "№ рейса",
	cnt_seats.cnt_s - count (bp.seat_no) as "количество свободных мест",
	round ((cnt_seats.cnt_s - count (bp.seat_no)) * 100.0 / cnt_seats.cnt_s, 1) as "доля свободных мест, %",
	sum (count (bp.seat_no)) over (partition by f.departure_airport, f.actual_departure::date order by f.actual_departure) as "вылетело пассажиров за день"
from flights f
	join ticket_flights tf  using (flight_id)
	join boarding_passes bp  using (ticket_no, flight_id)
	join airports_data ad on ad.airport_code = f.departure_airport
	join (select s.aircraft_code, count (s.seat_no) as cnt_s
	from seats s
	group by s.aircraft_code) as cnt_seats
	on cnt_seats.aircraft_code = f.aircraft_code
group by ad.airport_name, f.actual_departure,  cnt_seats.cnt_s, f.flight_no, f.departure_airport
order by ad.airport_name ->> lang(), f.actual_departure::date;


-- 6. Найдите процентное соотношение перелетов по типам самолетов от общего количества.
with
	ratio
	as
	(
		select distinct f.aircraft_code,
			round ((count (f.flight_id) over (partition by f.aircraft_code)) *100.0 / (select count (*)
			from flights), 2)
		from flights f
	)
select ad.model ->> lang() as "модель самолёта", ratio."round" as "доля перелётов, %"
from ratio
	join aircrafts_data ad  using (aircraft_code)
order by ratio."round" desc;


-- 7. Были ли города, в которые можно добраться бизнес - классом дешевле, чем эконом-классом в рамках перелета?
select res.flight_id as "ID перелёта", res.flight_no as "№ рейса", res.d_f as "дата рейса", res.city as "город прибытия"
from
	(with amnt as
(select distinct tf.flight_id, f.flight_no, f.scheduled_arrival::date as d_f, ad.city ->> lang() as city, tf.fare_conditions, tf.amount,
	coalesce (max (tf.amount) filter (where tf.fare_conditions = 'Economy') over (partition by tf.flight_id, city, tf.fare_conditions), '0')
+ 
		coalesce
(min
(tf.amount) filter
(where tf.fare_conditions = 'Business') over
(partition by tf.flight_id, city, tf.fare_conditions), '0') as max_amount 	
		from ticket_flights tf	
		join flights f using
(flight_id)	
		join airports_data ad on f.arrival_airport = ad.airport_code)
select distinct amnt.flight_id, amnt.flight_no, amnt.d_f, amnt.city,
	coalesce (min (amnt.max_amount) filter (where amnt.fare_conditions = 'Business') over (partition by amnt.flight_id, amnt.city), '0'
) as max_bus,
	coalesce
(max
(amnt.max_amount) filter
(where amnt.fare_conditions = 'Economy') over
(partition by amnt.flight_id, amnt.city), '0') as max_eco
	from amnt) as res
where res.max_bus < res.max_eco and res.max_bus <> 0;


-- 8. Между какими городами нет прямых рейсов?
with
	twin
	as
	(
							select *
			from
				(select ad.city ->> lang()  as city_1
				from airports_data ad) as city_1,
				(select ad2.city ->> lang()  as city_2
				from airports_data ad2) as city_2
		except
			select distinct ad.city ->> lang() as city_1, ad2.city ->> lang() as city_2
			from flights f
				join airports_data ad on f.departure_airport = ad.airport_code
				join airports_data ad2 on f.arrival_airport = ad2.airport_code
		except
			select distinct ad2.city ->> lang() as city_1, ad.city ->> lang() as city_2
			from flights f
				join airports_data ad on f.departure_airport = ad.airport_code
				join airports_data ad2 on f.arrival_airport = ad2.airport_code
	)
select twin.city_1 as "город 1", twin.city_2 as "город 2"
from twin
where twin.city_1 <> twin.city_2;


-- 9. Вычислите расстояние между аэропортами, связанными прямыми рейсами, сравните с допустимой максимальной дальностью перелетов
-- в самолетах, обслуживающих эти рейсы.
with
	table_flights
	as
	(
		select distinct ac.model ->> lang() as model, ac."range", ad.city ->> lang() as d_city, ad.airport_name ->> lang() as d_airport,
			ad2.city ->> lang() as a_city, ad2.airport_name ->> lang () as a_airport,
			round ((6371 * radians (acosd (sind (ad.coordinates [1]) * sind (ad2.coordinates [1]) + cosd (ad.coordinates [1]) *
						cosd (ad2.coordinates [1]) * cosd (ad.coordinates [0] - ad2.coordinates[0]))))::numeric, 1) as distance
		from flights f
			join airports_data ad on f.departure_airport = ad.airport_code
			join airports_data ad2 on f.arrival_airport = ad2.airport_code
			join aircrafts_data ac on f.aircraft_code = ac.aircraft_code
	)
select table_flights.model as "модель самолёта", table_flights."range" as "дальность полёта", table_flights.d_city as "город вылета", table_flights.d_airport as "аэропрот вылета",
	a_city as "город назначения", table_flights.a_airport as "аэропорт назначения", table_flights.distance as "дальность перелёта",
	case	when table_flights."range" - table_flights.distance > 1000 then 'прекрасно долетит'
		when table_flights."range" - table_flights.distance > 500 then 'хорошо долетит'
		when table_flights."range" - table_flights.distance > 50 then 'нормально долетит'
		when table_flights."range" - table_flights.distance > 10 then 'сможет долететь'
		when table_flights."range" - table_flights.distance > 0.00001 then 'почти долетит'
		else 'шансов нет'
end as "шанс долететь",
	table_flights."range" - table_flights.distance as "запас полёта"
from table_flights;
