select * from breadcrumb inner join (select trip_id, avg(speed) from breadcrumb group by trip_id having count(*) > 100 order by avg(speed) desc limit 1) as t1 on breadcrumb.trip_id = t1.trip_id;
