select * from (select * from breadcrumb inner join (select * from trip where route_id != 0) as t2 on breadcrumb.trip_id = t2.trip_id) as t3 inner join (select route_id, avg(speed) from breadcrumb inner join (select * from trip where route_id != 0) as t1 on breadcrumb.trip_id = t1.trip_id group by route_id order by avg(speed) DESC limit 1) as t4 on t3.route_id = t4.route_id;

