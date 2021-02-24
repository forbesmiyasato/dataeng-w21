SELECT * FROM (SELECT trip_id FROM breadcrumb WHERE latitude BETWEEN 45.586158 AND 45.592404 AND LONGITUDE BETWEEN -122.550711 AND -122.541270) AS breadcrumbs_on_bridge INNER JOIN trip ON breadcrumbs_on_bridge.trip_id = trip.trip_id WHERE route_id = 65;

SELECT * FROM (SELECT * FROM breadcrumb WHERE latitude BETWEEN 45.586158 AND 45.592404 AND LONGITUDE BETWEEN -122.550711 AND -122.541270) AS breadcrumbs_on_bridge INNER JOIN trip ON breadcrumbs_on_bridge.trip_id = trip.trip_id WHERE route_id = 65 and trip.trip_id = 170004942;
