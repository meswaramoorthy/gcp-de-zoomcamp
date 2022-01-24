--How many taxi trips were there on January 15?
--Consider only trips that started on January 15.
-- Answer -  53,024

SELECT count(*)
FROM yellow_taxi_trips
where CAST(tpep_pickup_datetime AS DATE) = '2021-01-15';

-- Find the largest tip for each day. On which day it was the largest tip in January?
-- Use the pick up time for your calculations.
-- (note: it's not a typo, it's "tip", not "trip")
-- Answer - 20/01/2021

select DATE_TRUNC('DAY', tpep_pickup_datetime) as d, max(tip_amount) as m
from yellow_taxi_trips
group by d
order by m desc;


-- What was the most popular destination for passengers picked up in central park on January 14?
-- Use the pick up time for your calculations.
-- Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown"

-- Answer -  Upper East Side South

select d_zone."Zone", count(*) total
from yellow_taxi_trips ytt
join zones p_zone ON p_zone."LocationID" = ytt."PULocationID"
join zones d_zone ON d_zone."LocationID" = ytt."DOLocationID"
WHERE LOWER(p_zone."Zone") = LOWER('central park')
AND CAST(tpep_pickup_datetime AS DATE) = '2021-01-14'
group by d_zone."Zone"
order by total desc;

-- What's the pickup-dropoff pair with the largest average price for a ride (calculated based on total_amount)?
-- Enter two zone names separated by a slash
--For example:
-- "Jamaica Bay / Clinton East"
-- If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East".
-- Answer - Alphabet City / Unknown

select p_zone."Zone" "Pickup Zone", d_zone."Zone" "Drop Off Zone", AVG(total_amount) Average_total_amt
from yellow_taxi_trips ytt
join zones p_zone ON p_zone."LocationID" = ytt."PULocationID"
join zones d_zone ON d_zone."LocationID" = ytt."DOLocationID"
group by p_zone."Zone", d_zone."Zone"
order by Average_total_amt desc;