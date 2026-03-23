SELECT current_database()

 -- vendorid and most used payment_type
SELECT 
   ny_jan.vendorid, 
	count(ny_jan.tip_amount) AS tip_count, 
    sum(ny_jan.tip_amount) AS sum_of_tips, 
    max(ny_jan.tip_amount), 
    min(ny_jan.tip_amount)
FROM public."nyc_yellow_2025-01" AS ny_jan
WHERE ny_jan.tip_amount >= 0
GROUP BY ny_jan.vendorid 
ORDER BY sum(ny_jan.tip_amount) DESC;

--     #tip statistics per vendor
WITH payment_type_ranks AS (
	SELECT ny_jan.vendorid, 
           ny_jan.payment_type, 
           count(*) AS total_trips, 
           DENSE_RANK() OVER (PARTITION BY ny_jan.vendorid ORDER BY count(*)DESC) AS rn
    FROM public."nyc_yellow_2025-01" AS ny_jan
    GROUP BY ny_jan.vendorid, ny_jan.payment_type
    )
SELECT * 
FROM payment_type_ranks
WHERE rn = 1;

--    #Peak hour analysis per day of week
WITH day_hour_trip_count AS (
	SELECT 
    	EXTRACT(DOW FROM tpep_pickup_datetime) AS day_of_week,
        EXTRACT(HOUR FROM ny_jan.tpep_pickup_datetime) AS pickup_hour,
        COUNT(*) AS trip_count
    FROM public."nyc_yellow_2025-01" AS ny_jan
    GROUP BY EXTRACT(DOW FROM tpep_pickup_datetime),EXTRACT(HOUR FROM ny_jan.tpep_pickup_datetime)
    RDER BY EXTRACT(DOW FROM tpep_pickup_datetime),COUNT(*) DESC
        ),
ranks AS (
	SELECT
    	*,
       	dense_rank() OVER (PARTITION BY day_of_week ORDER BY trip_count) AS rank_per_count
	FROM 
		day_hour_trip_count
    )
SELECT *
FROM ranks
WHERE rank_per_count = 1;

