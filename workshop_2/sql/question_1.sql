DROP MATERIALIZED VIEW question_1;
DROP MATERIALIZED VIEW trip_time_stat;


CREATE MATERIALIZED VIEW trip_time_stat AS
    WITH stat AS (
        SELECT
            PULocationID AS pu_location_id,
            DOLocationID AS do_location_id,
            MIN(tpep_dropoff_datetime - tpep_pickup_datetime) AS min_time,
            AVG(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_time,
            MAX(tpep_dropoff_datetime - tpep_pickup_datetime) AS max_time
        FROM trip_data
        GROUP BY
            PULocationID,
            DOLocationID
    ), renamed AS (
        SELECT
            pu_tz.Zone AS pu_location_name,
            do_tz.Zone AS do_location_name,

            t.*
        FROM stat AS t
        JOIN taxi_zone AS pu_tz
            ON t.pu_location_id = pu_tz.location_id
        JOIN taxi_zone AS do_tz
            ON t.do_location_id = do_tz.location_id
    )
        SELECT * FROM renamed;


CREATE MATERIALIZED VIEW question_1 AS
    WITH t AS (
        SELECT MAX(avg_time) AS max_avg_time
        FROM trip_time_stat
    )
    SELECT trip_time_stat.*
    FROM trip_time_stat
    JOIN t
        ON trip_time_stat.avg_time = t.max_avg_time;
