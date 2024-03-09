DROP MATERIALIZED VIEW question_3;

CREATE MATERIALIZED VIEW question_3 AS
    WITH res AS (
        SELECT
            pulocationid,
            tz.Zone as pu_location_name,
            num_rides,
            ROW_NUMBER() OVER (PARTITION BY _pb ORDER BY num_rides DESC) AS rn
        FROM (
            SELECT
                pulocationid,
                COUNT(*) AS num_rides,
                1 AS _pb
            FROM
                trip_data
            WHERE
                tpep_pickup_datetime + INTERVAL '17 hour' > (SELECT MAX(tpep_pickup_datetime) AS ts FROM trip_data)
            GROUP BY
                pulocationid
        ) AS t
        JOIN taxi_zone AS tz
            ON t.pulocationid = tz.location_id
    )
    SELECT * FROM res;
