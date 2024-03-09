DROP MATERIALIZED VIEW question_2;

CREATE MATERIALIZED VIEW question_2 AS
    WITH t AS (
        SELECT
            q1.pu_location_name,
            q1.do_location_name,
            pu_location_id,
            do_location_id,

            COUNT(*) AS num_rides
        FROM trip_data as d
        JOIN question_1 AS q1
            ON d.pulocationid = q1.pu_location_id
                AND d.dolocationid = q1.do_location_id
        GROUP BY
            q1.pu_location_id,
            q1.do_location_id,
            q1.pu_location_name,
            q1.do_location_name
    )
        SELECT * FROM t;



