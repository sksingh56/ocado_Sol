CREATE OR REPLACE TABLE FUNCTION `aqueous-walker-379718.ocado_sol.date_interval`(date_param TIMESTAMP) AS (
WITH intervals AS (
  SELECT
    GENERATE_TIMESTAMP_ARRAY(
      TIMESTAMP_TRUNC(TIMESTAMP(date_param), HOUR),
      TIMESTAMP_ADD(TIMESTAMP_TRUNC(TIMESTAMP(date_param), HOUR), INTERVAL 23 HOUR),
      INTERVAL 1 HOUR
    ) AS ts_array
),
extended_intervals AS (
  SELECT
    ARRAY_CONCAT(ts_array, [TIMESTAMP_ADD(TIMESTAMP_TRUNC(TIMESTAMP(date_param), HOUR), INTERVAL 24 HOUR)]) AS extended_ts_array
  FROM intervals
)
SELECT
  CAST(FORMAT_TIMESTAMP('%F %T', start_ts) AS TIMESTAMP) AS start_time,
  CAST(FORMAT_TIMESTAMP('%F %T', end_ts) AS TIMESTAMP) AS end_time
FROM extended_intervals
CROSS JOIN UNNEST(extended_ts_array) AS start_ts
JOIN UNNEST(extended_ts_array) AS end_ts
ON start_ts = TIMESTAMP_SUB(end_ts, INTERVAL 1 HOUR)
ORDER BY start_time
);