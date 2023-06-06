  -- Build Staging table
CREATE OR REPLACE TABLE
  ocado_sol.stg_hourly_aggregate AS
WITH
  date_to_process AS (
  SELECT
    *
  FROM
    ocado_sol.date_interval('2021-05-24') ),
  machine_list AS (
  SELECT
    DISTINCT sos.meta_orb_site,
    osi.timezone,
    sos.meta_machine_name,
    sos.meta_deploy_mode
  FROM
    `ocado_sol.sorting_states` sos
  JOIN
    `ocado_sol.orb_site_info` osi
  ON
    osi.orb_site = sos.meta_orb_site )
SELECT
  mcl.meta_orb_site,
  mcl.meta_machine_name,
  mcl.meta_deploy_mode,
  mcl.timezone,
  dtp.start_time,
  dtp.end_time,
  DATETIME(dtp.start_time,mcl.timezone) local_start_time,
  DATETIME(dtp.end_time,mcl.timezone) local_end_time
FROM
  machine_list mcl
CROSS JOIN
  date_to_process dtp
ORDER BY
  start_time