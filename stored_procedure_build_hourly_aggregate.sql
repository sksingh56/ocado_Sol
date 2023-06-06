CREATE OR REPLACE PROCEDURE `aqueous-walker-379718.ocado_sol.build_hourly_aggregagte`(input_start_process_datetime TIMESTAMP, input_end_process_datetime TIMESTAMP, input_slide_window INT64)
BEGIN
  

  DECLARE last_received_datetime TIMESTAMP;
  DECLARE start_process_datetime TIMESTAMP;
  DECLARE table_count INT64;
    -- Check Hourly aggregate table exists
    SET table_count = (
      SELECT COUNT(1) AS cnt
      FROM `ocado_sol.__TABLES_SUMMARY__`
      WHERE table_id = 'hourly_sorting_aggregate'
    );
  -- Check if the input parameter is null
  IF input_start_process_datetime IS NULL 
   AND table_count > 0    
  THEN
    -- Get start processing time from hourly_sorting_aggregate
    SET start_process_datetime = (
      SELECT TIMESTAMP_SUB(MAX(start_time), INTERVAL input_slide_window HOUR)
      FROM `ocado_sol.hourly_sorting_aggregate` hsa
    );
  ELSE
    SET start_process_datetime = input_start_process_datetime;
  END IF;

  -- Identify end date for processing window using the max_processed_datetime
  IF input_end_process_datetime IS NULL THEN
    SET last_received_datetime = (
      SELECT MIN(meta_received_time) meta_received_time
      FROM (
        SELECT MAX(soat.meta_received_time) meta_received_time FROM `ocado_sol.sort_attempts` soat
        UNION ALL
        SELECT MAX(sost.meta_received_time) meta_received_time FROM `ocado_sol.sorting_states` sost
        UNION ALL
        SELECT MAX(unit.meta_received_time) meta_received_time FROM `ocado_sol.unload_items` unit
        UNION ALL
        SELECT MAX(unlo.meta_received_time) meta_received_time FROM `ocado_sol.unloads` unlo
      )
    );
  ELSE
    SET last_received_datetime = input_end_process_datetime;
  END IF;

  -- Build Stage Table
  CREATE OR REPLACE TEMP TABLE `stg_machine_hourly_aggregate` AS (
    WITH
      date_to_process AS (
        SELECT *
        FROM ocado_sol.date_interval(start_process_datetime)
      ),
      machine_list AS (
        SELECT DISTINCT
          sos.meta_orb_site,
          osi.timezone,
          sos.meta_machine_name,
          sos.meta_deploy_mode
        FROM
          `ocado_sol.sorting_states` sos
        JOIN
          `ocado_sol.orb_site_info` osi ON osi.orb_site = sos.meta_orb_site
      )
      SELECT
        mcl.meta_orb_site,
        mcl.meta_machine_name,
        mcl.meta_deploy_mode,
        mcl.timezone,
        dtp.start_time,
        dtp.end_time,
        DATETIME(dtp.start_time, mcl.timezone) local_start_time,
        DATETIME(dtp.end_time, mcl.timezone) local_end_time
      FROM
        machine_list mcl
      CROSS JOIN
        date_to_process dtp
      WHERE
        dtp.start_time BETWEEN start_process_datetime AND last_received_datetime
      ORDER BY
        start_time
  );

  -- Build aggregate
  CREATE OR REPLACE TEMP TABLE stg_hourly_sorting_aggregate AS (
    WITH
      sort_attempt_aggregate AS (
        SELECT
          sha.meta_orb_site,
          sha.meta_machine_name,
          sha.meta_deploy_mode,
          sha.start_time,
          sha.end_time,
          COUNT(*) total_attempts,
          SUM(CASE WHEN outcome = 'complete' THEN 1 ELSE 0 END) successful_attempt,
          SUM(CASE WHEN outcome = 'partial' THEN 1 ELSE 0 END) partial_attempt
        FROM
          `stg_machine_hourly_aggregate` sha
        LEFT OUTER JOIN
          `ocado_sol.sort_attempts` sat ON (
            sat.meta_orb_site = sha.meta_orb_site
            AND sat.meta_machine_name = sha.meta_machine_name
            AND sat.meta_deploy_mode = sha.meta_deploy_mode
            AND sat.meta_event_time BETWEEN sha.start_time AND sha.end_time
            AND sat.meta_received_time BETWEEN start_process_datetime AND last_received_datetime
          )
        GROUP BY
          sha.meta_orb_site,
          sha.meta_machine_name,
          sha.meta_deploy_mode,
          sha.start_time,
          sha.end_time
      ),
      sort_state_aggregate AS (
        SELECT
          sha.meta_orb_site,
          sha.meta_machine_name,
          sha.meta_deploy_mode,
          sha.start_time,
          sha.end_time,
          SUM(CASE WHEN sos.state = 'sorting' THEN TIMESTAMP_DIFF(sos.end_time, sos.start_time, SECOND) ELSE 0 END) sorting_seconds,
          SUM(CASE WHEN sos.state = 'blocked' THEN TIMESTAMP_DIFF(sos.end_time, sos.start_time, SECOND) ELSE 0 END) locked_seconds,
          SUM(CASE WHEN sos.state = 'scheduled_downtime' THEN TIMESTAMP_DIFF(sos.end_time, sos.start_time, SECOND) ELSE 0 END) scheduled_downtime_seconds,
          SUM(CASE WHEN sos.state = 'unscheduled_downtime' THEN TIMESTAMP_DIFF(sos.end_time, sos.start_time, SECOND) ELSE 0 END) unscheduled_downtime_seconds
        FROM
          `stg_machine_hourly_aggregate` sha
        LEFT OUTER JOIN
          `ocado_sol.sorting_states` sos ON (
            sha.meta_orb_site = sos.meta_orb_site
            AND sha.meta_machine_name = sos.meta_machine_name
            AND sha.meta_deploy_mode = sos.meta_deploy_mode
            AND sos.meta_event_time BETWEEN sha.start_time AND sha.end_time
            AND sos.meta_received_time BETWEEN start_process_datetime AND last_received_datetime
          )
        GROUP BY
          sha.meta_orb_site,
          sha.meta_machine_name,
          sha.meta_deploy_mode,
          sha.start_time,
          sha.end_time
      ),
      units_aggregate AS (
        SELECT
          sha.meta_orb_site,
          sha.meta_machine_name,
          sha.meta_deploy_mode,
          sha.start_time,
          sha.end_time,
          COUNT(*) number_of_units
        FROM
          `stg_machine_hourly_aggregate` sha
        LEFT OUTER JOIN
          `ocado_sol.unload_items` uni ON (
            uni.meta_orb_site = sha.meta_orb_site
            AND uni.meta_machine_name = sha.meta_machine_name
            AND uni.meta_deploy_mode = sha.meta_deploy_mode
            AND uni.meta_event_time BETWEEN sha.start_time AND sha.end_time
            AND uni.meta_received_time BETWEEN start_process_datetime AND last_received_datetime
          )
        GROUP BY
          sha.meta_orb_site,
          sha.meta_machine_name,
          sha.meta_deploy_mode,
          sha.start_time,
          sha.end_time
      )
      SELECT
        soat.meta_orb_site,
        soat.meta_machine_name,
        soat.meta_deploy_mode,
        soat.start_time,
        soat.end_time,
        soat.total_attempts,
        soat.successful_attempt,
        soat.partial_attempt,
        sost.sorting_seconds,
        sost.locked_seconds,
        sost.scheduled_downtime_seconds,
        sost.unscheduled_downtime_seconds,
        unag.number_of_units
      FROM
        sort_attempt_aggregate soat
      JOIN
        sort_state_aggregate sost ON (
          soat.meta_orb_site = sost.meta_orb_site
          AND soat.meta_machine_name = sost.meta_machine_name
          AND soat.meta_deploy_mode = sost.meta_deploy_mode
          AND soat.start_time = sost.start_time
          AND soat.end_time = sost.end_time
        )
      JOIN
        units_aggregate unag ON (
          soat.meta_orb_site = unag.meta_orb_site
          AND soat.meta_machine_name = unag.meta_machine_name
          AND soat.meta_deploy_mode = unag.meta_deploy_mode
          AND soat.start_time = unag.start_time
          AND soat.end_time = unag.end_time
        )
  );

    IF table_count > 0 THEN
      MERGE INTO `ocado_sol.hourly_sorting_aggregate` hsa
      USING (
        SELECT *
        FROM stg_hourly_sorting_aggregate
      ) shsa ON (
        hsa.meta_orb_site = shsa.meta_orb_site
        AND hsa.meta_deploy_mode = shsa.meta_deploy_mode
        AND hsa.meta_machine_name = shsa.meta_machine_name
        AND hsa.start_time = shsa.start_time
        AND hsa.end_time = shsa.end_time
      )
      WHEN MATCHED THEN
        UPDATE SET
          hsa.total_attempts = shsa.total_attempts,
         -- hsa.successful_attempt = shsa.successful_attempt,
          hsa.partial_attempt = shsa.partial_attempt,
          hsa.sorting_seconds = shsa.sorting_seconds,
          hsa.scheduled_downtime_seconds = shsa.scheduled_downtime_seconds,
          hsa.unscheduled_downtime_seconds = shsa.unscheduled_downtime_seconds,
          hsa.number_of_units = shsa.number_of_units
      WHEN NOT MATCHED THEN
        INSERT (
          meta_orb_site,
          meta_machine_name,
          meta_deploy_mode,
          start_time,
          end_time,
          total_attempts,
          sorting_seconds,
          scheduled_downtime_seconds,
          number_of_units
        )
        VALUES (
          meta_orb_site,
          meta_machine_name,
          meta_deploy_mode,
          start_time,
          end_time,
          total_attempts,
          sorting_seconds,
          scheduled_downtime_seconds,
          number_of_units
        );
    ELSE
      CREATE OR REPLACE TABLE ocado_sol.hourly_sorting_aggregate AS (
        SELECT *
        FROM stg_hourly_sorting_aggregate
      );
    END IF;

  END;