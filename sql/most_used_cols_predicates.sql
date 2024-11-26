CREATE TEMP FUNCTION mapColumns(where_clause STRING, column_list ARRAY<STRING>)
RETURNS STRING
LANGUAGE js AS r"""
  if (!where_clause) {
    return null;
  }
  column_list.forEach(function(col) {
    const tokens = col.split(":");
    where_clause = where_clause.replaceAll(tokens[0].trim(), tokens[1]);
  });
  return where_clause;
""";

with table_read_patterns as (
  SELECT
    DATE(creation_time) AS date,
    project_id,
    IF(ARRAY_LENGTH(SPLIT(table_id, '.'))=2, SPLIT(table_id, '.')[OFFSET(0)], SPLIT(table_id, '.')[OFFSET(1)]) AS dataset_id,
    SPLIT(table_id, '.')[ORDINAL(ARRAY_LENGTH(SPLIT(table_id, '.')))] AS table_id,
    IF(ARRAY_LENGTH(SPLIT(table_id, '.'))=2, project_id || '.' || table_id, table_id) AS full_table_id,
    job_id,
    --bqutil.fn.job_url(project_id || ':us.' || job_id) AS job_url,
    parent_job_id,
    --bqutil.fn.job_url(project_id || ':us.' || parent_job_id) AS parent_job_url,
    reservation_id,
    total_bytes_billed,
    total_slot_ms,
    creation_time,
    start_time,
    end_time,
    stage_name,
    stage_id,
    stage_slot_ms,
    total_job_read_slot_ms,
    records_read,
    records_written,
    shuffle_output_bytes,
    shuffle_output_bytes_spilled,
    parallel_inputs,
    read_ratio_avg,
    read_ms_avg,
    wait_ratio_avg,
    wait_ms_avg,
    compute_ratio_avg,
    compute_ms_avg,
    write_ratio_avg,
    write_ms_avg,
    ARRAY(
      SELECT STRUCT(
        REGEXP_EXTRACT(predicate, '^[[:word:]]+') AS operator,
        REGEXP_EXTRACT(predicate, '[(]([[:word:]]+)') AS column,
        REGEXP_EXTRACT(predicate, '[,](.+)[)]') AS value )
      FROM UNNEST(filters) AS predicate
    ) AS predicates
  FROM (
    SELECT *,
      REGEXP_EXTRACT_ALL(
        mapcolumns(where_clause, projection_list),
        '[[:word:]]+[(][^()]*?[)]') AS filters
    FROM (
      SELECT
        jbp.project_id,
        job_id,
        parent_job_id,
        reservation_id,
        total_bytes_billed,
        total_slot_ms,
        creation_time,
        start_time,
        end_time,
        js.name AS stage_name,
        js.id AS stage_id,
        SUM(js.slot_ms) OVER (PARTITION BY job_id) AS total_job_read_slot_ms,
        js.slot_ms AS stage_slot_ms,
        js.records_read,
        js.records_written,
        js.shuffle_output_bytes,
        js.shuffle_output_bytes_spilled,
        js.parallel_inputs,
        js.read_ratio_avg,
        js.read_ms_avg,
        js.wait_ratio_avg,
        js.wait_ms_avg,
        js.compute_ratio_avg,
        js.compute_ms_avg,
        js.write_ratio_avg,
        js.write_ms_avg,
        SPLIT(js_steps.substeps[safe_OFFSET(0)], ',') AS projection_list,
        REPLACE(js_steps.substeps[safe_OFFSET(1)],'FROM ', '') AS table_id,
        js_steps.substeps[safe_OFFSET(2)] AS where_clause
      FROM `region-us-central1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT jbp --TODO: Developer adjust the region accordingly
      JOIN UNNEST(job_stages) AS js
      JOIN UNNEST(steps) AS js_steps
      WHERE
        DATE(creation_time) >= CURRENT_DATE - 90
        AND js_steps.kind = 'READ'
        AND jbp.job_type = 'QUERY'
        AND jbp.statement_type != 'SCRIPT'
        AND NOT cache_hit
        AND error_result IS NULL
        AND NOT EXISTS ( -- Exclude queries over INFORMATION_SCHEMA
          SELECT 1
          FROM UNNEST(js_steps.substeps) AS substeps
          WHERE substeps LIKE 'FROM %%.INFORMATION_SCHEMA.%%')
        AND EXISTS ( -- Only include substeps with a FROM clause
          SELECT 1
          FROM UNNEST(js_steps.substeps) AS substeps
          WHERE substeps LIKE 'FROM %%.%%')
    )
  )
)

SELECT *,
dataset_id || '.' || table_id
FROM(
  SELECT
    dataset_id,
    table_id,
    predicate.column,
  COUNT(*) cnt
  FROM table_read_patterns,
      UNNEST(predicates) AS predicate
      --WHERE dataset_id = ''
      --	AND table_id = ''

    GROUP BY 1,2,3
)
ORDER BY  dataset_id,table_id,cnt DESC;
