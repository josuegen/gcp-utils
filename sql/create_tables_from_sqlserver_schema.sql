/*************************************************************************************************************************************
Dinamycally crates BigQuery tables from a extract of Information Schema from SQL Server
https://learn.microsoft.com/en-us/sql/relational-databases/system-information-schema-views/columns-transact-sql?view=sql-server-ver16

Author: Josue Velazquez (josuegen@google.com)
***************************************************************************************************************************************/

FOR record IN
  (
    SELECT
      TABLE_CATALOG dataset,
      TABLE_NAME table_name,
      array_agg(
        COLUMN_NAME ||
        ' ' ||
        (
          CASE DATA_TYPE 
            WHEN 'nvarchar' THEN 'string'
            WHEN 'varchar' THEN 'string'
            WHEN 'datetime' THEN 'timestamp'
            WHEN 'float' THEN 'float64'
            WHEN 'bit' THEN 'boolean'
            WHEN 'decimal' THEN 'float64'
            WHEN 'money' THEN 'float64'
            ELSE DATA_TYPE END
        ) ||
        ' ' ||
        CASE WHEN IS_NULLABLE THEN '' ELSE 'NOT NULL' END
      ) cols
    FROM (
      SELECT * FROM bq-poc-pso.sap_staging.SCHEMAS
      ORDER BY TABLE_NAME,ORDINAL_POSITION
    )
    GROUP BY 1,2
    ORDER BY TABLE_NAME
  )
DO
  EXECUTE IMMEDIATE FORMAT("CREATE TABLE IF NOT EXISTS %s.%s(%s)",LOWER(record.dataset),record.table_name,ARRAY_TO_STRING(record.cols, ","));
END FOR;
