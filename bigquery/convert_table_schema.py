from google.cloud import bigquery

client = bigquery.Client()

table_mapping = {
    'hr-datalake-rw.ds_sfsf_ec_bulk.EmpCompensation': 'hr-datalake-rw.ds_sfsf_ec.EmpCompensation'
}

base_query = """
INSERT INTO {target_table}(
{target_columns}
)
SELECT
{select_statement}
FROM {source_table};
"""

select_lines = []

for raw, formatted in table_mapping.items():
    select_lines = []
    formatted_table = client.get_table(formatted)
    raw_table = client.get_table(raw)

    formatted_schema = formatted_table.schema
    raw_schema = raw_table.schema

    field_mapping = {
        field.name: field.field_type for field in formatted_schema
    }

    for field in raw_schema:
        select_line = ""
        raw_line = ""
        if field_mapping.get(field.name) == 'STRING':
            select_line = f"{field.name}"
        elif field_mapping.get(field.name) == 'DATETIME':
            select_line = f'EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(CAST(REGEXP_EXTRACT({field.name}, r"/Date\((-?\d+)\)") AS INT64))) AS {field.name}'
        elif field_mapping.get(field.name) == 'TIMESTAMP':
            select_line = f'TIMESTAMP_MILLIS(CAST(REGEXP_EXTRACT({field.name}, r"/Date\(([-+]?\d+)\+0000\)/") AS INT64)) AS {field.name}'
        elif field_mapping.get(field.name) == 'FLOAT':
            select_line = f"CAST({field.name} AS FLOAT64) AS {field.name}"
        else:
            select_line = f"CAST({field.name} AS {field_mapping.get(field.name)}) AS {field.name}"

        raw_line = '\t'+f"{field.name} AS {field.name}_raw,"
        #select_line = raw_line+'\n\t'+select_line
        select_line = '\t'+select_line
     
        select_lines.append(select_line)
    
    print(base_query.format(
            table_name=raw,
            select_statement=',\n'.join(select_lines),
            source_table=raw,
            target_table=formatted,
            target_columns=',\n'.join([field.name for field in raw_schema])
        )
    )
