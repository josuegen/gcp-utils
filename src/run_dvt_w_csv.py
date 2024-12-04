"""
DVT automation to build and run vaidation commands

1. Pulls the configuration from a confiuguratioin table in BigQuery (Line 36)
2. Builds the DVT command based on the configuration
3. Executes the built command and stores the results in a BigQuery table

Author: josuegen@google.com
"""

import argparse
import subprocess
import google.auth
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from typing import List

pd.options.mode.chained_assignment = None


def convert_to_fmtd_str(date) -> str:
    if (not pd.isna(date)) and date.isnumeric():
        return datetime.strptime(str(int(date)),'%Y%m%d').strftime('%Y-%m-%d')
    else:
        pass


def run_process(row_range: List[int]):

    sql = """
        SELECT
            Project,
            Dataset,
            Table_Name,
            Filter_Column,
            Validation_Date
        FROM `<project>.<dataset>.<table>`; --TODO: UPDATE FQDN source config table ID
    """

    base_filter_statement = """
    data-validation validate column \
    -tc bq-dev \
    -sc bq-dev \
    -tbls '{source_project}.{source_dataset}.{table}=i-dss-dw-poc.dw_drop.{table}' \
    --filters 'date({date_column})='"'"'{validation_date}'"'"'' \
    -bqrh <project>.<dataset>.<table> --TODO: UPDATE FQDN result table ID
    """

    base_statement = """
    data-validation validate column \
    -tc bq-dev \
    -sc bq-dev \
    -tbls '{source_project}.{source_dataset}.{table}=i-dss-dw-poc.dw_drop.{table}' \
    -bqrh <project>.<dataset>.<table> --TODO: UPDATE FQDN result table ID
    """

    credentials, project = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
        ]
    )
    client = bigquery.Client(credentials=credentials, project=project)
    bq_cp_template = client.query(sql).to_dataframe()

    cp_template = bq_cp_template.iloc[[i for i in range(row_range[0]-2,row_range[1]-1)], :]

    statements = []

    cp_template['Validation_Date'] = cp_template['Validation_Date'].apply(convert_to_fmtd_str)

    for column,row in cp_template.iterrows():
        if row['Filter_Column'] != None:
            statement = base_filter_statement.format(
                source_project=row['Project'].replace('-prod','-dev'),
                source_dataset=row['Dataset'],
                table=row['Table_Name'],
                date_column=row['Filter_Column'],
                validation_date=row['Validation_Date']
            )
        else:
            statement = base_statement.format(
                source_project=row['Project'].replace('-prod','-dev'),
                source_dataset=row['Dataset'],
                table=row['Table_Name']
            )

        print("========================================")
        print(f"RUNNING: {statement}")
        print("========================================")

        statements.append(statement)

        p = subprocess.Popen(statement, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in p.stdout.readlines():
            print(line.decode('utf-8'))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-rr',
        '--row_range',
        help='Range of rows where the tables to validate are at, in the format n-N, where n<=N',
        required=True
    )
    args = parser.parse_args()

    if '-' not in args.row_range:
        raise Exception('row_range argument not in the format n-N')
    rows = [int(value) for value in args.row_range.split('-')]
    if len(rows) == 0:
        raise Exception('row_range argument not in the format n-N')
    if len(rows) > 2:
        raise Exception('row_range argument not in the format n-N')
    if rows[1] < rows[0]:
        raise Exception('row_range argument invalid  (n-N) N must be geq than n')

    run_process(row_range=rows)
