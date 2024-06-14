from google.cloud import bigquery
import csv


# Create schema on *.csv file (ONLY STRING COLUMNS)
def tbl_schema_str_creator_csv(file_path):
    with open(file_path, 'r') as file_path:
        reader = csv.reader(file_path)
        header = next(reader)
        schema = [bigquery.SchemaField(column_name, "STRING", mode="NULLABLE") for column_name in header]

    return schema


# CREATE OR REPLACE tbl
def create_or_replace_tbl (client, table_ref, schema=None):
    client.delete_table(table_ref, not_found_ok=True)
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)

    return '[INFO] - Table created.'

