from flask import Flask
from google.cloud import bigquery, storage
from datetime import datetime
import os
from table_crud import tbl_schema_str_creator_csv, create_or_replace_tbl


app = Flask(__name__)

client = bigquery.Client()              # init clients
storage_client = storage.Client()


@app.route('/gcs-to-gbq/<src>/<last_row_marker>', methods=['GET'])
def data_transporting_to_BQ (src, last_row_marker):

    project_id = 'crack-battery-413519'     # BQ
    dataset_csv_id = 'bronze'
    dataset_json_id = 'silver'
    table_id = src

    bucket_name = 'raw_bucket_vost'         # GCS
    blobs_list = [b.name for b in list(storage_client.list_blobs(bucket_name)) if b.name.startswith(src)]

    if last_row_marker.lower() == 'true':       # Change blobs_list if it need to get only last folder
        def dt_comparing(blob):
            return datetime.strptime(blob.split('/')[1], '%Y-%m-%d').date()

        last_blob = max(blobs_list, key=dt_comparing).split('/')[:2]
        start_str = f'{'/'.join(last_blob)}/'
        blobs_list = [blob for blob in blobs_list if blob.startswith(start_str)]
    
    try:
        if blobs_list[0].endswith('.json'):
            table_ref = f'{project_id}.{dataset_json_id}.{table_id}'
            data_loader_json(bucket_name, blobs_list, table_ref)
        elif blobs_list[0].endswith('.csv'):
            table_ref = f'{project_id}.{dataset_csv_id}.{table_id}'
            data_loader_csv(bucket_name, blobs_list, table_ref)
        return 'Loaded', 200
    except:
        return 'Error', 400


def data_loader_csv(bucket_name, blobs_list, table_ref):
    job_config = bigquery.LoadJobConfig()
    job_config.skip_leading_rows = 1

    for idx, blob_name in enumerate(blobs_list):
        gcs_uri = f'https://storage.cloud.google.com/{bucket_name}/{blob_name}'

        if idx == 0:    # create file.csv for 1st file for schema creation
            storage_client.bucket(bucket_name).blob(blob_name).download_to_filename('data/schema_creator_file.csv')
            schema = tbl_schema_str_creator_csv('data/schema_creator_file.csv')
            create_or_replace_tbl(client, table_ref, schema)
            os.remove('data/schema_creator_file.csv')

        load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        load_job.result()

    return 200


def data_loader_json(bucket_name, blobs_list, table_ref):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
    )

    create_or_replace_tbl(client, table_ref)

    for blob_name in blobs_list:
        gcs_uri = f'https://storage.cloud.google.com/{bucket_name}/{blob_name}'
        load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        load_job.result()


if __name__ == '__main__':
    app.run(port=8081)