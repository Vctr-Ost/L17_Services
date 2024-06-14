from google.cloud import bigquery
import os


def bq_querier(query):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'keys/gcp_key.json'
    client = bigquery.Client()

    try:
        query_job = client.query(query)
        query_job.result()
        return 'Success'
    except:
        return 'Unsuccess'


