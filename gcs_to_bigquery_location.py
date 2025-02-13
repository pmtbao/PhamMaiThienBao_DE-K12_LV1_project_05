from google.cloud import storage
from google.cloud import bigquery
import logging
def gcs_to_bigquery ():
    # Config log
    logging.basicConfig(filename='app.log',style="{", datefmt=f"%Y-%m-%d %H:%M", level=logging.INFO, format="{asctime} - {levelname} - {message}")

    # Đường dẫn lưu trữ access key để truy cập vào cloud         
    acess_key = "./key_access/zippy-cab-443813-c6-7917553ff779.json"

    storage_client = storage.Client.from_service_account_json(acess_key)
    bigquery_client = bigquery.Client.from_service_account_json(acess_key)
    
    #Set table ID
    table_id = "zippy-cab-443813-c6.glamira_ubl.location"

    schema = [
        bigquery.SchemaField("_id", "JSON", mode="NULLABLE"),
        bigquery.SchemaField("collection", "STRING",mode="REQUIRED"),
        bigquery.SchemaField("ip", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("country_short", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("country_long", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("region", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
    ]
    table = bigquery.Table(table_id, schema=schema)

    # try:
    #     bigquery_client.delete_table(table)
    # except Exception:
    #     pass

    try:
        table = bigquery_client.create_table(table)
    except Exception as e:
        print(f"Error creating table: {e}")
        pass

    job_config = bigquery.LoadJobConfig(
    schema=schema,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    
    bucket = storage_client.bucket("glamira_ubl_project_06")
    for blob in bucket.list_blobs(prefix="location/"):
        if ".json" in blob.name:
            uri = "gs://glamira_ubl_project_06/"+blob.name
            print(uri)
            load_job = bigquery_client.load_table_from_uri(
                uri,
                table_id,
                location="US",  # Must match the destination dataset location.
                job_config=job_config,
            )  # Make an API request.

            load_job.result()  # Waits for the job to complete.

        destination_table = bigquery_client.get_table(table_id)
        print("Loaded {} rows.".format(destination_table.num_rows))

if __name__  =="__main__":
    gcs_to_bigquery()