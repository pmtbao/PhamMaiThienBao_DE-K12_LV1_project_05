import functions_framework
from google.cloud import bigquery

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def trigger_bigquery_load(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")

    if 'location' in name:
        bigquery_client = bigquery.Client()
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
        job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        uri = f"gs://{bucket}/{name}"
        print(uri)
        load_job = bigquery_client.load_table_from_uri(
            uri,
            table_id,
            location="US",
            job_config=job_config,
        )  # Make an API request.

        load_job.result()  # Waits for the job to complete.

        destination_table = bigquery_client.get_table(table_id)
        print("Loaded {} rows.".format(destination_table.num_rows))
    
    elif 'summary' in name:
        bigquery_client = bigquery.Client()
        table_id = "zippy-cab-443813-c6.glamira_ubl.raw_layer"
        schema = [
            bigquery.SchemaField("_id", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("time_stamp","INT64", mode="NULLABLE"),
            bigquery.SchemaField("ip", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("user_agent", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("resolution", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("user_id_db", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("device_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("api_version", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("store_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("local_time", "DATETIME", mode="NULLABLE"),
            bigquery.SchemaField("show_recommendation", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("current_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("referrer_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("email_address", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("collection", "STRING",mode="REQUIRED"),
            # ADD TO CART
            bigquery.SchemaField("product_id", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("price", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("is_paypal", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("option", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("option_label", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("option_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("value_label", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("value_id", "STRING", mode="NULLABLE"),
                # LISTING PAGE REC CLICKED + LISTING PAGE REC NOTICED + LISTING PAGE REC VISIBLE
                bigquery.SchemaField("alloy", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("diamond", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("shapediamond", "STRING", mode="NULLABLE"),
                # SELECT PRODUCT OPTION QUALITY
                bigquery.SchemaField("quality", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("quality_label", "STRING", mode="NULLABLE"),
                # VIEW ALL REC
                bigquery.SchemaField("stone", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("pearlcolor", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("finish", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("price", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("category id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("Kollektion", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("kollektion_id", "STRING", mode="NULLABLE"),
                ]),
            # BACK TO PRODUCT
            # CHECK OUT + CHECK OUT SUCCESS
            bigquery.SchemaField("order_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("cart_products", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("product_id", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("amount", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("price", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("option", "JSON", mode="NULLABLE")
                # bigquery.SchemaField("option", "RECORD", mode="REPEATED", fields=[
                #     bigquery.SchemaField("option_label", "STRING", mode="NULLABLE"),
                #     bigquery.SchemaField("option_id", "INT64", mode="NULLABLE"),
                #     bigquery.SchemaField("value_label", "STRING", mode="NULLABLE"),
                #     bigquery.SchemaField("value_id", "INT64", mode="NULLABLE"),
                # ]),
                ]),
            # LANDING PAGE REC CLICKED
            bigquery.SchemaField("recommendation_product_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("recommendation_product_position", "INT64", mode="NULLABLE"),
            # LANDING PAGE REC NOTICED
            # LANDING PAGE REC VISIBLE
            # LISTING PAGE REC CLICKED
            bigquery.SchemaField("recommendation_clicked_position", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("cat_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("collect_id", "STRING", mode="NULLABLE"),
            # LISTING PAGE REC NOTICED
            # LISTING PAGE REC VISIBLE

            # PRODUCT DETAIL REC CLICKED
            bigquery.SchemaField("viewing_product_id", "STRING", mode="NULLABLE"),
            
            # PRODUCT DETAIL REC NOTICED
            # PRODUCT DETAIL REC VISIBLE

            # PRODUCT VIEW ALL REC CLICKED

            # SEARCH BOX
            bigquery.SchemaField("key_search", "STRING", mode="NULLABLE"),
            # SELECT PRODUCT OPTION QUALITY
            # SELECT PRODUCT OPTION
            # SORTING RELEVANCE CLICK
            # VIEW ALL REC
            # VIEW HOME PAGE
            # VIEW LANDING PAGE
            # VIEW LISTING PAGE
            # VIEW MY ACCOUNT
            # VIEW PRODUCT DETAIL
            bigquery.SchemaField("recommendation", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("utm_source", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("utm_medium", "STRING", mode="NULLABLE"),
            # VIEW SHOPPING CART
            # VIEW SORTING RELEVANCE
            # VIEW STATIC PAGE
        ]
        
        job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        uri = f"gs://{bucket}/{name}"
        print(uri)
        load_job = bigquery_client.load_table_from_uri(
            uri,
            table_id,
            location="US",
            job_config=job_config,
        )  # Make an API request.

        load_job.result()  # Waits for the job to complete.

        destination_table = bigquery_client.get_table(table_id)
        print("Loaded {} rows.".format(destination_table.num_rows))


