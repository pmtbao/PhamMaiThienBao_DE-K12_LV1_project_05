Note: Install necessary libraries

## Mô tả file và thư mục:
- [dim_date.sql](dim_date.sql): file tạo chiều thời gian (date dimension) để phục vụ phân tích.
- [dim_date.csv](dim_date.csv): file .csv được tạo ra sau khi chạy [](dim_date.sql).
- [export_to_gcs.py](export_to_gcs.py) : script python để chuyển dữ liệu từ MongoDB lên GCS.
- [](gcs_to_bigquery.py): script python để chuyển dữ liệu raw từ GCS vào BigQuery
- [](gcs_to_bigquery_location.py): script python để chuyển dữ liệu về vị trí (ip) từ GCS vào BigQuery.
- [](ip.py): script để xử lý dữ liệu ip.
- [](product.py): script để xử lý dữ liệu product.
- [](trigger-bigquery-load_function-source): thư mục chứa script của Cloud Function để trigger việc tự động hóa update dữ liệu trong BigQuery khi có dữ liệu được thêm mới vào GCS.

## Quy trình thực thi:
[*Quy trình thực hiện*](https://funky-grin-fc2.notion.site/GCP-Project-5-6-15a4ed8143e98052b0d0edb034061c57?pvs=4)
