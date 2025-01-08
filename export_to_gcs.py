import pymongo
import json
import logging
from google.cloud import storage
from bson import json_util
import os

if __name__ == "__main__":
    # Config log
    logging.basicConfig(filename='app.log',style="{", datefmt=f"%Y-%m-%d %H:%M", level=logging.INFO, format="{asctime} - {levelname} - {message}")

    # Đường dẫn lưu trữ access key để truy cập vào cloud         
    acess_key = "./key_access/zippy-cab-443813-c6-7917553ff779.json"
    # Hàm tạo kết nối với GCS
    def create_gcs_client(service_account_key_path):
        return storage.Client.from_service_account_json(service_account_key_path)

    # Hàm để tải file từ local lên GCS 
    def upload_file(bucket_name, source_file_path, destination_blob_name):
        client = create_gcs_client(acess_key)
        
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_path)
        print(f"File {source_file_path} uploaded to {destination_blob_name}.")

    def export_to_gcs():
        try:
            # Kết nối với MongoDB
            myclient = pymongo.MongoClient("mongodb://localhost:27017/")
            mydb = myclient['glamira_ubl']
            mycol_full = mydb['glamira_ubl']
            mycol_ip = mydb['ip']

            # Tải raw data
            idx = 1
            # Thiết lập tên file temp ban đầu
            output = 'summary_0.json'

            # Lặp qua các document trong các collection 
            for doc in mycol_full.find():
                with open(output, 'a') as f:
                    json.dump(doc,f,default=json_util.default)
                    f.write('\n')
                # Tải lên GCS khi đủ 100000 document (1 batch)
                if (idx%100000 == 0):
                    try:
                        upload_file('glamira_ubl_project_06', output, f'summary/{output}')
                        logging.info(f'{output} uploaded success')
                        # Xóa file temp ở local
                        os.remove(output)
                        output = f'summary_{int(idx/100000)}.json'
                    except Exception as e:
                        logging.error(f'Error uploading {output} to GCS: {e}')
                        logging.error(f'index end at: {idx}')
                        idx += 1

                print(idx)
                idx += 1
            # Tải các document còn lại
            upload_file('glamira_ubl_project_06', output, f'summary/{output}')
            logging.info(f'summary uploaded success')
            try:
                os.remove(output)
            except Exception:
                pass

            # Tải location data (Tương tự các bước trên)
            idx = 1
            output = 'location_0.json'
            
            for doc in mycol_ip.find():
                with open(output, 'a') as f:
                    json.dump(doc,f,default=json_util.default)
                    f.write('\n')

                if (idx%100000 == 0):
                    try:
                        upload_file('glamira_ubl_project_06', output, f'location/{output}')
                        logging.info(f'{output} uploaded success')
                        os.remove(output)
                        output = f'location_{int(idx/100000)}.json'
                    except Exception as e:
                        logging.error(f'Error uploading {output} to GCS: {e}')
                        logging.error(f'index end at: {idx}')
                        idx += 1
                    
                print(idx)
                idx += 1

            upload_file('glamira_ubl_project_06', output, f'location/{output}')
            logging.info(f'location uploaded success')
            try:
                os.remove(output)
            except Exception:
                pass
        except Exception as e:
            logging.error(f'Error uploading data to GCS: {e}')
            

    export_to_gcs()
