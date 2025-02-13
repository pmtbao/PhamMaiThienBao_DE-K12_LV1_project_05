from bs4 import BeautifulSoup as bs
import requests as re
import pymongo
import timeit
import time
import random
import logging


def process_ip_locations():
    start = timeit.default_timer()
    logging.basicConfig(filename='app.log',style="{", datefmt=f"%Y-%m-%d %H:%M", level=logging.DEBUG, format="{asctime} - {levelname} - {message}")
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient['glamira_ubl']
    mycol = mydb['glamira_ubl']
    
    product = []
    
    
    none_product_name_count = 0
    i = 1
    with open('product.json', 'w') as f:
        f.write('')
    logging.info('product.json is cleared')

    product_ids = mycol.distinct("product_id")
    for id in product_ids:
        for rec in mycol.find({'product_id' : id}).limit(1):
            print(rec['collection'])
            print(rec['current_url'])
            
            dic = {}
            dic['product_id'] = rec["product_id"]
            dic['url'] = rec['current_url']

            product.append(dic)
            with open('product.json', 'a') as f:
                f.write(f'{dic}\n')

            i += 1
    logging.info(f'Have {i} product be found')
    with open('product.json', 'a') as f:
        f.write(f'{i}')
    
    myclient.close()

    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient['glamira_ubl']
    with open('product_dim.json', 'w') as f:
        f.write('')
    for rec in product:
        try:
            mycol_pro = mydb['product']
            response = re.get(rec['url'])
            soup = bs(response.content, "html.parser")
            result = soup.select_one('div[class = "page-title-wrapper product"] h1.page-title span[data-ui-id = "page-title-wrapper"]')
            dic = {}
            dic['product_id'] = rec["product_id"]
            if result is not None:
                dic['product_name'] = result.text
            else: 
                dic['product_name'] = 'None'
                none_product_name_count += 1
                logging.info(f'{rec["product_id"]} do not has name')
                logging.info(f'no-name product count: {none_product_name_count}')
            
            # dic['url'] = rec['url']
            with open('product_dim.json', 'a') as f:
                f.write(f'{dic}\n')

            mycol_pro.insert_one(dic)
        except Exception as e:
            logging.error(f'Error: {e}')
            logging.error(f"At url: {rec['url']}")
            logging.error(f'Product ID: {rec["product_id"]}')
        

        if(i%10000 == 0): time.sleep(random.uniform(2,5))
        i += 1
    
    stop = timeit.default_timer()
    total_time_exec = stop - start
    print('Time: ' ,total_time_exec,  " seconds")
    logging.info(f'Total time: {total_time_exec}')

process_ip_locations()

