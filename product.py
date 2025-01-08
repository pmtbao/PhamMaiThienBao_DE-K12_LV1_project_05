from bs4 import BeautifulSoup as bs
import requests as re
import pymongo
import timeit
import time
import random

start = timeit.default_timer()

def process_ip_locations():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient['glamira_ubl']
    mycol = mydb['glamira_ubl']
    mycol_pro = mydb['product']
    
    i = 1
    for rec in mycol.find( {"collection" : {'$in' : ['view_product_detail','select_product_option','select_product_option_quality']}} ):
        print(i)
        print(rec['collection'])
        print(rec['current_url'])
        dic = {}
        dic['product_id'] = rec["product_id"]
        url = rec['current_url']
        response = re.get(url)
        soup = bs(response.content, "html.parser")
        result = soup.select_one('div[class = "page-title-wrapper product"] h1.page-title span[data-ui-id = "page-title-wrapper"]')
        if result is not None:
            dic['product_name'] = result.text
        else: dic['product_name'] = 'None'
        mycol_pro.insert_one(dic)
        if(i%10000 == 0): time.sleep(random.uniform(2,5))
        i += 1

process_ip_locations()
stop = timeit.default_timer()
print('Time: ' ,stop - start,  " seconds")
