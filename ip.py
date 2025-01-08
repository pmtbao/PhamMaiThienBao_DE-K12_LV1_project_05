import IP2Location as ip2loc
import pymongo
import timeit

start = timeit.default_timer()

def process_ip_locations():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient['glamira_ubl']
    mycol = mydb['glamira_ubl']
    mycol_loc = mydb['ip']
    database = ip2loc.IP2Location("/home/pmtbao/IP-COUNTRY-REGION-CITY.BIN")
    i = 1
    for rec in mycol.find():
        print(i)
        ip_infor = database.get_all(rec["ip"])
        loc_dic = {}
        loc_dic['_id'] = rec["_id"]
        loc_dic['collection'] = rec['collection']
        loc_dic['ip'] = ip_infor.ip
        loc_dic['country_short'] = ip_infor.country_short
        loc_dic['country_long'] = ip_infor.country_long
        loc_dic['region'] = ip_infor.region
        loc_dic['city'] = ip_infor.city
        mycol_loc.insert_one(loc_dic)
        i += 1
process_ip_locations()
stop = timeit.default_timer()
print('Time: ' ,stop - start,  " seconds")
          
