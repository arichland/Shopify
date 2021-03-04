_author_ = 'arichland'
import pydict
import requests
import json
import pprint
from datetime import datetime, timedelta, timezone, date
import pymysql
pp = pprint.PrettyPrinter(indent=1)

# SQL Connection Setup
sql = pydict.sql_dict.get
user = sql('user')
password = sql('password')
host = sql('host')
database = sql('db_shopify')

# Variables
auth = pydict.auth.get
get = pydict.base_urls.get
format = "%Y-%m-%dT%H:%M:%S%z"
dt = datetime.strptime

# Dictionaries
variants = {}
orders = {}

key_list = []
dict1 = {}
tax = {}

def update_time():
    dt = datetime
    td = timedelta
    now = dt.now()
    utc_sec = td(seconds=18000)
    yd_sec = td(seconds=86400)
    yd_utc = (now + utc_sec)-yd_sec
    #update_time = dt(year=yd_utc.year, month=yd_utc.month, day=yd_utc.day, hour=23, minute=59, second=59)
    update_time = "2019-03-01T15:57:11-04:00"
    return update_time

# Get page info error on last call so data is not pass along
def TEST_pg_api(base, fields, time, limit):
    print("API Call")
    api_data = {}
    url = base+fields %(time, limit)
    req = requests.get(url)
    header = req.headers
    link = header.get('Link', '')
    nx_pg = 'next'
    pg_link = ''
    pg_count = 0
    count = 0
    try:
        while nx_pg == 'next':
                pg_count += 1
                print("PAGE:", pg_count)
                pg_link = link[1:-13]
                nx_pg = link[-5:-1]

                pg_info= "&page_info=%s&limit=%s" %(pg_link, limit)
                url2 = base+pg_info

                req = requests.get(url2)
                header = req.headers
                link = header.get('Link', '')

                response = req.json()
                api_data = response

    except:
        pass
    finally:
        return api_data

#WORKING
def shopify_pg_api(api):
    print("API Call")
    api_data = {}
    fields = "created_at_min=2019-03-01T15:57:11-04:00&limit=50"
    #base = pydict.base_urls.get(api)
    url = api+fields
    req = requests.get(url)
    header = req.headers
    link = header.get('Link', '')
    nx_pg = 'next'
    pg_link = ''
    pg_count = 0
    count = 0
    while nx_pg == 'next':
            limit = 50
            pg_count += 1
            print("PAGE:", pg_count)
            pg_link = link[1:-13]
            nx_pg = link[-5:-1]

            pg_info= "&page_info=%s&limit=%s" %(pg_link, limit)
            url2 = api+pg_info

            req = requests.get(url2)
            header = req.headers
            link = header.get('Link', '')

            response = req.json()
            data = response
            #pp.pprint(data)
    #except KeyError:
    #    pass
    return data


def get_pg_products():
    print("Shopify Products: Start")
    base_url = get("products")
    time = update_time()
    limit = 50
    fields = "created_at_min=%s&limit=%s"
    data = shopify_pg_api(base_url)
    pp.pprint(data)
get_pg_products()


def shopify_api(url):
    req = requests.get(url)
    response = req.json()
    data = response
    return data

def api_collections():
   url = get('smart_collections')
   r = requests.get(url)
   data = json.loads(r.text)
   pp.pprint(data)

def sql_products():
   print("Shopify Products: Start")
   products = shopify_api("products")
   default = products.get('')
   con = pymysql.connect(user=user, password=password, host=host, database=database)
   try:
       for i in products.values():
           created_at = i['created_at']
           handle = i['handle']
           product_id = i['id']
           product_type = i['product_type']
           status = i['status']
           tags = i['tags']
           title = i['title']
           updated_at = i['updated_at']
           vendor = i['vendor']

           qry_insert_data = """Insert into shopify.tbl_products(
                           created_at,
                           handle,
                           product_id,
                           product_type,
                           status,
                           tags,
                           title,
                           updated_at,
                           vendor) 
                           VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);"""

           with con.cursor() as cur:
               cur.execute(qry_insert_data, (
                   created_at,
                   handle,
                   product_id,
                   product_type,
                   status,
                   tags,
                   title,
                   updated_at,
                   vendor
               ))
   except KeyError:
       default
   finally:
       con.commit()
       cur.close()
       con.close()

def new_products():
    print("Shopify Products: Start")
    base_url = get("products")
    time = update_time()
    limit = 250
    fields = "created_at_min=%s&limit=%s" % (time, limit)
    url = base_url + fields
    data = shopify_api(url)
    default = data.get('')
    products = data["products"]

    #pp.pprint(products)
    con = pymysql.connect(user=user, password=password, host=host, database=database)
    try:
        with con.cursor() as cur:
            print("   Creating temp SQL table")
            qry_temp_table = """CREATE TEMPORARY TABLE IF NOT EXISTS tbl_temp like shopify.tbl_products;"""
            cur.execute(qry_temp_table)
            #con.commit()

            for i in products:
                created_at = i['created_at']
                handle = i['handle']
                product_id = i['id']
                product_type = i['product_type']
                status = i['status']
                tags = i['tags']
                title = i['title']
                updated_at = i['updated_at']
                vendor = i['vendor']

                qry_temp_data = """
                Insert into shopify.tbl_temp(
                                       created_at,
                                       handle,
                                       product_id,
                                       product_type,
                                       status,
                                       tags,
                                       title,
                                       updated_at,
                                       vendor) 
                                       VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);"""
                cur.execute(qry_temp_data, (created_at,
                                            handle,
                                            product_id,
                                            product_type,
                                            status,
                                            tags,
                                            title,
                                            updated_at,
                                            vendor))

            qry_insert_new_data = """
                INSERT INTO tbl_products(
                created_at,
                handle,
                product_id,
                product_type,
                status,
                tags,
                title,
                updated_at,
                vendor)
                
                SELECT
                SQ1.created_at,
                SQ1.handle,
                SQ1.product_id,
                SQ1.product_type,
                SQ1.status,
                SQ1.tags,
                SQ1.title,
                SQ1.updated_at,
                SQ1.vendor
                
                FROM(SELECT
                created_at,
                handle,
                product_id,
                product_type,
                status,
                tags,
                title,
                updated_at,
                vendor
                FROM tbl_temp) AS SQ1 LEFT JOIN tbl_products ON SQ1.product_id = tbl_products.product_id WHERE tbl_products.product_id IS NULL;"""
            cur.execute(qry_insert_new_data)
        cur.close()
    finally:
        con.commit()
        con.close()
#new_products()