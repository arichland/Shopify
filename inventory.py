_author_ = 'arichland'
import pydict
import requests
import json
import pprint
from datetime import datetime, timedelta, timezone, date
import pymysql
import math
pp = pprint.PrettyPrinter(indent=1)

# Variables
auth = pydict.auth.get
get = pydict.base_urls.get
format = "%Y-%m-%dT%H:%M:%S%z"
dt = datetime.strptime

class sql:
   # SQL Connection Setup
   sql = pydict.sql_dict.get
   user = sql('user')
   password = sql('password')
   host = sql('host')
   database = sql('db_shopify')

   def products(data):
       con = pymysql.connect(user=sql.user, password=sql.password, host=sql.host, database=sql.database)
       count = 0
       products = data['products']

       with con.cursor() as cur:
           qry_temp_table = """CREATE TEMPORARY TABLE IF NOT EXISTS tbl_temp like shopify.tbl_shopify_products;"""
           cur.execute(qry_temp_table)
           con.commit()

           for i in products:
               count += 1
               created_at = i['created_at']
               handle = i['handle']
               product_id = i['id']
               product_type = i['product_type']
               published_at = i['published_at']
               published_scope = i['published_scope']
               status = i['status']
               tags = i['tags']
               template_suffix = i['template_suffix']
               title = i['title']
               updated_at = i['updated_at']
               vendor = i['vendor']
               print(handle)

               qry_temp_data = """Insert into shopify.tbl_temp(
                                                  created_at,
                                                  handle,
                                                  product_id,
                                                  product_type,
                                                  published_at,
                                                  published_scope,
                                                  status,
                                                  tags,
                                                  template_suffix,
                                                  title,
                                                  updated_at,
                                                  vendor) 
                                                  VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
               cur.execute(qry_temp_data, (
                       created_at,
                       handle,
                       product_id,
                       product_type,
                       published_at,
                       published_scope,
                       status,
                       tags,
                       template_suffix,
                       title,
                       updated_at,
                       vendor))
               con.commit()

               qry_insert_new_data = """
               INSERT INTO tbl_shopify_products(
               created_at,
               handle,
               product_id,
               product_type,
               published_at,
               published_scope,
               status,
               tags,
               template_suffix,
               title,
               updated_at,
               vendor)
               
               SELECT
               SQ1.created_at,
               SQ1.handle,
               SQ1.product_id,
               SQ1.product_type,
               SQ1.published_at,
               SQ1.published_scope,
               SQ1.status,
               SQ1.tags,
               SQ1.template_suffix,
               SQ1.title,
               SQ1.updated_at,
               SQ1.vendor
               
               FROM(SELECT
               created_at,
               handle,
               product_id,
               product_type,
               published_at,
               published_scope,
               status,
               tags,
               template_suffix,
               title,
               updated_at,
               vendor
               FROM tbl_temp) AS SQ1 LEFT JOIN tbl_shopify_products ON SQ1.product_id = tbl_shopify_products.product_id WHERE tbl_shopify_products.product_id IS NULL;"""
               cur.execute(qry_insert_new_data)
           con.commit()
       cur.close()
       con.close()

   def variants(data):
       con = pymysql.connect(user=sql.user, password=sql.password, host=sql.host, database=sql.database)
       count = 0
       variants = data['variants']

       with con.cursor() as cur:
           qry_temp_table = """CREATE TEMPORARY TABLE IF NOT EXISTS tbl_temp like shopify.tbl_shopify_variants;"""
           cur.execute(qry_temp_table)


           for i in variants:
               barcode = i['barcode']
               created_at = i['created_at']
               fulfillment_service = i['fulfillment_service']
               grams = i['grams']
               inventory_item_id = i['inventory_item_id']
               inventory_management = i['inventory_management']
               inventory_policy = i['inventory_policy']
               inventory_quantity = i['inventory_quantity']
               old_inventory_quantity = i['old_inventory_quantity']
               option1 = i['option1']
               option2 = i['option2']
               option3 = i['option3']
               position = i['position']
               price = i['price']
               product_id = i['product_id']
               requires_shipping = i['requires_shipping']
               sku = i['sku']
               taxable = i['taxable']
               title = i['title']
               updated_at = i['updated_at']
               variant_id = i['id']
               weight = i['weight']
               weight_unit =i['weight_unit']
               print(title, variant_id)

               qry_temp_data = """Insert into shopify.tbl_temp(
                barcode,
                created_at,
                fulfillment_service,
                grams,
                inventory_item_id,
                inventory_management,
                inventory_policy,
                inventory_quantity,
                old_inventory_quantity,
                option1,
                option2,
                option3,
                position,
                price,
                product_id,
                requires_shipping,
                sku,
                taxable,
                title,
                updated_at,
                variant_id,
                weight,
                weight_unit)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
               cur.execute(qry_temp_data, (
                                          barcode,
                                          created_at,
                                          fulfillment_service,
                                          grams,
                                          inventory_item_id,
                                          inventory_management,
                                          inventory_policy,
                                          inventory_quantity,
                                          old_inventory_quantity,
                                          option1,
                                          option2,
                                          option3,
                                          position,
                                          price,
                                          product_id,
                                          requires_shipping,
                                          sku,
                                          taxable,
                                          title,
                                          updated_at,
                                          variant_id,
                                          weight,
                                          weight_unit))

           qry_insert_new_data = """
                              INSERT INTO tbl_shopify_variants(
                barcode,
                created_at,
                fulfillment_service,
                grams,
                inventory_item_id,
                inventory_management,
                inventory_policy,
                inventory_quantity,
                old_inventory_quantity,
                option1,
                option2,
                option3,
                position,
                price,
                product_id,
                requires_shipping,
                sku,
                taxable,
                title,
                updated_at,
                variant_id,
                weight,
                weight_unit)
                
                SELECT
                SQ1.barcode,
                SQ1.created_at,
                SQ1.fulfillment_service,
                SQ1.grams,
                SQ1.inventory_item_id,
                SQ1.inventory_management,
                SQ1.inventory_policy,
                SQ1.inventory_quantity,
                SQ1.old_inventory_quantity,
                SQ1.option1,
                SQ1.option2,
                SQ1.option3,
                SQ1.position,
                SQ1.price,
                SQ1.product_id,
                SQ1.requires_shipping,
                SQ1.sku,
                SQ1.taxable,
                SQ1.title,
                SQ1.updated_at,
                SQ1.variant_id,
                SQ1.weight,
                SQ1.weight_unit
                
                FROM(SELECT
                barcode,
                created_at,
                fulfillment_service,
                grams,
                inventory_item_id,
                inventory_management,
                inventory_policy,
                inventory_quantity,
                old_inventory_quantity,
                option1,
                option2,
                option3,
                position,
                price,
                product_id,
                requires_shipping,
                sku,
                taxable,
                title,
                updated_at,
                variant_id,
                weight,
                weight_unit
                FROM tbl_temp) AS SQ1 LEFT JOIN tbl_shopify_variants ON SQ1.variant_id = tbl_shopify_variants.variant_id WHERE tbl_shopify_variants.variant_id IS NULL;"""
           cur.execute(qry_insert_new_data)
           con.commit()
       cur.close()
       con.close()

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

def select_sql_function(data, type):
    if type == "products":
        sql.products(data)

    elif type == "variants":
        sql.variants(data)

    else:
        print("Error selecting SQL function")

def call_shopify_api(method, url, endpoint):
    print("API Call")
    api_data = {}
    req = requests(method, url)
    data = req.json()
    select_sql_function(data, endpoint)
    header = req.headers
    link = header.get('Link', '')
    spl = str.split(link, ";")
    nxt = spl[len(spl) - 1]
    nx_pg = nxt[6:-1]
    pg_link = spl[0][1:-1]
    limit = 50
    pg_count = 0
    count = 0
    count += 1
    print("API Call", count)
    while nx_pg == 'next':
        pg_count += 1
        print("\nPAGE:", pg_count)

        pg_info = "limit=%s&page_info=%s" %(limit, pg_link)
        url = url+pg_info
        req = requests.get(url)
        header = req.headers

        # Get URL to next page from header
        link = header.get('Link', '')
        spl = str.split(link, ",")
        lnk = spl[len(spl) - 1]
        indx1 = lnk.index("https")
        indx2 = lnk.index(">;")
        indx3 = lnk.index("rel=")
        pg_link = lnk[indx1:indx2]
        nx_pg = lnk[indx3+5:-1]
        data = req.json()
        select_sql_function(data, endpoint)

def get_products():
    print("Shopify Products: Start")
    base_url = get("products")
    time = update_time()
    limit = 50
    fields = "created_at_min=%s&limit=%s"
    data = call_shopify_api(base_url)
#get_products()

def get_all_inventory_levels():
    print("Shopify Products Variants: Start")
    def product_list():
        # SQL Connection Setup
        sql = pydict.sql_dict.get
        user = sql('user')
        password = sql('password')
        host = sql('host')
        database = sql('db_shopify')
        prods = []
        con = pymysql.connect(user=user, password=password, host=host, database=database)
        with con.cursor() as cur:
            qry_products = """SELECT product_id from tbl_shopify_products;"""
            cur.execute(qry_products)
            rows = cur.fetchall()
            prod = rows[0]
            for row in rows:
                prods.append(row[0])

        return prods

    base_url = get("variants")
    prod_ids = product_list()
    for i in prod_ids:
        url = base_url %(i)
        call_shopify_api(url, "variants")


def get_inventory_item():
    def inventory_list():
        # SQL Connection Setup
        sql = pydict.sql_dict.get
        user = sql('user')
        password = sql('password')
        host = sql('host')
        database = sql('db_shopify')
        inventory = []
        con = pymysql.connect(user=user, password=password, host=host, database=database)
        with con.cursor() as cur:
            qry_products = """SELECT inventory_item_id FROM tbl_shopify_variants WHERE inventory_management = "shopify";"""
            cur.execute(qry_products)
            rows = cur.fetchall()
            prod = rows[0]
            for row in rows:
                inventory.append(row[0])
        return inventory

    items = inventory_list()
    call = 0
    item_range = range(len(items))
    item_len = len(items)

    limit = 100
    for i in range(math.ceil(len(items)/100)):
        call += 1
        #min = items[0:limit]
        max = item_len - call * limit

        print(max)

        #print(call,item_range, item_range[i])


    params = {}


    base_url = get("inventory_items")
    #url = base_url % (39551326322843)
   #call_shopify_api("get", url, "inventory_items")

get_inventory_item()


