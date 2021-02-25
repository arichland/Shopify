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
    update_time = dt(year=yd_utc.year, month=yd_utc.month, day=yd_utc.day, hour=23, minute=59, second=59)
    return update_time

def shopify_api(url):
    req = requests.get(url)
    response = req.json()
    api_data = response
    return api_data

def api_collections():
   url = get('smart_collections')
   r = requests.get(url)
   data = json.loads(r.text)
   pp.pprint(data)

def get_customers():
    print('Shopify Products SQL: Start')
    count = 0
    con = pymysql.connect(user=user, password=password, host=host, database=database)
    customers = shopify_api("customers")
    default = customers.get('')
    try:
        for i in customers.values():
            accepts_marketing = i['accepts_marketing']
            accepts_marketing_updated_at = i['accepts_marketing_updated_at']
            address1 = i['default_address']['address1']
            address2 = i['default_address']['address2']
            city = i['default_address']['city']
            company = i['default_address']['company']
            country = i['default_address']['country']
            country_code = i['default_address']['country_code']
            created_at = i['created_at']
            customer_id = i['id']
            email = i['email']
            first_name = i['first_name']
            last_name = i['last_name']
            marketing_opt_in_level = i['marketing_opt_in_level']
            multipass_identifier = i['multipass_identifier']
            name = i['default_address']['name']
            phone = i['phone']
            state = i['default_address']['province']
            state_code = i['default_address']['province_code']
            tags = i['tags']
            tax_exempt = i['tax_exempt']
            updated_at = i['updated_at']
            verified_email = i['verified_email']
            zip = i['default_address']['zip']
            qry_insert_data = """Insert into shopify.tbl_customers(
                    accepts_marketing,
                    accepts_marketing_updated_at,
                    address1, 
                    address2, 
                    city, 
                    company, 
                    country, 
                    country_code, 
                    created_at, 
                    customer_id, 
                    email, 
                    first_name, 
                    last_name, 
                    marketing_opt_in_level, 
                    multipass_identifier, 
                    name,
                    phone, 
                    state, 
                    state_code, 
                    tags, 
                    tax_exempt, 
                    updated_at, 
                    verified_email, 
                    zip 
                    ) 
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
            with con.cursor() as cur:
                cur.execute(qry_insert_data, (
                    accepts_marketing,
                    accepts_marketing_updated_at,
                    address1,
                    address2,
                    city,
                    company,
                    country,
                    country_code,
                    created_at,
                    customer_id,
                    email,
                    first_name,
                    last_name,
                    marketing_opt_in_level,
                    multipass_identifier,
                    name,
                    phone,
                    state,
                    state_code,
                    tags,
                    tax_exempt,
                    updated_at,
                    verified_email,
                    zip
                ))
    except KeyError:
        default
    finally:
        con.commit()
        cur.close()
        con.close()

def get_products():
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

def order_updates():
   print('Shopify Orders Start')
   base_url = get('orders')
   status = "open"
   time = update_time()
   limit = 250
   fields = "status=%s&limit=%s&updated_at_min=%s" % (status, limit, time)
   url = base_url + fields
   data = shopify_api(url)
   orders = data['orders']
   count = 0

   try:
           for i in orders: #Assign list fields to variables
               browser_ip = i['browser_ip']
               buyer_accepts_marketing = i['buyer_accepts_marketing']
               cancel_reason = i['cancel_reason']
               cancelled_at = i['cancelled_at']
               cart_token = i['cart_token']
               checkout_id = i['checkout_id']
               checkout_token = i['checkout_token']
               closed_at = i['closed_at']
               confirmed = i['confirmed']
               contact_email = i['contact_email']
               created_at = i['created_at']
               currency = i['currency']
               current_total_duties_set = i['current_total_duties_set']
               customer_locale = i['customer_locale']
               device_id = i['device_id']
               email = i['email']
               financial_status = i['financial_status']
               fulfillment_status = i['fulfillment_status']
               gateway = i['gateway']
               order_id = int(i['id'])
               landing_site = i['landing_site']
               landing_site_ref = i['landing_site_ref']
               location_id = i['location_id']
               name = i['name']
               note = i['note']
               number = int(i['number'])
               order_number = int(i['order_number'])
               order_status_url = i['order_status_url']
               original_total_duties_set = i['original_total_duties_set']
               payment_gateway_names = i['payment_gateway_names']
               phone = i['phone']
               presentment_currency = i['presentment_currency']
               processed_at = i['processed_at']
               processing_method = i['processing_method']
               reference = i['reference']
               referring_site = i['referring_site']
               source_identifier = i['source_identifier']
               source_name = i['source_name']
               source_url = i['source_url']
               subtotal_price = float(i['subtotal_price'])
               tags = i['tags']
               taxes_included = i['taxes_included']
               test = i['test']
               token = i['token']
               user_id = i['user_id']
               updated_at = i['updated_at']

               ship = i['shipping_address']
               ship_address1 = ship['address1']
               ship_address2 = ship['address2']
               ship_city = ship['city']
               ship_country = ship['country']
               ship_country_code = ship['country_code']
               ship_firstname = ship['first_name']
               ship_lastname = ship['last_name']
               ship_latitude = ship['latitude']
               ship_longitude = ship['longitude']
               ship_name = ship['name']
               ship_phone = ship['phone']
               ship_state = ship['province']
               ship_state_code = ship['province_code']
               ship_zip = ship['zip']

               items = i['line_items']
               for i in items:
                   count += 1
                   item_fulfillment_service = i['fulfillment_service']
                   item_fulfillment_status = i['fulfillment_status']
                   gift_card = i['gift_card']
                   grams = int(i['grams'])
                   item_id = i['id']
                   item_name = i['name']
                   item_price = float(i['price'])
                   product_id = i['product_id']
                   quantity = i['quantity']
                   sku = i['sku']
                   title = i['title']
                   total_discount = float(i['total_discount'])
                   variant_id = i['variant_id']
                   variant_title = i['variant_title']
                   vendor = i['vendor']
                   taxes = i['tax_lines']
                   tax_type = ''
                   tax_rate = ''
                   tax_total = ''

               qry_temp_table = """CREATE TEMPORARY TABLE tbl_order_update(
                           browser_ip TEXT,
                           buyer_accepts_marketing VARCHAR(25),
                           cancel_reason VARCHAR(25),
                           cancelled_at VARCHAR(25),
                           cart_token VARCHAR(25),
                           checkout_id VARCHAR(25),
                           checkout_token VARCHAR(25),
                           closed_at VARCHAR(25),
                           confirmed VARCHAR(25),
                           contact_email VARCHAR(255),
                           created_at DATETIME,
                           currency VARCHAR(7),
                           current_total_duties_set VARCHAR(25),
                           customer_locale VARCHAR(25),
                           device_id VARCHAR(25),
                           email VARCHAR(25),
                           financial_status VARCHAR(25),
                           fulfillment_status VARCHAR(25),
                           gateway VARCHAR(25),
                           gift_card TEXT,
                           grams INT,
                           import_timestamp DATETIME,
                           item_fulfillment_service TEXT,
                           item_fulfillment_status TEXT,
                           item_id BIGINT,
                           item_name TEXT,
                           item_price FLOAT,
                           landing_site VARCHAR(255),
                           landing_site_ref VARCHAR(255),
                           location_id VARCHAR(255),
                           name VARCHAR(25),
                           note TEXT,
                           number INT,
                           order_id BIGINT,
                           order_number INT,
                           order_status_url TEXT,
                           original_total_duties_set VARCHAR(255),
                           phone VARCHAR(255),
                           presentment_currency VARCHAR(255),
                           processed_at DATETIME,
                           processing_method VARCHAR(255),
                           product_id BIGINT,
                           quantity INT,
                           reference VARCHAR(255),
                           referring_site VARCHAR(255),
                           ship_address1 TEXT,
                           ship_address2 TEXT,
                           ship_city TEXT,
                           ship_country TEXT,
                           ship_country_code TEXT,
                           ship_firstname TEXT,
                           ship_lastname TEXT,
                           ship_latitude DECIMAL(7,4),
                           ship_longitude DECIMAL(7,4),
                           ship_name TEXT,
                           ship_phone TEXT,
                           ship_state TEXT,
                           ship_state_code TEXT,
                           ship_zip TEXT,            
                           sku TEXT,
                           source_identifier VARCHAR(255),
                           source_name VARCHAR(255),
                           source_url TEXT,
                           subtotal_price FLOAT,
                           tags TEXT,
                           tax_rate FLOAT,
                           tax_total FLOAT,
                           tax_type TEXT,          
                           taxes_included VARCHAR(255),
                           test VARCHAR(255),
                           title TEXT,
                           token VARCHAR(255),           
                           updated_at DATETIME,
                           user_id VARCHAR(255),
                           variant_id BIGINT,
                           variant_title TEXT,
                           vendor TEXT    
                           )
                           ENGINE=INNODB;"""
               qry_insert = """Insert into shopify.tbl_order_update(browser_ip,
                                  buyer_accepts_marketing,
                                  cancel_reason,
                                  cancelled_at,
                                  cart_token,
                                  checkout_id,
                                  checkout_token,
                                  closed_at,
                                  confirmed,
                                  contact_email,
                                  created_at,
                                  currency,
                                  current_total_duties_set,
                                  customer_locale,
                                  device_id,
                                  email,
                                  financial_status,
                                  fulfillment_status,
                                  gateway,
                                  order_id,
                                  landing_site,
                                  landing_site_ref,
                                  location_id,
                                  name,
                                  note,
                                  number,
                                  order_number,
                                  order_status_url,
                                  original_total_duties_set,
                                  phone,
                                  presentment_currency,
                                  processed_at,
                                  processing_method,
                                  reference,
                                  referring_site,
                                  source_identifier,
                                  source_name,
                                  source_url,
                                  subtotal_price,
                                  tags,
                                  taxes_included,
                                  test,
                                  token,      
                                  updated_at,
                                  user_id,
                                  item_fulfillment_status,
                                  gift_card,
                                  grams,
                                  item_id,
                                  item_name,
                                  item_price,
                                  product_id,
                                  quantity,
                                  sku,
                                  title,
                                  variant_id,
                                  variant_title,
                                  vendor,
                                  tax_rate,
                                  tax_total,
                                  tax_type,
                                  item_fulfillment_service ,
                                  ship_address1,
                                  ship_address2,
                                  ship_city,
                                  ship_country,
                                  ship_country_code,
                                  ship_firstname,
                                  ship_lastname,
                                  ship_latitude,
                                  ship_longitude,
                                  ship_name,
                                  ship_phone,
                                  ship_state,
                                  ship_state_code,
                                  ship_zip)
                                  VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
               qry_update ="""
                    UPDATE shopify.tbl_orders 
                    INNER JOIN shopify.tbl_order_update ON shopify.tbl_orders.order_id = shopify.tbl_order_update.order_id
                    SET 
                    shopify.tbl_orders.buyer_accepts_marketing = shopify.tbl_order_update.buyer_accepts_marketing,
                    shopify.tbl_orders.cancel_reason = shopify.tbl_order_update.cancel_reason,
                    shopify.tbl_orders.cancelled_at = shopify.tbl_order_update.cancelled_at,
                    shopify.tbl_orders.cart_token = shopify.tbl_order_update.cart_token,
                    shopify.tbl_orders.checkout_id = shopify.tbl_order_update.checkout_id,
                    shopify.tbl_orders.checkout_token = shopify.tbl_order_update.checkout_token,
                    shopify.tbl_orders.closed_at = shopify.tbl_order_update.closed_at,
                    shopify.tbl_orders.confirmed = shopify.tbl_order_update.confirmed,
                    shopify.tbl_orders.contact_email = shopify.tbl_order_update.contact_email,
                    shopify.tbl_orders.created_at = shopify.tbl_order_update.created_at,
                    shopify.tbl_orders.currency = shopify.tbl_order_update.currency,
                    shopify.tbl_orders.current_total_duties_set = shopify.tbl_order_update.current_total_duties_set,
                    shopify.tbl_orders.customer_locale = shopify.tbl_order_update.customer_locale,
                    shopify.tbl_orders.device_id = shopify.tbl_order_update.device_id,
                    shopify.tbl_orders.email = shopify.tbl_order_update.email,
                    shopify.tbl_orders.financial_status = shopify.tbl_order_update.financial_status,
                    shopify.tbl_orders.fulfillment_status = shopify.tbl_order_update.fulfillment_status,
                    shopify.tbl_orders.gateway = shopify.tbl_order_update.gateway,
                    shopify.tbl_orders.order_id = shopify.tbl_order_update.order_id,
                    shopify.tbl_orders.landing_site = shopify.tbl_order_update.landing_site,
                    shopify.tbl_orders.landing_site_ref = shopify.tbl_order_update.landing_site_ref,
                    shopify.tbl_orders.location_id = shopify.tbl_order_update.location_id,
                    shopify.tbl_orders.name = shopify.tbl_order_update.name,
                    shopify.tbl_orders.note = shopify.tbl_order_update.note,
                    shopify.tbl_orders.number = shopify.tbl_order_update.number,
                    shopify.tbl_orders.order_number = shopify.tbl_order_update.order_number,
                    shopify.tbl_orders.order_status_url = shopify.tbl_order_update.order_status_url,
                    shopify.tbl_orders.original_total_duties_set = shopify.tbl_order_update.original_total_duties_set,
                    shopify.tbl_orders.phone = shopify.tbl_order_update.phone,
                    shopify.tbl_orders.presentment_currency = shopify.tbl_order_update.presentment_currency,
                    shopify.tbl_orders.processed_at = shopify.tbl_order_update.processed_at,
                    shopify.tbl_orders.processing_method = shopify.tbl_order_update.processing_method,
                    shopify.tbl_orders.reference = shopify.tbl_order_update.reference,
                    shopify.tbl_orders.referring_site = shopify.tbl_order_update.referring_site,
                    shopify.tbl_orders.source_identifier = shopify.tbl_order_update.source_identifier,
                    shopify.tbl_orders.source_url = shopify.tbl_order_update.source_url,
                    shopify.tbl_orders.subtotal_price = shopify.tbl_order_update.subtotal_price,
                    shopify.tbl_orders.tags = shopify.tbl_order_update.tags,
                    shopify.tbl_orders.taxes_included = shopify.tbl_order_update.taxes_included,
                    shopify.tbl_orders.test = shopify.tbl_order_update.test,
                    shopify.tbl_orders.token = shopify.tbl_order_update.token,
                    shopify.tbl_orders.updated_at = shopify.tbl_order_update.updated_at,
                    shopify.tbl_orders.user_id = shopify.tbl_order_update.user_id,
                    shopify.tbl_orders.item_fulfillment_status = shopify.tbl_order_update.item_fulfillment_status,
                    shopify.tbl_orders.gift_card = shopify.tbl_order_update.gift_card,
                    shopify.tbl_orders.grams = shopify.tbl_order_update.grams,
                    shopify.tbl_orders.item_id = shopify.tbl_order_update.item_id,
                    shopify.tbl_orders.item_name = shopify.tbl_order_update.item_name,
                    shopify.tbl_orders.item_price = shopify.tbl_order_update.item_price,
                    shopify.tbl_orders.product_id = shopify.tbl_order_update.product_id,
                    shopify.tbl_orders.quantity = shopify.tbl_order_update.quantity,
                    shopify.tbl_orders.sku = shopify.tbl_order_update.sku,
                    shopify.tbl_orders.title = shopify.tbl_order_update.title,
                    shopify.tbl_orders.variant_id = shopify.tbl_order_update.variant_id,
                    shopify.tbl_orders.variant_title = shopify.tbl_order_update.variant_title,
                    shopify.tbl_orders.vendor = shopify.tbl_order_update.vendor,
                    shopify.tbl_orders.tax_rate = shopify.tbl_order_update.tax_rate,
                    shopify.tbl_orders.tax_total = shopify.tbl_order_update.tax_total,
                    shopify.tbl_orders.tax_type = shopify.tbl_order_update.tax_type,
                    shopify.tbl_orders.item_fulfillment_service  = shopify.tbl_order_update.item_fulfillment_service ,
                    shopify.tbl_orders.ship_address1 = shopify.tbl_order_update.ship_address1,
                    shopify.tbl_orders.ship_address2 = shopify.tbl_order_update.ship_address2,
                    shopify.tbl_orders.ship_city = shopify.tbl_order_update.ship_city,
                    shopify.tbl_orders.ship_country = shopify.tbl_order_update.ship_country,
                    shopify.tbl_orders.ship_country_code = shopify.tbl_order_update.ship_country_code,
                    shopify.tbl_orders.ship_firstname = shopify.tbl_order_update.ship_firstname,
                    shopify.tbl_orders.ship_lastname = shopify.tbl_order_update.ship_lastname,
                    shopify.tbl_orders.ship_latitude = shopify.tbl_order_update.ship_latitude,
                    shopify.tbl_orders.ship_longitude = shopify.tbl_order_update.ship_longitude,
                    shopify.tbl_orders.ship_name = shopify.tbl_order_update.ship_name,
                    shopify.tbl_orders.ship_phone = shopify.tbl_order_update.ship_phone,
                    shopify.tbl_orders.ship_state = shopify.tbl_order_update.ship_state,
                    shopify.tbl_orders.ship_state_code = shopify.tbl_order_update.ship_state_code,
                    shopify.tbl_orders.ship_zip = shopify.tbl_order_update.ship_zip;"""

               con = pymysql.connect(user=user, password=password, host=host, database=database)
               with con.cursor() as cur:
                   cur.execute(qry_temp_table)
                   cur.execute(qry_insert, (browser_ip,
                                                   buyer_accepts_marketing,
                                                   cancel_reason,
                                                   cancelled_at,
                                                   cart_token,
                                                   checkout_id,
                                                   checkout_token,
                                                   closed_at,
                                                   confirmed,
                                                   contact_email,
                                                   created_at,
                                                   currency,
                                                   current_total_duties_set,
                                                   customer_locale,
                                                   device_id,
                                                   email,
                                                   financial_status,
                                                   fulfillment_status,
                                                   gateway,
                                                   order_id,
                                                   landing_site,
                                                   landing_site_ref,
                                                   location_id,
                                                   name,
                                                   note,
                                                   number,
                                                   order_number,
                                                   order_status_url,
                                                   original_total_duties_set,
                                                   phone,
                                                   presentment_currency,
                                                   processed_at,
                                                   processing_method,
                                                   reference,
                                                   referring_site,
                                                   source_identifier,
                                                   source_name,
                                                   source_url,
                                                   subtotal_price,
                                                   tags,
                                                   taxes_included,
                                                   test,
                                                   token,
                                                   updated_at,
                                                   user_id,
                                                   item_fulfillment_status,
                                                   gift_card,
                                                   grams,
                                                   item_id,
                                                   item_name,
                                                   item_price,
                                                   product_id,
                                                   quantity,
                                                   sku,
                                                   title,
                                                   variant_id,
                                                   variant_title,
                                                   vendor,
                                                   tax_rate,
                                                   tax_total,
                                                   tax_type,
                                                   item_fulfillment_service,
                                                   ship_address1,
                                                   ship_address2,
                                                   ship_city,
                                                   ship_country,
                                                   ship_country_code,
                                                   ship_firstname,
                                                   ship_lastname,
                                                   ship_latitude,
                                                   ship_longitude,
                                                   ship_name,
                                                   ship_phone,
                                                   ship_state,
                                                   ship_state_code,
                                                   ship_zip
                                                   ))
                   cur.execute(qry_update)
               con.commit()
               cur.close()
               con.close()

   except KeyError:
       print("KeyError")

def functions():
    #api_collections()
    #api_all_orders()
    #get_customers()
    #get_products()
    order_updates()
functions()