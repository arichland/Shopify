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

def update_time():
    dt = datetime
    td = timedelta
    now = dt.now()
    utc_sec = td(seconds=18000)
    yd_sec = td(seconds=86400)
    yd_utc = (now + utc_sec)-yd_sec
    update_time = dt(year=yd_utc.year, month=yd_utc.month, day=yd_utc.day, hour=23, minute=59, second=59)
    #update_time = "2019-01-01T15:57:11-04:00"
    return update_time

def shopify_api(url):
    print("   Calling API")
    req = requests.get(url)
    response = req.json()
    api_data = response
    return api_data

def update_orders():
    print('Shopify Order Updates Start:')

    # URL Setup Fields
    base_url = get('orders')
    status = "any"
    time = update_time()
    limit = 250
    fields = "status=%s&updated_at_min=%s&limit=%s" % (status, time, limit)
    url = base_url + fields

    # Pass URL fields to API and retrieve data
    data = shopify_api(url)
    orders = data['orders']
    count = 0
    con = pymysql.connect(user=user, password=password, host=host, database=database)

    try:
        with con.cursor() as cur:
            print("   Creating temp SQL table")
            qry_temp_table = """CREATE TEMPORARY TABLE IF NOT EXISTS tbl_temp like shopify.tbl_shopify_orders;"""
            cur.execute(qry_temp_table)
            con.commit()

            # Assign list fields to variables
            for i in orders:
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

                    print("   Inserting data into temp table")
                    # Insert API data into temp table
                    qry_insert_temp_data = """Insert into shopify.tbl_temp(
                                      browser_ip,
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

                    cur.execute(qry_insert_temp_data, (browser_ip,
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
                                                       ship_zip))
                    con.commit()

            # Execute query to find new records from API that are in SQL and insert those records into SQL
            print("   Updating orders in SQL")
            qry_update_orders = """
                UPDATE shopify.tbl_shopify_orders 
                        INNER JOIN shopify.tbl_temp ON shopify.tbl_shopify_orders.order_id = shopify.tbl_temp.order_id
                        SET 
                        shopify.tbl_shopify_orders.buyer_accepts_marketing = shopify.tbl_temp.buyer_accepts_marketing,
                        shopify.tbl_shopify_orders.cancel_reason = shopify.tbl_temp.cancel_reason,
                        shopify.tbl_shopify_orders.cancelled_at = shopify.tbl_temp.cancelled_at,
                        shopify.tbl_shopify_orders.cart_token = shopify.tbl_temp.cart_token,
                        shopify.tbl_shopify_orders.checkout_id = shopify.tbl_temp.checkout_id,
                        shopify.tbl_shopify_orders.checkout_token = shopify.tbl_temp.checkout_token,
                        shopify.tbl_shopify_orders.closed_at = shopify.tbl_temp.closed_at,
                        shopify.tbl_shopify_orders.confirmed = shopify.tbl_temp.confirmed,
                        shopify.tbl_shopify_orders.contact_email = shopify.tbl_temp.contact_email,
                        shopify.tbl_shopify_orders.created_at = shopify.tbl_temp.created_at,
                        shopify.tbl_shopify_orders.currency = shopify.tbl_temp.currency,
                        shopify.tbl_shopify_orders.current_total_duties_set = shopify.tbl_temp.current_total_duties_set,
                        shopify.tbl_shopify_orders.customer_locale = shopify.tbl_temp.customer_locale,
                        shopify.tbl_shopify_orders.device_id = shopify.tbl_temp.device_id,
                        shopify.tbl_shopify_orders.email = shopify.tbl_temp.email,
                        shopify.tbl_shopify_orders.financial_status = shopify.tbl_temp.financial_status,
                        shopify.tbl_shopify_orders.fulfillment_status = shopify.tbl_temp.fulfillment_status,
                        shopify.tbl_shopify_orders.gateway = shopify.tbl_temp.gateway,
                        shopify.tbl_shopify_orders.order_id = shopify.tbl_temp.order_id,
                        shopify.tbl_shopify_orders.landing_site = shopify.tbl_temp.landing_site,
                        shopify.tbl_shopify_orders.landing_site_ref = shopify.tbl_temp.landing_site_ref,
                        shopify.tbl_shopify_orders.location_id = shopify.tbl_temp.location_id,
                        shopify.tbl_shopify_orders.name = shopify.tbl_temp.name,
                        shopify.tbl_shopify_orders.note = shopify.tbl_temp.note,
                        shopify.tbl_shopify_orders.number = shopify.tbl_temp.number,
                        shopify.tbl_shopify_orders.order_number = shopify.tbl_temp.order_number,
                        shopify.tbl_shopify_orders.order_status_url = shopify.tbl_temp.order_status_url,
                        shopify.tbl_shopify_orders.original_total_duties_set = shopify.tbl_temp.original_total_duties_set,
                        shopify.tbl_shopify_orders.phone = shopify.tbl_temp.phone,
                        shopify.tbl_shopify_orders.presentment_currency = shopify.tbl_temp.presentment_currency,
                        shopify.tbl_shopify_orders.processed_at = shopify.tbl_temp.processed_at,
                        shopify.tbl_shopify_orders.processing_method = shopify.tbl_temp.processing_method,
                        shopify.tbl_shopify_orders.reference = shopify.tbl_temp.reference,
                        shopify.tbl_shopify_orders.referring_site = shopify.tbl_temp.referring_site,
                        shopify.tbl_shopify_orders.source_identifier = shopify.tbl_temp.source_identifier,
                        shopify.tbl_shopify_orders.source_url = shopify.tbl_temp.source_url,
                        shopify.tbl_shopify_orders.subtotal_price = shopify.tbl_temp.subtotal_price,
                        shopify.tbl_shopify_orders.tags = shopify.tbl_temp.tags,
                        shopify.tbl_shopify_orders.taxes_included = shopify.tbl_temp.taxes_included,
                        shopify.tbl_shopify_orders.test = shopify.tbl_temp.test,
                        shopify.tbl_shopify_orders.token = shopify.tbl_temp.token,
                        shopify.tbl_shopify_orders.updated_at = shopify.tbl_temp.updated_at,
                        shopify.tbl_shopify_orders.user_id = shopify.tbl_temp.user_id,
                        shopify.tbl_shopify_orders.item_fulfillment_status = shopify.tbl_temp.item_fulfillment_status,
                        shopify.tbl_shopify_orders.gift_card = shopify.tbl_temp.gift_card,
                        shopify.tbl_shopify_orders.grams = shopify.tbl_temp.grams,
                        shopify.tbl_shopify_orders.item_id = shopify.tbl_temp.item_id,
                        shopify.tbl_shopify_orders.item_name = shopify.tbl_temp.item_name,
                        shopify.tbl_shopify_orders.item_price = shopify.tbl_temp.item_price,
                        shopify.tbl_shopify_orders.product_id = shopify.tbl_temp.product_id,
                        shopify.tbl_shopify_orders.quantity = shopify.tbl_temp.quantity,
                        shopify.tbl_shopify_orders.sku = shopify.tbl_temp.sku,
                        shopify.tbl_shopify_orders.title = shopify.tbl_temp.title,
                        shopify.tbl_shopify_orders.variant_id = shopify.tbl_temp.variant_id,
                        shopify.tbl_shopify_orders.variant_title = shopify.tbl_temp.variant_title,
                        shopify.tbl_shopify_orders.vendor = shopify.tbl_temp.vendor,
                        shopify.tbl_shopify_orders.tax_rate = shopify.tbl_temp.tax_rate,
                        shopify.tbl_shopify_orders.tax_total = shopify.tbl_temp.tax_total,
                        shopify.tbl_shopify_orders.tax_type = shopify.tbl_temp.tax_type,
                        shopify.tbl_shopify_orders.item_fulfillment_service  = shopify.tbl_temp.item_fulfillment_service ,
                        shopify.tbl_shopify_orders.ship_address1 = shopify.tbl_temp.ship_address1,
                        shopify.tbl_shopify_orders.ship_address2 = shopify.tbl_temp.ship_address2,
                        shopify.tbl_shopify_orders.ship_city = shopify.tbl_temp.ship_city,
                        shopify.tbl_shopify_orders.ship_country = shopify.tbl_temp.ship_country,
                        shopify.tbl_shopify_orders.ship_country_code = shopify.tbl_temp.ship_country_code,
                        shopify.tbl_shopify_orders.ship_firstname = shopify.tbl_temp.ship_firstname,
                        shopify.tbl_shopify_orders.ship_lastname = shopify.tbl_temp.ship_lastname,
                        shopify.tbl_shopify_orders.ship_latitude = shopify.tbl_temp.ship_latitude,
                        shopify.tbl_shopify_orders.ship_longitude = shopify.tbl_temp.ship_longitude,
                        shopify.tbl_shopify_orders.ship_name = shopify.tbl_temp.ship_name,
                        shopify.tbl_shopify_orders.ship_phone = shopify.tbl_temp.ship_phone,
                        shopify.tbl_shopify_orders.ship_state = shopify.tbl_temp.ship_state,
                        shopify.tbl_shopify_orders.ship_state_code = shopify.tbl_temp.ship_state_code,
                        shopify.tbl_shopify_orders.ship_zip = shopify.tbl_temp.ship_zip;"""
            qry_etsy_update ="""UPDATE shopify.tbl_shopify_orders SET source_name = "Etsy" WHERE source_name = 279941;"""

            cur.execute(qry_update_orders)
            cur.execute(qry_etsy_update)
            con.commit()
    except KeyError:
        print("KeyError")
    finally:
        cur.close()
        con.close()
        print("Process Complete")

def new_orders():
   print('New Shopify Orders Start:')

   # URL Setup Fields
   base_url = get('orders')
   status = "any"
   time = update_time()
   limit = 250
   fields = "status=%s&created_at_min=%s&limit=%s" %(status, time, limit)
   url = base_url + fields

   # Pass URL fields to API and retrieve data
   data = shopify_api(url)
   orders = data['orders']
   count = 0
   con = pymysql.connect(user=user, password=password, host=host, database=database)

   try:
       with con.cursor() as cur:
           print("   Creating temp SQL table")
           qry_temp_table = """CREATE TEMPORARY TABLE IF NOT EXISTS tbl_temp like shopify.tbl_shopify_orders;"""
           cur.execute(qry_temp_table)
           con.commit()

       # Assign list fields to variables
           for i in orders:
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

                   print("   Inserting data into temp table")
# Insert API data into temp table
                   qry_insert_temp_data = """Insert into shopify.tbl_temp(
                                      browser_ip,
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
                   cur.execute(qry_insert_temp_data, (browser_ip,
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
                                                      ship_zip))
                   con.commit()

       # Execute query to find new records from API that are in SQL and insert those records into SQL
           print("   Inserting new records into SQL")
           qry_insert_new_data = """
           INSERT INTO tbl_shopify_orders(
                browser_ip,
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
                ship_zip)
                
                SELECT
                SQ1.browser_ip,
                SQ1.buyer_accepts_marketing,
                SQ1.cancel_reason,
                SQ1.cancelled_at,
                SQ1.cart_token,
                SQ1.checkout_id,
                SQ1.checkout_token,
                SQ1.closed_at,
                SQ1.confirmed,
                SQ1.contact_email,
                SQ1.created_at,
                SQ1.currency,
                SQ1.current_total_duties_set,
                SQ1.customer_locale,
                SQ1.device_id,
                SQ1.email,
                SQ1.financial_status,
                SQ1.fulfillment_status,
                SQ1.gateway,
                SQ1.order_id,
                SQ1.landing_site,
                SQ1.landing_site_ref,
                SQ1.location_id,
                SQ1.name,
                SQ1.note,
                SQ1.number,
                SQ1.order_number,
                SQ1.order_status_url,
                SQ1.original_total_duties_set,
                SQ1.phone,
                SQ1.presentment_currency,
                SQ1.processed_at,
                SQ1.processing_method,
                SQ1.reference,
                SQ1.referring_site,
                SQ1.source_identifier,
                SQ1.source_name,
                SQ1.source_url,
                SQ1.subtotal_price,
                SQ1.tags,
                SQ1.taxes_included,
                SQ1.test,
                SQ1.token,
                SQ1.updated_at,
                SQ1.user_id,
                SQ1.item_fulfillment_status,
                SQ1.gift_card,
                SQ1.grams,
                SQ1.item_id,
                SQ1.item_name,
                SQ1.item_price,
                SQ1.product_id,
                SQ1.quantity,
                SQ1.sku,
                SQ1.title,
                SQ1.variant_id,
                SQ1.variant_title,
                SQ1.vendor,
                SQ1.tax_rate,
                SQ1.tax_total,
                SQ1.tax_type,
                SQ1.item_fulfillment_service,
                SQ1.ship_address1,
                SQ1.ship_address2,
                SQ1.ship_city,
                SQ1.ship_country,
                SQ1.ship_country_code,
                SQ1.ship_firstname,
                SQ1.ship_lastname,
                SQ1.ship_latitude,
                SQ1.ship_longitude,
                SQ1.ship_name,
                SQ1.ship_phone,
                SQ1.ship_state,
                SQ1.ship_state_code,
                SQ1.ship_zip
                
                FROM(SELECT
                browser_ip,
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
                FROM tbl_temp) AS SQ1 LEFT JOIN tbl_shopify_orders ON SQ1.order_id = tbl_shopify_orders.order_id WHERE tbl_shopify_orders.order_id IS NULL ORDER BY created_at ASC;"""
           cur.execute(qry_insert_new_data)
           con.commit()
   except KeyError:
       print("KeyError")
   finally:
       cur.close()
       con.close()
       print("Process Complete")

