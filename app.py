_author_ = 'arichland'
import pydict
import requests
import json
import pprint
from datetime import datetime
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

def shopify_api(api):
    print("API Call:", api)
    api_data = {}
    url = get(api)
    nx_pg = 'next'
    pg_link = ''
    pg_count = 0
    count = 0

    try:
        while nx_pg == 'next':
            pg_count += 1
            print("PAGE:", pg_count)
            req = requests.get(url, pg_link)
            header = req.headers
            response = req.json()
            pg_link = header['Link'][1:-13]
            nx_pg = header['Link'][-5:-1]
            data = response[api]

            for i in data:
                count += 1
                temp = {count: i}
                api_data.update(temp)
    except KeyError:
        pass

    return api_data

def api_collections():
   url = get('smart_collections')
   r = requests.get(url)
   data = json.loads(r.text)
   pp.pprint(data)

def api_all_orders():
    count = 0
    print('Shopify Orders API: Start')
    url = get('orders')
    req = requests.get(url)
    req_data = req.json()
    orders = req_data['orders']

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
        created_at = dt(i['created_at'], format).isoformat()
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
        processed_at = dt(i['processed_at'], format).isoformat()
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
        tax_type = ''
        tax_rate = 0
        tax_total = float(0)
        updated_at = dt(i['updated_at'], format).isoformat()
        user_id = i['user_id']
        items = i['line_items']
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

            for i in taxes:
                tax_type = i['title']
                tax_rate = i['rate']
                tax_total = i['price']
                temp = {order_id: {'tax_type': i['title'],
                                   'tax_rate': i['rate'],
                                   'tax_total': i['price']}}
                tax.update(temp)

            temp = {order_id: {
                'browser_ip': browser_ip,
                'buyer_accepts_marketing': buyer_accepts_marketing,
                'cancel_reason': cancel_reason,
                'cancelled_at': cancelled_at,
                'cart_token': cart_token,
                'checkout_id': checkout_id,
                'checkout_token': checkout_token,
                'closed_at': closed_at,
                'confirmed': confirmed,
                'contact_email': contact_email,
                'created_at': created_at,
                'currency': currency,
                'current_total_duties_set': current_total_duties_set,
                'customer_locale': customer_locale,
                'device_id': device_id,
                'email': email,
                'financial_status': financial_status,
                'fulfillment_status': fulfillment_status,
                'gateway': gateway,
                'order_id': order_id,
                'landing_site': landing_site,
                'landing_site_ref': landing_site_ref,
                'location_id': location_id,
                'name': name,
                'note': note,
                'number': number,
                'order_number': order_number,
                'order_status_url': order_status_url,
                'original_total_duties_set': original_total_duties_set,
                'payment_gateway_names': payment_gateway_names,
                'phone': phone,
                'presentment_currency': presentment_currency,
                'processed_at': processed_at,
                'processing_method': processing_method,
                'reference': reference,
                'referring_site': referring_site,
                'source_identifier': source_identifier,
                'source_name': source_name,
                'source_url': source_url,
                'subtotal_price': subtotal_price,
                'tags': tags,
                'taxes_included': taxes_included,
                'tax_type': tax_type,
                'tax_rate': tax_rate,
                'tax_total': tax_total,
                'test': test,
                'token': token,
                'updated_at': updated_at,
                'user_id': user_id,
                'item_fulfillment_service': item_fulfillment_service,
                'item_fulfillment_status': item_fulfillment_status,
                'gift_card': gift_card,
                'grams': grams,
                'item_id': item_id,
                'item_name': item_name,
                'item_price': item_price,
                'product_id': product_id,
                'quantity': quantity,
                'ship_address1': ship_address1,
                'ship_address2': ship_address2,
                'ship_city': ship_city,
                'ship_country': ship_country,
                'ship_country_code': ship_country_code,
                'ship_firstname': ship_firstname,
                'ship_lastname': ship_lastname,
                'ship_latitude': ship_latitude,
                'ship_longitude': ship_longitude,
                'ship_name': ship_name,
                'ship_phone': ship_phone,
                'ship_state': ship_state,
                'ship_state_code': ship_state_code,
                'ship_zip': ship_zip,
                'sku': sku,
                'title': title,
                'total_discount': total_discount,
                'variant_id': variant_id,
                'variant_title': variant_title,
                'vendor': vendor}}
            orders.update(temp)

    for i in orders:
        if i in tax:
            orders[i]['tax_rate'] = tax[i]['tax_rate']
            orders[i]['tax_total'] = tax[i]['tax_total']
            orders[i]['tax_type'] = tax[i]['tax_type']
        else: pass
    pp.pprint(orders)

    print('Shopify Orders API: Complete')

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

def sql_orders():
   print('Shopify Orders Start')
   orders = shopify_api("orders")
   default = orders.get('')
   count = 0
   pp.pprint(orders)
   con = pymysql.connect(user=user, password=password, host=host, database=database)

   try:
       for i in orders.values():
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
           tax_type = ''
           tax_rate = 0
           tax_total = float(0)
           updated_at = i['updated_at']
           user_id = i['user_id']
           items = i['line_items']
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

               for i in taxes:
                   tax_type = i['title']
                   tax_rate = i['rate']
                   tax_total = i['price']
                   temp = {order_id: {'tax_type': i['title'],
                                      'tax_rate': i['rate'],
                                      'tax_total': i['price']}}
                   tax.update(temp)

                   qry_insert_orders = """Insert into shopify.tbl_orders(
                         import_timestamp,
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
                         tax_type
                         )     
                         VALUES(Now(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
                   qry_update_source = """UPDATE shopify.tbl_orders SET source_name = "Etsy" WHERE source_name = "279941";"""

                   with con.cursor() as cur:
                       cur.execute(qry_insert_orders, (browser_ip,
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
                                                       tax_type
                                                       ))
                       cur.execute(qry_update_source)
   except KeyError:
       default
   finally:
       con.commit()
       cur.close()
       con.close()

def functions():
    api_collections()
    api_all_orders()
    get_customers()
    get_products()
    sql_orders()
functions()