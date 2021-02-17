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
count = 0
format = "%Y-%m-%dT%H:%M:%S%z"
dt = datetime.strptime

# Dictionaries
products = {}
variants = {}
orders = {}
key_list = []
dict1 = {}
tax= {}
def api_collections():
   url = get('smart_collections')
   r = requests.get(url)
   data = json.loads(r.text)
   pp.pprint(data)
#get_collections()

def api_all_rrhome_products():
   url = get('products')
   params = {'product_type': 'weighted blanket'}
   params2 = {'vendor': 'R&R Home'}
   r = requests.get(url, params=params2)
   data = json.loads(r.text)

   for i in data['products']:
      handle = i['handle']
      status = i['status']
      tags = i['tags']
      title = i['title']
      vendor = i['vendor']
      created_at = i['created_at']
      p_id = i['id']
      variants = i['variants']

      temp1 = {p_id: {
         'handle': handle,
         'status': status,
         'tags': tags,
         'title': title,
         'vendor': vendor,
         'created_at': created_at
         }}
      products.update(temp1)

      for i in variants:
         barcode = i['barcode']
         grams = i['grams']
         id = i['id']
         inven_item_id = i['inventory_item_id']
         inven_quantity = i['inventory_quantity']
         price = i['price']
         product_id = i['product_id']
         sku = i['sku']
         taxable = i['taxable']
         title = i['title']
         weight = i['weight']
         weight_unit = i['weight_unit']

         temp2 ={id: {
            'id': id,
            'barcode': barcode,
            'parent_id': p_id,
            'product_id': product_id,
            'inven_item_id': inven_item_id,
            'inven_quantity': inven_quantity,
            'price': price,
            'sku': sku,
            'taxable': taxable,
            'title': title,
            'weight': weight,
            'weight_unit': weight_unit
         }}
         pp.pprint(temp2)
   #pp.pprint(data)
#get_all_rrhome_products()


def api_all_shopify_products():
   url = get('products')
   r = requests.get(url)
   data = json.loads(r.text)
   pp.pprint(data)


def api_all_shopify_orders():
    count = 0
    print('Shopify Orders API: Start')
    url = get('orders')
    r = requests.get(url)
    data = json.loads(r.text)
    data2 = data['orders']

    for i in data2:
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
api_all_shopify_orders()

def get_all_shopify_customers():
   url = get('customers')
   r = requests.get(url)
   data = json.loads(r.text)
  #customer_data = data['customers']
  #pp.pprint(data)

   for i in data['customers']:
      customer_id = i['id']
      address1 = i['addresses']
      accepts_marketing = i['accepts_marketing']
      for i in address1:
         address_line1 = i['address1']
         address_line2 = i['address2']
         city = i['city']
         company = i['company']

def sql_shopify_orders():
   print(     'Shopify Orders SQL: Start')
   con = pymysql.connect(
      user=user,
      password=password,
      host=host,
      database=database
      )
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
      created_at = dt(i['created_at'], format).isoformat()
      currency = i['currency']
      current_total_duties_set = i['current_total_duties_set']
      customer_locale = i['customer_locale']
      device_id = i['device_id']
      email = i['email']
      financial_status = i['financial_status']
      fulfillment_status = i['fulfillment_status']
      gateway = i['gateway']
      order_id = int(i['order_id'])
      landing_site = i['landing_site']
      landing_site_ref = i['landing_site_ref']
      location_id = i['location_id']
      name = i['name']
      note = i['note']
      number = int(i['number'])
      order_number = int(i['order_number'])
      order_status_url = i['order_status_url']
      original_total_duties_set = i['original_total_duties_set']
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
      tax_rate = i['tax_rate']
      tax_total = i['tax_total']
      tax_type = i['tax_type']
      taxes_included = i['taxes_included']
      test = i['test']
      token = i['token']
      updated_at = dt(i['updated_at'], format).isoformat()
      user_id = i['user_id']
      item_fulfillment_status = i['item_fulfillment_status']
      gift_card = i['gift_card']
      grams = int(i['grams'])
      item_id = i['item_id']
      item_name = i['item_name']
      item_price = float(i['item_price'])
      product_id = i['product_id']
      quantity = i['quantity']
      sku = i['sku']
      title = i['title']
      total_discount = float(i['total_discount'])
      variant_id = i['variant_id']
      variant_title = i['variant_title']
      vendor = i['vendor']

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
         cur.execute(qry_insert_orders,(browser_ip,
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

      con.commit()
   print(     'Shopify Orders SQL: Complete')
#sql_shopify_orders()