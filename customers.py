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


def shopify_api(url):
    req = requests.get(url)
    response = req.json()
    api_data = response
    return api_data

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
