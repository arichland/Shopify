_author_ = 'arichland'
import pydict
import requests
import json
import pprint
from datetime import datetime, timedelta, timezone, date
import pymysql
from orders import new_orders, update_orders
from products import new_products

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

def functions():
    new_products()
    new_orders()
    update_orders()
functions()