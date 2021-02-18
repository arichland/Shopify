_author_ = 'arichland'

import pymysql
import pydict

# SQL DB Connection Fields
sql = pydict.sql_dict.get
user = sql('user')
password = sql('password')
host = sql('host')
db = sql('db_shopify')
charset = sql('charset')
cusrorType = pymysql.cursors.DictCursor

# Establish connection to SQL DB
con = pymysql.connect(user=user,
                      password=password,
                      host=host,
                      database=db,
                      charset=charset,
                      cursorclass=cusrorType)

def create_tbl_orders():
    with con.cursor() as cur:
        qry_create_table = """CREATE TABLE IF NOT EXISTS tbl_orders(
            id INT AUTO_INCREMENT PRIMARY KEY,
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
        cur.execute(qry_create_table)
    con.commit()

def create_tbl_products():
    with con.cursor() as cur:
        qry_create_table = """
        CREATE TABLE IF NOT EXISTS tbl_products(
            id INT AUTO_INCREMENT PRIMARY KEY,
            created_at DATETIME,
            handle TEXT,
            product_id BIGINT,
            product_type VARCHAR(25),
            published_at DATETIME,
            status VARCHAR(12),
            tags TEXT,
            title TEXT,
            updated_at DATETIME,
            vendor TEXT
            )
            ENGINE=INNODB;"""
        cur.execute(qry_create_table)
    con.commit()

def create_tbl_customers():
    with con.cursor() as cur:
        qry_create_table = """
        CREATE TABLE IF NOT EXISTS tbl_customers(
            id INT AUTO_INCREMENT PRIMARY KEY,
            accepts_marketing VARCHAR(10),
            accepts_marketing_updated_at DATETIME,
            address1 TEXT,
            address2 TEXT,
            city TEXT,
            company TEXT,
            country TEXT,
            country_code VARCHAR(5),
            created_at DATETIME,
            customer_id BIGINT,
            email TEXT,
            first_name TEXT,
            last_name TEXT,
            marketing_opt_in_level VARCHAR(10),
            multipass_identifier VARCHAR(10),
            name TEXT,
            phone TEXT,
            state TEXT,
            state_code VARCHAR(5),
            tags TEXT,
            tax_exempt VARCHAR(10),
            updated_at DATETIME,
            verified_email VARCHAR(10),
            zip VARCHAR(12)
            )
            ENGINE=INNODB;"""
        cur.execute(qry_create_table)
    con.commit()

def create_tables():
    create_tbl_orders()
    create_tbl_products()
    create_tbl_customers()
create_tables()