# -*- coding: utf-8 -*-
# TODO:
# - Create faster IDs (no traversal):
#   - D = W * 10 + D
#   - C = D * 3000 + C
# - Hardcode DS_INFO
# - Customer of distict
# - Order of customer
# - Orderline of order
# -----------------------------------------------------------------------

from __future__ import with_statement

import os
import sqlite3
import logging
import subprocess
from pprint import pprint,pformat
import time
from typedb.driver import TypeDB, SessionType, TransactionType, TypeDBOptions, TypeDBCredential

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import constants
from drivers.abstractdriver import AbstractDriver
from enum import Enum

class EDITION(Enum):
    Cloud = 1
    Core = 2

DPW = constants.DISTRICTS_PER_WAREHOUSE
CPD = constants.CUSTOMERS_PER_DISTRICT

TXN_QUERIES = {
    "DELIVERY": {
        "getNewOrder": "SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1", #
        "deleteNewOrder": "DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?", # d_id, w_id, no_o_id
        "getCId": "SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", # no_o_id, d_id, w_id
        "updateOrders": "UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", # o_carrier_id, no_o_id, d_id, w_id
        "updateOrderLine": "UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # o_entry_d, no_o_id, d_id, w_id
        "sumOLAmount": "SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # no_o_id, d_id, w_id
        "updateCustomer": "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?", # ol_total, c_id, d_id, w_id
    },

    "NEW_ORDER": {
        # match
        #  $i isa ITEM, has I_ID {i_id}, has I_PRICE $i_price;
        #  $w isa WAREHOUSE, has W_ID {w_id};
        #  $d (warehouse: $w) isa DISTRICT, has D_ID %d_id;
        #  $s (item: $i, warehouse: $w) isa STOCKING, has S_QUANTITY $s_quantity;
        #  (stocking: $s, district: $d) isa DISTRICT_STOCK_INFO, has DS_INFO $ds_info;
        # get $i_price, $s_quantity, $ds_info;
        #
        # match 
        #  $w isa WAREHOUSE, has W_ID {w_id}, has W_TAX $w_tax;
        #  $c (district: $d) isa CUSTOMER;
        #  $d (warehouse: $w) isa DISTRICT, has D_ID {d_id}, has D_NEXT_O_ID $d_next_o_id;
        #  ?d_next_o_id_old = $d_next_o_id;
        #  ?d_next_o_id_new = $d_next_o_id + 1;
        #  $c has C_ID $c_id, C_DISCOUNT $c_discount, C_LAST $c_last, C_CREDIT $c_credit;
        #  $i isa ITEM, has I_ID %i_id, has I_PRICE $i_price;  # repeat for each item
        # delete
        #  $d has $d_next_o_id;
        # insert
        #  $d has D_NEXT_O_ID $d_next_o_id_new;
        #  $o (customer: $c, district: $d) isa ORDER, 
        #    has O_ID ?d_next_o_id,
        #    has O_ENTRY_D %o_entry_d, 
        #    has O_CARRIED_ID %o_carrier_id, 
        #    has O_OL_CNT %o_ol_cnt, 
        #    has O_ALL_LOCAL %o_all_local,
        #    has O_NEW true,
        #  (order: $o, item: $i) isa ORDER_LINE;
        #   
        # match 
        #  $w isa WAREHOUSE, has W_ID %w_id;
        #  
        "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?", # w_id
        "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?", # d_id, w_id
        "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?", # d_next_o_id, d_id, w_id
        "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
        "createOrder": "INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
        "createNewOrder": "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)", # o_id, d_id, w_id
        "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?", # ol_i_id
        "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?", # d_id, ol_i_id, ol_supply_w_id
        "updateStock": "UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?", # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
        "createOrderLine": "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info        
    },
    
    "ORDER_STATUS": {
        "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", # w_id, d_id, c_last
        "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1", # w_id, d_id, c_id
        "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?", # w_id, d_id, o_id        
    },
    
    "PAYMENT": {
        "getWarehouse": "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = ?", # w_id
        "updateWarehouseBalance": "UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?", # h_amount, w_id
        "getDistrict": "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?", # w_id, d_id
        "updateDistrictBalance": "UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID  = ? AND D_ID = ?", # h_amount, d_w_id, d_id
        "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", # w_id, d_id, c_last
        "updateBCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id
        "updateGCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id
        "insertHistory": "INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    },
    
    "STOCK_LEVEL": {
        "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?", 
        "getStockCount": """
            SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
            WHERE OL_W_ID = ?
              AND OL_D_ID = ?
              AND OL_O_ID < ?
              AND OL_O_ID >= ?
              AND S_W_ID = ?
              AND S_I_ID = OL_I_ID
              AND S_QUANTITY < ?
        """,
    },
}


## ==============================================
## SqliteDriver
## ==============================================
class TypedbDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "database": ("Name of DB", "tpcc" ),
        "addr": ("Address of server", "127.0.0.1:1729" ),
        "edition": ("TypeDB Edition (Core or Cloud)", "Core" ),
        "user": ("DB User", "admin" ),
        "password": ("DB Password", "password"),
        "schema": ("Script-relative path to schema file", "tql/tpcc-schema.tql"),
    }
    
    def __init__(self, ddl):
        super(TypedbDriver, self).__init__("typedb", ddl)
        self.database = None
        self.addr = None
        self.edition = None
        self.user = None
        self.password = None
        self.driver = None
        self.session = None
        self.tx = None
    
    ## ----------------------------------------------
    ## makeDefaultConfig
    ## ----------------------------------------------
    def makeDefaultConfig(self):
        return TypedbDriver.DEFAULT_CONFIG
    
    ## ----------------------------------------------
    ## loadConfig
    ## ----------------------------------------------
    def loadConfig(self, config):
        # Config passed here contains some extra parameters (see `driver.loadConfig` in tpcc.py)
        for key in TypedbDriver.DEFAULT_CONFIG.keys():
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)
        
        self.database = str(config["database"])
        self.addr = str(config["addr"])
        if config["edition"] == "Core":
            self.edition = EDITION.Core
        if config["edition"] == "Cloud":
            self.edition = EDITION.Cloud
        self.schema = str(config["schema"])

        if self.edition is EDITION.Core:
            self.driver = TypeDB.core_driver(self.addr)
        if self.edition is EDITION.Cloud:
            self.credentials = TypeDBCredential(self.username, self.password, tls_enabled=True)
            self.driver =  TypeDB.cloud_driver(self.addr, self.credentials)

        if config["reset"] and self.driver.databases.contains(self.database):
            logging.debug("Deleting database '%s'" % self.database)
            self.driver.databases.get(self.database).delete()
        
        if not self.driver.databases.contains(self.database):
            logging.debug("Creating database'%s'" % (self.database))
            self.driver.databases.create(self.database)
            with self.driver.session(self.database, SessionType.SCHEMA) as session:
                logging.debug("Loading schema file'%s'" % (self.schema))
                script_dir = os.path.dirname(os.path.abspath(__file__))
                full_path = os.path.join(script_dir, self.schema)
                with open(full_path, 'r') as data:
                    define_query = data.read()
                logging.debug("Writing schema")
                with session.transaction(TransactionType.WRITE) as tx:
                    tx.query.define(define_query)
                    tx.commit()
                logging.debug("Committed schema")
        ## IF
    ## ----------------------------------------------
    ## loadStart
    ## ----------------------------------------------
    def loadStart(self):
        self.session = self.driver.session(self.database, SessionType.DATA)
        return None 

    ## ----------------------------------------------
    ## loadTuples
    ## ----------------------------------------------
    def loadTuples(self, tableName, tuples):
        if len(tuples) == 0: return

        with self.session.transaction(TransactionType.WRITE) as tx:
            write_query = [ ]
            is_update = False;

            if tableName == "WAREHOUSE":
                for tuple in tuples:
                    w_id = tuple[0]
                    w_name = tuple[1]
                    w_street_1 = tuple[2]
                    w_street_2 = tuple[3]
                    w_city = tuple[4]
                    w_state = tuple[5]
                    w_zip = tuple[6]
                    w_tax = tuple[7]
                    w_ytd = tuple[8]

                    q = f"""
insert 
$warehouse isa WAREHOUSE, 
has W_ID {w_id}, has W_NAME "{w_name}", has W_STREET_1 "{w_street_1}", 
has W_STREET_2 "{w_street_2}", has W_CITY "{w_city}", has W_STATE "{w_state}", 
has W_ZIP "{w_zip}", has W_TAX {w_tax}, has W_YTD {w_ytd};"""
                    write_query.append(q)

            if tableName == "DISTRICT":
                for tuple in tuples:
                    d_id = tuple[0]
                    d_w_id = tuple[1]
                    d_name = tuple[2]
                    d_street_1 = tuple[3]
                    d_street_2 = tuple[4]
                    d_city = tuple[5]
                    d_state = tuple[6]
                    d_zip = tuple[7]
                    d_tax = tuple[8]
                    d_ytd = tuple[9]
                    d_next_o_id = tuple[10]

                    q = f"""
match 
$w isa WAREHOUSE, has W_ID {d_w_id};
insert 
$district (warehouse: $w) isa DISTRICT,
has D_ID {d_w_id * DPW + d_id}, has D_NAME "{d_name}",
has D_STREET_1 "{d_street_1}", has D_STREET_2 "{d_street_2}",
has D_CITY "{d_city}", has D_STATE "{d_state}", has D_ZIP "{d_zip}",
has D_TAX {d_tax}, has D_YTD {d_ytd}, has D_NEXT_O_ID {d_next_o_id};"""
                    write_query.append(q)

            if tableName == "ITEM":
                for tuple in tuples:
                    i_id = tuple[0]
                    i_im_id = tuple[1]
                    i_name = tuple[2]
                    i_price = tuple[3]
                    i_data = tuple[4]

                    q = f"""
insert 
$item isa ITEM,
has I_ID {i_id}, has I_IM_ID {i_im_id}, has I_NAME "{i_name}",
has I_PRICE {i_price}, has I_DATA "{i_data}";"""
                    write_query.append(q)

            if tableName == "CUSTOMER":
                for tuple in tuples:
                    c_id = tuple[0]
                    c_d_id = tuple[1]
                    c_w_id = tuple[2]
                    c_first = tuple[3]
                    c_middle = tuple[4]
                    c_last = tuple[5]
                    c_street_1 = tuple[6]
                    c_street_2 = tuple[7]
                    c_city = tuple[8]
                    c_state = tuple[9]
                    c_zip = tuple[10]
                    c_phone = tuple[11]
                    c_since = tuple[12].isoformat()[:-3]
                    c_credit = tuple[13]
                    c_credit_lim = tuple[14]
                    c_discount = tuple[15]
                    c_balance = tuple[16]
                    c_ytd_payment = tuple[17]
                    c_payment_cnt = tuple[18]
                    c_delivery_cnt = tuple[19]
                    c_data = tuple[20]

                    q = f"""
match
$d isa DISTRICT, has D_ID {c_w_id * DPW + c_d_id};
insert 
$customer (district: $d) isa CUSTOMER,
has C_ID {c_w_id * DPW * CPD + c_d_id * CPD + c_id}, 
has C_FIRST "{c_first}", has C_MIDDLE "{c_middle}", has C_LAST "{c_last}",
has C_STREET_1 "{c_street_1}", has C_STREET_2 "{c_street_2}",
has C_CITY "{c_city}", has C_STATE "{c_state}", has C_ZIP "{c_zip}",
has C_PHONE "{c_phone}", has C_SINCE {c_since}, has C_CREDIT "{c_credit}",
has C_CREDIT_LIM {c_credit_lim}, has C_DISCOUNT {c_discount},
has C_BALANCE {c_balance}, has C_YTD_PAYMENT {c_ytd_payment},
has C_PAYMENT_CNT {c_payment_cnt}, has C_DELIVERY_CNT {c_delivery_cnt},
has C_DATA "{c_data}";"""
                    write_query.append(q)

            if tableName == "ORDERS":
                for tuple in tuples:
                    o_id = tuple[0]
                    o_c_id = tuple[1]
                    o_d_id = tuple[2]
                    o_w_id = tuple[3]
                    o_entry_d = tuple[4].isoformat()[:-3]
                    o_carrier_id = tuple[5]
                    o_ol_cnt = tuple[6]
                    o_all_local = tuple[7]

                    q = f"""
match 
$c isa CUSTOMER, has C_ID {o_w_id * DPW * CPD + o_d_id * CPD + o_c_id};
insert 
$order (customer: $c) isa ORDER,
has O_ID {o_id},
has O_ENTRY_D {o_entry_d}, has O_CARRIER_ID {o_carrier_id},
has O_OL_CNT {o_ol_cnt}, has O_ALL_LOCAL {o_all_local}, has O_NEW_ORDER false;"""
                    write_query.append(q)

            if tableName == "NEW_ORDER":
                is_update = True;
                for tuple in tuples:
                    no_o_id = tuple[0]
                    no_d_id = tuple[1]
                    no_w_id = tuple[2]

                    q = f"""
match 
$d isa DISTRICT, has D_ID {no_w_id * DPW + no_d_id};
$order (district: $d) isa ORDER, has O_ID {no_o_id}, has O_NEW_ORDER $status;
delete $order has $status;
insert $order has O_NEW_ORDER true;
"""
                    write_query.append(q)

            if tableName == "ORDER_LINE":
                for tuple in tuples:
                    ol_o_id = tuple[0]
                    ol_d_id = tuple[1]
                    ol_w_id = tuple[2]
                    ol_number = tuple[3]
                    ol_i_id = tuple[4]
                    ol_supply_w_id = tuple[5]
                    # See TPCC Spec: delivery date may be null
                    if tuple[6] is not None:
                        has_ol_delivery_d = f"has OL_DELIVERY_D {tuple[6].isoformat()[:-3]},"
                    else:
                        has_ol_delivery_d = ""
                    ol_quantity = tuple[7]
                    ol_amount = tuple[8]
                    ol_dist_info = tuple[9]

                    q = f"""
match 
$w isa WAREHOUSE, has W_ID {ol_w_id};
$d isa DISTRICT, has D_ID {ol_w_id * DPW + ol_d_id};
$order (district: $d) isa ORDER, has O_ID {ol_o_id};
$item has I_ID {ol_i_id};
insert 
$order_line (order: $order, item: $item) isa ORDER_LINE,
has OL_NUMBER {ol_number}, has OL_SUPPLY_W_ID {ol_supply_w_id},
""" + has_ol_delivery_d + f"""
has OL_QUANTITY {ol_quantity}, has OL_AMOUNT {ol_amount},
has OL_DIST_INFO "{ol_dist_info}";
"""
                    write_query.append(q)
    
            if tableName == "STOCK":
                for tuple in tuples:
                    s_i_id = tuple[0]
                    s_w_id = tuple[1]
                    s_quantity = tuple[2]
                    s_ytd = tuple[13]
                    s_order_cnt = tuple[14]
                    s_remote_cnt = tuple[15]
                    s_data = tuple[16]

                    q_stock = f"""
match 
$i isa ITEM, has I_ID {s_i_id};   
$w isa WAREHOUSE, has W_ID {s_w_id};
insert 
$stock (item: $i, warehouse: $w) isa STOCKING, 
has S_QUANTITY {s_quantity}, has S_YTD {s_ytd}, has S_ORDER_CNT {s_order_cnt},
has S_REMOTE_CNT {s_remote_cnt}, has S_DATA "{s_data}";"""
                    write_query.append(q_stock)
    
                    for i in range(1, 11):

                        q_stock_info = f"""
match 
$i isa ITEM, has I_ID {s_i_id};
$w isa WAREHOUSE, has W_ID {s_w_id};   
$stock (item: $i, warehouse: $w) isa STOCKING;
insert
$stock has S_DIST_{i} "{tuple[2+i]}";"""
                        write_query.append(q_stock_info)

    
            if tableName == "HISTORY":
                for tuple in tuples:
                    h_c_id = tuple[0]
                    h_d_id = tuple[3]
                    h_w_id = tuple[4]
                    h_date = tuple[5].isoformat()[:-3]
                    h_amount = tuple[6]
                    h_data = tuple[7]
    
                    q = f"""
match 
$c isa CUSTOMER, has C_ID {h_w_id * DPW * CPD + h_d_id * CPD + h_c_id};
insert 
$history (customer: $c) isa CUSTOMER_HISTORY,
has H_DATE {h_date}, has H_AMOUNT {h_amount}, has H_DATA "{h_data}";"""
                    write_query.append(q)
    
            for query in write_query:
                if is_update:
                    tx.query.update(query)
                else:
                    tx.query.insert(query)

            if tableName == "STOCK":
                full_query = "\n".join(write_query)
                with open(f"full_query_{tableName}.tql", "w") as f:
                    f.write(full_query)
 
            logging.info("Committing %d queries for type %s" % (len(tuples), tableName))
            start_time = time.time()
            tx.commit()
            logging.info(f"Committed! Time per query: {(time.time() - start_time) / len(tuples)}")
        return

    ## ----------------------------------------------
    ## loadFinish
    ## ----------------------------------------------
    def loadFinish(self):
        logging.info("Closing write session")
        self.session.close()
        return None

    ## ----------------------------------------------
    ## doNewOrder
    ## ----------------------------------------------
    def doNewOrder(self, params):
        # For reference, the SQL queries are:
        # q = { 
        #     "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?", # ol_i_id
        #     "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?", # w_id
        #     "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?", # d_id, w_id
        #     "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?", # d_next_o_id, d_id, w_id
        #     "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
        #     "createOrder": "INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
        #     "createNewOrder": "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)", # o_id, d_id, w_id
        #     "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?", # d_id, ol_i_id, ol_supply_w_id
        #     "updateStock": "UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?", # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
        #     "createOrderLine": "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info        
        # }
        
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = params["o_entry_d"]
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]
            
        assert len(i_ids) > 0
        assert len(i_ids) == len(i_w_ids)
        assert len(i_ids) == len(i_qtys)

        all_local = True
        total = 0
        items = [{ }]
        item_data = [ ]
        ## Determine if this is an all local order or not
        for i in range(len(i_ids)):
            all_local = all_local and i_w_ids[i] == w_id
        assert len(items) == len(i_ids)

        with self.driver.session(self.database, SessionType.DATA) as data_session:
            with data_session.transaction(TransactionType.WRITE) as tx:
                for i in range(len(i_ids)):
                    item = tx.query.get(f"match $i isa ITEM, has I_ID {i_ids[i]}, has I_NAME $i_name, has I_PRICE $i_price, has I_DATA $i_data; get $i_name, $i_price, $i_data;")
                    if len(item) == 0:
                        return (None, 0)
                    items[i]['name'] = item.get('i_name').as_attribute().get_value()
                    items[i]['price'] = item.get('i_price').as_attribute().get_value()
                    items[i]['data'] = item.get('i_data').as_attribute().get_value()

                # Query: get warhouse, district, and customer info
                # TODO: potentially remove conditions for speed
                q = f"""
match 
$w isa WAREHOUSE, has W_ID {w_id}, has W_TAX $w_tax;
$d isa DISTRICT, has D_ID {w_id * DPW + d_id}, has D_TAX $d_tax, has D_NEXT_O_ID $d_next_o_id;
$c isa CUSTOMER, has C_ID {w_id * DPW * CPD + d_id * CPD + c_id}, has C_DISCOUNT $c_discount, has C_LAST $c_last, has C_CREDIT $c_credit;
(customer: $c, district: $d) isa CUSTOMER_LOCATION;
get $w_tax, $d_tax, $d_next_o_id, $c_discount, $c_last, $c_credit;"""
                general_info = tx.query.get(q)
                
                if len(general_info) == 0:
                    logging.warn("No general info for warehouse %d" % w_id)
                    return (None, 0)
                w_tax = general_info[0].get('w_tax').as_attribute().get_value()
                d_tax = general_info[0].get('d_tax').as_attribute().get_value()
                d_next_o_id = general_info[0].get('d_next_o_id').as_attribute().get_value()
                c_discount = general_info[0].get('c_discount').as_attribute().get_value()
                c_last = general_info[0].get('c_last').as_attribute().get_value()
                c_credit = general_info[0].get('c_credit').as_attribute().get_value()

                ol_cnt = len(i_ids)
                o_carrier_id = constants.NULL_CARRIER_ID

                # Query: update district's next order id, and create new order
                # TODO: experiment with constraining further
                q = f"""
match 
$d isa DISTRICT, has D_ID {w_id * DPW + d_id}, has D_NEXT_O_ID $d_next_o_id;
$c isa CUSTOMER, has C_ID {w_id * DPW * CPD + d_id * CPD + c_id};
delete 
$d has $d_next_o_id;
insert 
$d has D_NEXT_O_ID {d_next_o_id + 1};
$order (district: $d, customer: $c) isa ORDER,
has O_ID {d_next_o_id},
has O_ENTRY_D {o_entry_d}, has O_CARRIER_ID {o_carrier_id},
has O_OL_CNT {ol_cnt}, has O_ALL_LOCAL {all_local}, has O_NEW_ORDER true;"""
                tx.query.update(q)

                for i in range(len(i_ids)):
                    ol_number = i + 1
                    ol_supply_w_id = i_w_ids[i]
                    ol_i_id = i_ids[i]
                    ol_quantity = i_qtys[i]

                    i_name = items[i]['name']
                    i_data = items[i]['data']
                    i_price = items[i]['price']

                    # Query: get stock info of item i
                    q = f"""
match
$i isa ITEM, has I_ID {ol_i_id}; 
$w isa WAREHOUSE, has W_ID {ol_supply_w_id};
$s (item: $i, warehouse: $w) isa STOCKING, 
    has S_QUANTITY $s_quantity, has S_DATA $s_data, has S_YTD $s_ytd, 
    has S_ORDER_CNT $s_order_cnt, has S_REMOTE_CNT $s_remote_cnt, 
    has S_DIST_{d_id} $s_dist_xx;
get $s_quantity, $s_data, $s_ytd, $s_order_cnt, $s_remote_cnt, $s_dist_xx;"""
                    stock_info = tx.query.get(q)

                    if len(stock_info) == 0:
                        logging.warn("No STOCK record for (ol_i_id=%d, ol_supply_w_id=%d)" % (ol_i_id, ol_supply_w_id))
                        continue
                    s_quantity = stock_info[0].get('s_quantity').as_attribute().get_value()
                    s_data = stock_info[0].get('s_data').as_attribute().get_value()
                    s_ytd = stock_info[0].get('s_ytd').as_attribute().get_value()
                    s_order_cnt = stock_info[0].get('s_order_cnt').as_attribute().get_value()
                    s_remote_cnt = stock_info[0].get('s_remote_cnt').as_attribute().get_value()
                    s_dist_xx = stock_info[0].get('s_dist_xx').as_attribute().get_value()
                    
                    # Compute auxilliary values
                    s_ytd += ol_quantity
                    if s_quantity >= ol_quantity + 10:
                        s_quantity = s_quantity - ol_quantity
                    else:
                        s_quantity = s_quantity + 91 - ol_quantity
                    s_order_cnt += 1
                    
                    if ol_supply_w_id != w_id: s_remote_cnt += 1

                    if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
                        brand_generic = 'B'
                    else:
                        brand_generic = 'G'

                    # Query: update stock info of item i
                    q = f"""
match
$i isa ITEM, has I_ID {ol_i_id};
$w isa WAREHOUSE, has W_ID {ol_supply_w_id};
$d isa DISTRICT, has D_ID {w_id * DPW + d_id};
$o (district: $d) isa ORDER, has O_ID {d_next_o_id};
$s (item: $i, warehouse: $w) isa STOCKING, has S_QUANTITY $s_quantity;
delete $s has $s_quantity;
insert $s has S_QUANTITY {s_quantity}, has S_YTD {s_ytd}, 
has S_ORDER_CNT {s_order_cnt}, has S_REMOTE_CNT {s_remote_cnt};
(item: $i, order: $o) isa ORDER_LINE, 
has OL_NUMBER {ol_number}, has OL_SUPPLY_W_ID {ol_supply_w_id}, 
has OL_DELIVERY_D {o_entry_d}, has OL_QUANTITY {ol_quantity}, 
has OL_AMOUNT {ol_amount}, has OL_DIST_INFO {s_dist_xx};"""
                    tx.query.update(q)

                    ## Transaction profile states to use "ol_quantity * i_price"
                    ol_amount = ol_quantity * i_price
                    total += ol_amount
        
                    ## Add the info to be returned
                    item_data.append( (i_name, s_quantity, brand_generic, i_price, ol_amount) )
                ## FOR
                tx.commit()
                total *= (1 - c_discount) * (1 + w_tax + d_tax)

                ## Pack up values the client is missing (see TPC-C 2.4.3.5)
                misc = [ (w_tax, d_tax, d_next_o_id, total) ]
                return ([ [c_discount, c_last, c_credit], misc, item_data ], 0)
            ## WITH
        ## WITH

    ## ----------------------------------------------
    ## doDelivery
    ## ----------------------------------------------
    def doDelivery(self, params):
        q = { 
            "getNewOrder": "SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1", #
            "getCId": "SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", # no_o_id, d_id, w_id
            "sumOLAmount": "SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # no_o_id, d_id, w_id
            "deleteNewOrder": "DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?", # d_id, w_id, no_o_id
            "updateOrders": "UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", # o_carrier_id, no_o_id, d_id, w_id
            "updateOrderLine": "UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # o_entry_d, no_o_id, d_id, w_id
            "updateCustomer": "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?", # ol_total, c_id, d_id, w_id
        }

        
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["ol_delivery_d"].isoformat()[:-3]

        with self.driver.session(self.database, SessionType.DATA) as data_session:
            with data_session.transaction(TransactionType.WRITE) as tx:
                result = [ ]
                for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):
                    q = f"""
match
$d isa DISTRICT, has D_ID {w_id * DPW + d_id};
$o (customer: $c, district: $d) isa ORDER, has O_ID $o_id, has O_NEW_ORDER true;
$c has C_ID $c_id;
get $o_id, $c_id;
"""
                    new_order_info = tx.query.get(q)
                    if len(new_order_info) == 0:
                        ## No orders for this district: skip it. Note: This must be reported if > 1%
                        continue
                    assert len(new_order_info) == 1
                    no_o_id = new_order_info[0].get('o_id').as_attribute().get_value()
                    c_id = new_order_info[0].get('c_id').as_attribute().get_value()

                    q = f"""
match
$d isa DISTRICT, has D_ID {w_id * DPW + d_id};
$o (district: $d) isa ORDER, has O_ID {no_o_id};
$ol (order: $o, item: $i) isa ORDER_LINE, has OL_QUANTITY $ol_quantity;
get $ol_quantity;
sum;
"""
                    ol_total = tx.query.get(q)
                    assert len(ol_total) == 1
                    ol_total = ol_total[0].get('ol_quantity').as_attribute().get_value()
                    
                    q = f"""
match
$c isa CUSTOMER, has C_ID {w_id * DPW * CPD + d_id * CPD + c_id}, has C_BALANCE $c_balance;
?c_balance_new = $c_balance + {ol_total};
$o (customer: $c) isa ORDER, has O_ID {no_o_id}, has O_NEW_ORDER $o_new_order, has O_CARRIER_ID $o_carrier_id;
delete 
$o has $o_new_order, $o_carrier_id;
$c has $c_balance;
insert 
$o has O_NEW_ORDER false, has O_CARRIER_ID {o_carrier_id};
$c has C_BALANCE ?c_balance_new;
"""
                    tx.query.update(q)

                    q = f"""
match
$d isa DISTRICT, has D_ID {w_id * DPW + d_id};
$o (district: $d) isa ORDER, has O_ID {no_o_id};
$ol (order: $o, item: $i) isa ORDER_LINE;
insert
$ol has OL_DELIVERY_D {ol_delivery_d};
"""
                    tx.query.insert(q)

                    # If there are no order lines, SUM returns null. There should always be order lines.
                    assert ol_total != None, "ol_total is NULL: there are no order lines. This should not happen"
                    assert ol_total > 0.0

                    # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
                    # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
                    # them out
                    result.append((d_id, no_o_id))
                ## FOR

                self.driver.commit()
        return (result,0)

    ## ----------------------------------------------
    ## doOrderStatus
    ## ----------------------------------------------
    def doOrderStatus(self, params):
        q = TXN_QUERIES["ORDER_STATUS"]
        
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        
        assert w_id, pformat(params)
        assert d_id, pformat(params)

        if c_id != None:
            self.cursor.execute(q["getCustomerByCustomerId"], [w_id, d_id, c_id])
            customer = self.cursor.fetchone()
        else:
            # Get the midpoint customer's id
            self.cursor.execute(q["getCustomersByLastName"], [w_id, d_id, c_last])
            all_customers = self.cursor.fetchall()
            assert len(all_customers) > 0
            namecnt = len(all_customers)
            index = (namecnt-1) // 2
            customer = all_customers[index]
            c_id = customer[0]
        assert customer is not None
        assert c_id != None

        self.cursor.execute(q["getLastOrder"], [w_id, d_id, c_id])
        order = self.cursor.fetchone()
        if order:
            self.cursor.execute(q["getOrderLines"], [w_id, d_id, order[0]])
            orderLines = self.cursor.fetchall()
        else:
            orderLines = [ ]

        self.driver.commit()
        return ([ customer, order, orderLines ],0)

    ## ----------------------------------------------
    ## doPayment
    ## ----------------------------------------------    
    def doPayment(self, params):
        q = TXN_QUERIES["PAYMENT"]

        w_id = params["w_id"]
        d_id = params["d_id"]
        h_amount = params["h_amount"]
        c_w_id = params["c_w_id"]
        c_d_id = params["c_d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        h_date = params["h_date"]

        if c_id != None:
            self.cursor.execute(q["getCustomerByCustomerId"], [w_id, d_id, c_id])
            customer = self.cursor.fetchone()
        else:
            # Get the midpoint customer's id
            self.cursor.execute(q["getCustomersByLastName"], [w_id, d_id, c_last])
            all_customers = self.cursor.fetchall()
            assert len(all_customers) > 0
            namecnt = len(all_customers)
            index = (namecnt-1) // 2
            customer = all_customers[index]
            c_id = customer[0]
        assert customer is not None
        c_balance = customer[14] - h_amount
        c_ytd_payment = customer[15] + h_amount
        c_payment_cnt = customer[16] + 1
        c_data = customer[17]

        self.cursor.execute(q["getWarehouse"], [w_id])
        warehouse = self.cursor.fetchone()
        
        self.cursor.execute(q["getDistrict"], [w_id, d_id])
        district = self.cursor.fetchone()
        
        self.cursor.execute(q["updateWarehouseBalance"], [h_amount, w_id])
        self.cursor.execute(q["updateDistrictBalance"], [h_amount, w_id, d_id])

        # Customer Credit Information
        if customer[11] == constants.BAD_CREDIT:
            newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
            c_data = (newData + "|" + c_data)
            if len(c_data) > constants.MAX_C_DATA: c_data = c_data[:constants.MAX_C_DATA]
            self.cursor.execute(q["updateBCCustomer"], [c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id])
        else:
            c_data = ""
            self.cursor.execute(q["updateGCCustomer"], [c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id])

        # Concatenate w_name, four spaces, d_name
        h_data = "%s    %s" % (warehouse[0], district[0])
        # Create the history record
        self.cursor.execute(q["insertHistory"], [c_id, c_d_id, c_w_id, d_id, w_id, h_date, h_amount, h_data])

        self.driver.commit()

        # TPC-C 2.5.3.3: Must display the following fields:
        # W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
        # D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
        # C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
        # C_DISCOUNT, C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
        # H_AMOUNT, and H_DATE.

        # Hand back all the warehouse, district, and customer data
        return ([ warehouse, district, customer ],0)
        
    ## ----------------------------------------------
    ## doStockLevel
    ## ----------------------------------------------    
    def doStockLevel(self, params):
        q = TXN_QUERIES["STOCK_LEVEL"]

        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]
        
        self.cursor.execute(q["getOId"], [w_id, d_id])
        result = self.cursor.fetchone()
        assert result is not None
        o_id = result[0]
        
        self.cursor.execute(q["getStockCount"], [w_id, d_id, o_id, (o_id - 20), w_id, threshold])
        result = self.cursor.fetchone()
        
        self.driver.commit()
        
        return (int(result[0]),0)
        
## CLASS