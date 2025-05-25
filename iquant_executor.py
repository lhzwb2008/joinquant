# coding: utf-8
import json
import pymysql
import pandas as pd
from datetime import datetime, time

def init(ContextInfo):
    global position_flag, delete_flag, order_flag
    
    ContextInfo.run_time("handlebar", "3nSecond", "2024-10-14 13:56:00", "SH")
    
    position_flag = False
    delete_flag = True
    order_flag = True
    account = "410038217129"
    ContextInfo.accID = str(account)
    ContextInfo.set_account(ContextInfo.accID)
    
    print('init')

def get_data(query_str):
    today_date = datetime.today().date()
    today_date = today_date.strftime('%Y-%m-%d')
    host = "sh-cdb-kgv8etuq.sql.tencentcdb.com"
    port = 23333
    user = "root"
    password = "Hello2025"
    database = 'order'
    
    try:
        # Connect to MySQL database
        conn = pymysql.connect(host=host, port=port, user=user,
                               password=password, database=database,
                               charset='utf8')
        cursor = conn.cursor()
        
        # Execute SQL query
        cursor.execute(query_str)
        # Get query results
        result = cursor.fetchall()
        
        # Convert query results to Pandas dataframe
        if result:
            res = pd.DataFrame([result[i] for i in range(len(result))], 
                              columns=[i[0] for i in cursor.description])
            res['tradedate'] = res['tradetime'].apply(lambda x: x.strftime('%Y-%m-%d'))
            res = res[res['tradedate'] == today_date]
        else:
            res = pd.DataFrame()
        
        cursor.close()
        conn.close()
        
        return res
    except Exception as e:
        print('Database query error: {}'.format(e))
        return pd.DataFrame()

def delete_data():
    query_str = """DELETE FROM `order`.joinquant_stock WHERE DATE(tradetime) < CURDATE()"""
    host = "sh-cdb-kgv8etuq.sql.tencentcdb.com"
    port = 23333
    user = "root"
    password = "Hello2025"
    database = 'order'
    
    try:
        # Connect to MySQL database
        conn = pymysql.connect(host=host, port=port, user=user,
                               password=password, database=database,
                               charset='utf8')
        cursor = conn.cursor()
        
        # Execute SQL query
        cursor.execute(query_str)
        conn.commit()
        cursor.close()
        conn.close()
        print('Deleted old data from database')
    except Exception as e:
        print('Error: {}'.format(e))


def handlebar(ContextInfo):
    global position_flag, delete_flag, order_flag
    
    current_time = datetime.now().time()
    
    # Set start and end times
    morning_start_time = time(9, 30, 5)
    morning_end_time = time(9, 32)
    
    morning_delete_database_start = time(11, 29)
    morning_delete_database_end = time(11, 30)
    
    afternoon_delete_database_start = time(15, 25)
    afternoon_delete_database_end = time(15, 30)
    
    buy_direction = 23
    sell_direction = 24
    SALE3 = 2
    BUY3 = 8
    
    day_start_time = time(9, 29)
    day_end_time = time(15, 30)
    
    if day_start_time <= current_time and current_time <= day_end_time:
        
        query_str = """SELECT * FROM `order`.joinquant_stock WHERE if_deal = 0"""
        
        try:
            orders_df = get_data(query_str)
        except Exception as e:
            orders_df = pd.DataFrame()
            print('Error occurred: {}'.format(e))
        
        if len(orders_df) < 1:
            return
        
        if morning_start_time <= current_time and current_time <= morning_end_time:
            
            position_flag = True
            delete_flag = True
            
            if order_flag == False:
                return
            
            # Get current positions
            position_info = get_trade_detail_data(ContextInfo.accID, 'stock', 'position')
            position_code = []
            position_volume = {}
            if len(position_info) > 0:
                for ele in position_info:
                    if ele.m_nVolume > 0:
                        position_code.append(ele.m_strInstrumentID)
                        position_volume[ele.m_strInstrumentID] = ele.m_nVolume
            
            # Process orders
            for idx, order in orders_df.iterrows():
                code = order['code']
                ordertype = order['ordertype']
                order_values = int(order['order_values'])
                
                if ordertype == u'\u4e70':  # Buy
                    if order_values > 0:
                        passorder(buy_direction, 1101, ContextInfo.accID, code, 11, -1, order_values, '', 2, '', ContextInfo)
                        print('Buy order: {} x {}'.format(code, order_values))
                
                elif ordertype == u'\u5356':  # Sell
                    if code in position_volume and position_volume[code] > 0:
                        sell_amount = min(order_values, position_volume[code])
                        if sell_amount > 0:
                            passorder(sell_direction, 1101, ContextInfo.accID, code, 11, -1, sell_amount, '', 2, '', ContextInfo)
                            print('Sell order: {} x {}'.format(code, sell_amount))
            
            order_flag = False
        
        elif (morning_delete_database_start < current_time < morning_delete_database_end) or \
             (afternoon_delete_database_start < current_time < afternoon_delete_database_end):
            order_flag = True
            if delete_flag == True:
                delete_data()
                delete_flag = False