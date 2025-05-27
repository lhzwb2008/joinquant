import json
import pymysql
import pandas as pd
import time
from datetime import datetime, time as dt_time

# Trading configuration
EXECUTION_RATIO = 0.1  # Execute ratio of original order quantity (0.1 = 10%)

def init(ContextInfo):
    global position_flag, delete_flag, order_flag
    
    position_flag = False
    delete_flag = True
    order_flag = True
    account = "410038217129"
    ContextInfo.accID = str(account)
    ContextInfo.set_account(ContextInfo.accID)
    
    print('init - start continuous monitoring mode (EXECUTION RATIO: {}%)'.format(int(EXECUTION_RATIO * 100)))
    
    start_continuous_monitoring(ContextInfo)

def get_data(query_str):
    today_date = datetime.today().date()
    today_date = today_date.strftime('%Y-%m-%d')
    host = "sh-cdb-kgv8etuq.sql.tencentcdb.com"
    port = 23333
    user = "root"
    password = "Hello2025"
    database = 'order'
    
    try:
        conn = pymysql.connect(host=host, port=port, user=user,
                               password=password, database=database,
                               charset='utf8')
        cursor = conn.cursor()
        
        cursor.execute(query_str)
        result = cursor.fetchall()
        
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
        conn = pymysql.connect(host=host, port=port, user=user,
                               password=password, database=database,
                               charset='utf8')
        cursor = conn.cursor()
        
        cursor.execute(query_str)
        conn.commit()
        cursor.close()
        conn.close()
        print('Deleted old data from database')
    except Exception as e:
        print('Error: {}'.format(e))

def mark_order_as_executed(order_id):
    host = "sh-cdb-kgv8etuq.sql.tencentcdb.com"
    port = 23333
    user = "root"
    password = "Hello2025"
    database = 'order'
    
    try:
        conn = pymysql.connect(host=host, port=port, user=user,
                               password=password, database=database,
                               charset='utf8')
        cursor = conn.cursor()
        
        update_query = """UPDATE `order`.joinquant_stock SET if_deal = 1 WHERE pk = %s"""
        cursor.execute(update_query, (order_id,))
        affected_rows = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        
        if affected_rows > 0:
            print('Order {} marked as executed'.format(order_id))
            return True
        else:
            print('Warning: Order {} not found or already executed'.format(order_id))
            return False
    except Exception as e:
        print('Failed to mark order execution status: {}'.format(e))
        return False

def revert_order_status(order_id):
    host = "sh-cdb-kgv8etuq.sql.tencentcdb.com"
    port = 23333
    user = "root"
    password = "Hello2025"
    database = 'order'
    
    try:
        conn = pymysql.connect(host=host, port=port, user=user,
                               password=password, database=database,
                               charset='utf8')
        cursor = conn.cursor()
        
        update_query = """UPDATE `order`.joinquant_stock SET if_deal = 0 WHERE pk = %s"""
        cursor.execute(update_query, (order_id,))
        conn.commit()
        cursor.close()
        conn.close()
        print('Order {} status reverted to pending'.format(order_id))
    except Exception as e:
        print('Failed to revert order status: {}'.format(e))

def execute_trade_orders(ContextInfo):
    current_time = datetime.now().time()
    
    day_start_time = dt_time(9, 30)
    day_end_time = dt_time(15, 00)
    
    if not (day_start_time <= current_time <= day_end_time):
        return False
    
    buy_direction = 23
    sell_direction = 24
    
    query_str = """SELECT * FROM `order`.joinquant_stock WHERE if_deal = 0"""
    
    try:
        orders_df = get_data(query_str)
    except Exception as e:
        orders_df = pd.DataFrame()
        print('Error occurred: {}'.format(e))
        return False
    
    if len(orders_df) < 1:
        return False
    
    print('Found {} pending orders'.format(len(orders_df)))
    
    position_info = get_trade_detail_data(ContextInfo.accID, 'stock', 'position')
    position_code = []
    position_volume = {}
    if len(position_info) > 0:
        for ele in position_info:
            if ele.m_nVolume > 0:
                position_code.append(ele.m_strInstrumentID)
                position_volume[ele.m_strInstrumentID] = ele.m_nVolume
    
    executed_orders = []
    
    for idx, order in orders_df.iterrows():
        code = order['code']
        ordertype = order['ordertype']
        order_values = int(order['order_values'])
        # Use 'pk' as the primary key field
        order_id = order.get('pk', None)
        
        if not order_id:
            print('Warning: Order missing PK, skipping')
            continue
        
        # Mark order as executed BEFORE placing order to prevent duplicates
        if not mark_order_as_executed(order_id):
            print('Failed to mark order {} as executed, skipping to prevent duplicates'.format(order_id))
            continue
        
        # Apply execution ratio
        original_order_values = order_values
        order_values = int(order_values * EXECUTION_RATIO)
        
        # Round down to nearest 100
        order_values = (order_values // 100) * 100
        
        # Skip if less than 100 shares
        if order_values < 100:
            print('Order {} skipped: {} shares after ratio adjustment is less than 100'.format(
                code, int(original_order_values * EXECUTION_RATIO)))
            # Revert the order status since we're not executing it
            revert_order_status(order_id)
            continue
        
        print('Order {} adjusted from {} to {} shares (ratio: {}%)'.format(
            code, original_order_values, order_values, int(EXECUTION_RATIO * 100)))
        
        try:
            if ordertype == u'\u4e70':  # Buy
                if order_values > 0:
                    result = passorder(buy_direction, 1101, ContextInfo.accID, code, 5, 0, order_values, '', 2, '', ContextInfo)
                    print('Execute buy order: {} x {} shares'.format(code, order_values))
                    executed_orders.append(order_id)
            
            elif ordertype == u'\u5356':  # Sell
                if code in position_volume and position_volume[code] > 0:
                    sell_amount = min(order_values, position_volume[code])
                    if sell_amount > 0:
                        result = passorder(sell_direction, 1101, ContextInfo.accID, code, 5, 0, sell_amount, '', 2, '', ContextInfo)
                        print('Execute sell order: {} x {} shares'.format(code, sell_amount))
                        executed_orders.append(order_id)
                        position_volume[code] -= sell_amount
                else:
                    print('Warning: Insufficient position for {} to sell {} shares'.format(code, order_values))
        
        except Exception as e:
            print('Failed to execute order {}: {}'.format(code, e))
            # If order execution failed, try to revert the database status
            revert_order_status(order_id)
    
    return len(executed_orders) > 0

def start_continuous_monitoring(ContextInfo):
    print('Start continuous monitoring for trading signals...')
    
    last_cleanup_date = datetime.now().date()
    
    while True:
        try:
            current_time = datetime.now().time()
            current_date = datetime.now().date()
            
            cleanup_time = dt_time(15, 35)
            if (current_time >= cleanup_time and 
                current_date > last_cleanup_date):
                print('Execute daily data cleanup...')
                delete_data()
                last_cleanup_date = current_date
            
            if execute_trade_orders(ContextInfo):
                print('Trade orders executed')
            
            time.sleep(2)
            
        except KeyboardInterrupt:
            print('Monitoring stopped')
            break
        except Exception as e:
            print('Error during monitoring: {}'.format(e))
            time.sleep(5)

def handlebar(ContextInfo):
    pass