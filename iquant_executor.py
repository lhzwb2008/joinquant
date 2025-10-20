import json
import pymysql
import pandas as pd
import time
from datetime import datetime, time as dt_time

# Trading configuration
EXECUTION_RATIO = 1  # Execute ratio of original order quantity (0.1 = 10%)

# Price type configuration - based on API documentation prType parameter
PRICE_TYPE = 12  # Recommended: 12=market price, ensures immediate execution
"""
prType price type options:
0-10: Sell5 to Buy5 price levels
5: Latest price (may not execute immediately)
12: Market price (recommended, immediate execution)
14: Counter price (buy uses sell1, sell uses buy1)

If market order still fails, try:
PRICE_TYPE = 5   # Latest price
PRICE_TYPE = 14  # Counter price
"""

def init(ContextInfo):
    global position_flag, delete_flag, order_flag
    
    position_flag = False
    delete_flag = True
    order_flag = True
    account = "330200009169"
    ContextInfo.accID = str(account)
    ContextInfo.set_account(ContextInfo.accID)
    
    print('init - start continuous monitoring mode (EXECUTION RATIO: {}%)'.format(int(EXECUTION_RATIO * 100)))
    print('Price type: {} (12=market, 5=latest, 14=counter)'.format(PRICE_TYPE))
    
    start_continuous_monitoring(ContextInfo)

def normalize_stock_code(code):
    """
    Normalize stock code format
    Convert code with suffix (e.g. 603216.SH) to pure number format (e.g. 603216)
    """
    if isinstance(code, str):
        # Remove suffix if code contains dot
        if '.' in code:
            return code.split('.')[0]
        return code
    return str(code)

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
    
    # Check order quantity limit
    if len(orders_df) >= 10:
        print('WARNING: Found {} pending orders (>= 10), skipping execution for safety! This may indicate an abnormal batch order situation.'.format(len(orders_df)))
        return False
    
    position_info = get_trade_detail_data(ContextInfo.accID, 'stock', 'position')
    position_code = []
    position_volume = {}
    if len(position_info) > 0:
        print('Position info found: {} positions'.format(len(position_info)))
        for ele in position_info:
            if ele.m_nVolume > 0:
                # Print raw code format for debugging
                print('Position: {} (raw code: {}, volume: {})'.format(
                    ele.m_strInstrumentName if hasattr(ele, 'm_strInstrumentName') else 'Unknown',
                    ele.m_strInstrumentID,
                    ele.m_nVolume
                ))
                # Normalize position code format
                normalized_code = normalize_stock_code(ele.m_strInstrumentID)
                position_code.append(normalized_code)
                position_volume[normalized_code] = ele.m_nVolume
    
    executed_orders = []
    
    # Separate buy and sell orders
    sell_orders = []
    buy_orders = []
    
    for idx, order in orders_df.iterrows():
        ordertype = order['ordertype']
        if ordertype == u'\u5356':  # Sell
            sell_orders.append(order)
        elif ordertype == u'\u4e70':  # Buy
            buy_orders.append(order)
    
    # Process sell orders first
    print('Processing {} sell orders first'.format(len(sell_orders)))
    for order in sell_orders:
        order_id = process_single_order(order, ContextInfo, position_volume, executed_orders, sell_direction, buy_direction)
    
    # Wait 3 seconds before processing buy orders
    if len(buy_orders) > 0:
        print('Waiting 3 seconds before processing buy orders...')
        time.sleep(3)
        print('Processing {} buy orders'.format(len(buy_orders)))
        for order in buy_orders:
            order_id = process_single_order(order, ContextInfo, position_volume, executed_orders, sell_direction, buy_direction)
    
    return len(executed_orders) > 0

def process_single_order(order, ContextInfo, position_volume, executed_orders, sell_direction, buy_direction):
    """Process a single order (buy or sell)"""
    code = order['code']
    # Normalize order stock code
    normalized_code = normalize_stock_code(code)
    print('Processing order: {} -> normalized: {}'.format(code, normalized_code))
    ordertype = order['ordertype']
    order_values = int(order['order_values'])
    # Use 'pk' as the primary key field
    order_id = order.get('pk', None)
    
    if not order_id:
        print('Warning: Order missing PK, skipping')
        return None
    
    # Mark order as executed BEFORE placing order to prevent duplicates
    if not mark_order_as_executed(order_id):
        print('Failed to mark order {} as executed, skipping to prevent duplicates'.format(order_id))
        return None
    
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
        return None
    
    print('Order {} adjusted from {} to {} shares (ratio: {}%)'.format(
        code, original_order_values, order_values, int(EXECUTION_RATIO * 100)))
    
    try:
        if ordertype == u'\u4e70':  # Buy
            if order_values > 0:
                # Use market price for immediate execution (prType=12)
                result = passorder(buy_direction, 1101, ContextInfo.accID, normalized_code, PRICE_TYPE, 0, order_values, '', 2, '', ContextInfo)
                print('Execute buy order (prType={}): {} x {} shares'.format(PRICE_TYPE, normalized_code, order_values))
                executed_orders.append(order_id)
        
        elif ordertype == u'\u5356':  # Sell
            # Use normalized code to check position
            if normalized_code in position_volume and position_volume[normalized_code] > 0:
                sell_amount = min(order_values, position_volume[normalized_code])
                if sell_amount > 0:
                    result = passorder(sell_direction, 1101, ContextInfo.accID, normalized_code, PRICE_TYPE, 0, sell_amount, '', 2, '', ContextInfo)
                    print('Execute sell order (prType={}): {} x {} shares'.format(PRICE_TYPE, normalized_code, sell_amount))
                    executed_orders.append(order_id)
                    position_volume[normalized_code] -= sell_amount
            else:
                print('Warning: Insufficient position for {} (normalized: {}) to sell {} shares'.format(code, normalized_code, order_values))
        
    except Exception as e:
        print('Failed to execute order {} (normalized: {}): {}'.format(code, normalized_code, e))
        # If order execution failed, try to revert the database status
        revert_order_status(order_id)
    
    return order_id

def start_continuous_monitoring(ContextInfo):
    print('Start continuous monitoring for trading signals...')
    
    while True:
        try:
            current_time = datetime.now().time()
            current_date = datetime.now().date()
            
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


