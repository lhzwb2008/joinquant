# -*- coding: utf-8 -*-
"""
iQuant读取聚宽订单信息并执行交易
"""
import pymysql
import pandas as pd
from datetime import datetime, time

def get_data(query_str):
    """从数据库获取订单信息"""
    today_date = datetime.today().date()
    today_date = today_date.strftime('%Y-%m-%d')
    
    # 数据库连接参数
    host = "sh-cdb-kgv8etuq.sql.tencentcdb.com"
    port = 23333
    user = "root"
    password = "Hello2025"
    database = 'order'
    
    try:
        # 连接 MySQL 数据库
        conn = pymysql.connect(host=host, port=port, user=user,
                               password=password, database=database,
                               charset='utf8mb4')
        cursor = conn.cursor()

        # 执行 SQL 查询语句
        cursor.execute(query_str)
        # 获取查询结果
        result = cursor.fetchall()

        # 将查询结果转化为 Pandas dataframe 对象
        if result:
            res = pd.DataFrame([result[i] for i in range(len(result))], 
                              columns=[i[0] for i in cursor.description])
            # 筛选今天的订单
            res['tradedate'] = res['tradetime'].apply(lambda x: x.strftime('%Y-%m-%d'))
            res = res[res['tradedate'] == today_date]
        else:
            res = pd.DataFrame()
        
        cursor.close()
        conn.close()
        
        return res
    except Exception as e:
        print('数据库查询错误：%s' % str(e))
        return pd.DataFrame()

def update_order_status(pk_list):
    """更新订单状态为已处理"""
    if not pk_list:
        return
        
    host = "sh-cdb-kgv8etuq.sql.tencentcdb.com"
    port = 23333
    user = "root"
    password = "Hello2025"
    database = 'order'
    
    try:
        conn = pymysql.connect(host=host, port=port, user=user,
                               password=password, database=database,
                               charset='utf8mb4')
        cursor = conn.cursor()
        
        # 批量更新订单状态
        for pk in pk_list:
            query_str = "UPDATE `order`.`joinquant_stock` SET if_deal = 1 WHERE pk = '%s'" % pk
            cursor.execute(query_str)
        
        conn.commit()
        cursor.close()
        conn.close()
        print('已更新%d条订单状态' % len(pk_list))
    except Exception as e:
        print('更新订单状态错误：%s' % str(e))

def init(ContextInfo):
    """iQuant初始化函数"""
    # 设置定时运行，每天14:00执行一次
    ContextInfo.run_time("check_orders", "1nDay", "2019-10-14 14:00:00", "SH")
    
    # 设置账户
    account = "xxxxxxxx"  # 替换为实际账户
    ContextInfo.accID = str(account)
    ContextInfo.set_account(ContextInfo.accID)
    
    # 初始化标志位
    ContextInfo.order_executed_today = False
    
    print('iQuant订单执行器初始化完成')

def check_orders(ContextInfo):
    """每天14:00检查并执行订单"""
    current_time = datetime.now()
    current_hour = current_time.hour
    current_date = current_time.strftime('%Y-%m-%d')
    
    # 只在14:00执行
    if current_hour != 14:
        return
    
    # 检查今天是否已经执行过
    if hasattr(ContextInfo, 'last_execute_date') and ContextInfo.last_execute_date == current_date:
        if ContextInfo.order_executed_today:
            return
    else:
        ContextInfo.last_execute_date = current_date
        ContextInfo.order_executed_today = False
    
    # 查询今天未处理的订单
    query_str = """SELECT * FROM `order`.`joinquant_stock` WHERE if_deal = 0 AND DATE(tradetime) = '%s'""" % current_date
    
    try:
        orders_df = get_data(query_str)
        
        if len(orders_df) == 0:
            print('今天没有待处理的订单')
            return
        
        print('发现%d条待处理订单' % len(orders_df))
        
        # 获取当前持仓
        position_info = get_trade_detail_data(ContextInfo.accID, 'stock', 'position')
        current_positions = {}
        for pos in position_info:
            if pos.m_nVolume > 0:
                current_positions[pos.m_strInstrumentID] = pos.m_nVolume
        
        # 成功执行的订单列表
        success_orders = []
        
        # 处理每个订单
        for idx, order in orders_df.iterrows():
            pk = order['pk']
            code = order['code']
            ordertype = order['ordertype']
            order_values = order['order_values']
            
            if ordertype == '买':
                # 计算买入数量
                if order_values == 0:  # 如果聚宽没有指定数量，根据资金计算
                    acc_info = get_trade_detail_data(ContextInfo.accID, 'stock', 'account')
                    available_cash = acc_info[0].m_dAvailable
                    # 假设平均分配资金
                    buy_orders = orders_df[orders_df['ordertype'] == '买']
                    if len(buy_orders) > 0:
                        per_stock_cash = available_cash / len(buy_orders)
                        # 获取最新价格
                        price_data = ContextInfo.get_market_data(['close'], stock_code=[code], period='1d', count=1)
                        if price_data and code in price_data:
                            last_price = price_data[code].iloc[-1]
                            order_values = int(per_stock_cash / last_price / 100) * 100  # 按手取整
                
                if order_values > 0:
                    # 执行买入
                    order_id = passorder(23, 1101, ContextInfo.accID, code, 11, -1, order_values, '', 2, '', ContextInfo)
                    if order_id:
                        print('买入 %s，数量：%d' % (code, order_values))
                        success_orders.append(pk)
                
            elif ordertype == '卖':
                # 检查持仓
                if code in current_positions:
                    sell_amount = min(order_values, current_positions[code])
                    if sell_amount > 0:
                        # 执行卖出
                        order_id = passorder(24, 1101, ContextInfo.accID, code, 11, -1, sell_amount, '', 2, '', ContextInfo)
                        if order_id:
                            print('卖出 %s，数量：%d' % (code, sell_amount))
                            success_orders.append(pk)
        
        # 更新成功执行的订单状态
        if success_orders:
            update_order_status(success_orders)
            ContextInfo.order_executed_today = True
        
    except Exception as e:
        print('处理订单时发生错误：%s' % str(e))

def handlebar(ContextInfo):
    """主函数"""
    pass 