# -*- coding: utf-8 -*-
from jqdata import *
from jqlib.technical_analysis import *
import numpy as np
import pandas as pd
import datetime
import uuid
# 聚宽平台使用内置的sqlalchemy
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# 创建一个基类，用于声明数据模型
Base = declarative_base()

class JoinQuantTable(Base):
    __tablename__ = 'joinquant_stock'  # 设置数据库表名称

    pk = Column(String(36), primary_key=True)  # 唯一识别码，可以理解为订单号，区分不同的订单
    code = Column(String(20))  # 证券代码
    tradetime = Column(DateTime)  # 交易时间
    order_values = Column(Integer) # 下单数量，可以改名为amount
    price = Column(Integer)  # 下单价格
    ordertype = Column(String(10)) # 下单方向，买 或 卖
    if_deal = Column(Boolean) # 是否已经成交
    insertdate = Column(DateTime) # 订单信息插入数据库的时间

def initialize(context):
    set_benchmark('000001.XSHG')
    set_option('use_real_price', True)
    set_slippage(FixedSlippage(0))
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, 
                   open_commission=0.0003, close_commission=0.0003, 
                   close_today_commission=0, min_commission=5), type='fund')
    log.set_level('order', 'error')
    
    g.stock_num = 12
    g.limit_days = 20
    g.limit_up_list = []
    g.hold_list = []
    g.history_hold_list = []
    g.not_buy_again_list = []
    g.high_limit_list = []

    run_daily(prepare_stock_list, time='9:05')
    run_daily(push_orders_to_database, time='9:15')  # 在9:15推送订单到数据库
    run_daily(weekly_adjustment,time='13:55')
    run_daily(check_limit_up, time='13:56')

def get_dividend_ratio_filter_list(context, stock_list, sort, p1, p2):
    time1 = context.previous_date
    time0 = time1 - datetime.timedelta(days=365)
    interval = 1000
    list_len = len(stock_list)
    
    q = query(finance.STK_XR_XD.code, 
             finance.STK_XR_XD.a_registration_date, 
             finance.STK_XR_XD.bonus_amount_rmb
            ).filter(
                finance.STK_XR_XD.a_registration_date >= time0,
                finance.STK_XR_XD.a_registration_date <= time1,
                finance.STK_XR_XD.code.in_(stock_list[:min(list_len, interval)]))
    df = finance.run_query(q)
    
    if list_len > interval:
        df_num = list_len // interval
        for i in range(df_num):
            q = query(finance.STK_XR_XD.code, 
                     finance.STK_XR_XD.a_registration_date, 
                     finance.STK_XR_XD.bonus_amount_rmb
                    ).filter(
                        finance.STK_XR_XD.a_registration_date >= time0,
                        finance.STK_XR_XD.a_registration_date <= time1,
                        finance.STK_XR_XD.code.in_(stock_list[interval*(i+1):min(list_len,interval*(i+2))]))
            temp_df = finance.run_query(q)
            df = pd.concat([df, temp_df], sort=False)
    
    dividend = df.fillna(0).groupby('code').sum()
    temp_list = list(dividend.index)
    
    q = query(valuation.code, valuation.market_cap).filter(valuation.code.in_(temp_list))
    cap = get_fundamentals(q, date=time1).set_index('code')
    
    DR = pd.concat([dividend, cap], axis=1, sort=False)
    DR['dividend_ratio'] = (DR['bonus_amount_rmb']/10000) / DR['market_cap']
    DR = DR.sort_values(by=['dividend_ratio'], ascending=sort)
    return list(DR.index)[int(p1*len(DR)):int(p2*len(DR))]

def get_stock_list(context):
    yesterday = context.previous_date
    initial_list = get_all_securities().index.tolist()
    initial_list = filter_kcbj_stock(initial_list)
    initial_list = filter_new_stock(context, initial_list, 375)
    initial_list = filter_st_stock(initial_list)
    
    dr_list = get_dividend_ratio_filter_list(context, initial_list, False, 0, 0.5)
    
    q = query(valuation.code, balance.total_non_current_liability,valuation.market_cap
            ).filter(valuation.code.in_(dr_list)
            ).order_by((balance.total_non_current_liability/(valuation.market_cap+balance.total_non_current_liability)).asc())
    fun = get_fundamentals(q, date=yesterday)
    lev_list = list(fun['code'])[0:int(0.5*len(fun))]
    
    HSL1,MAHSL1 = HSL(lev_list, check_date=yesterday, N=5)
    factor_list = []
    factor_count = int(0.5*len(lev_list))
    for k in sorted(MAHSL1, key=MAHSL1.get, reverse=True):
        if factor_count > 0:
            factor_list.append(k)
            factor_count -= 1
    
    q1 = query(valuation.code, valuation.circulating_market_cap
             ).filter(valuation.code.in_(factor_list)
             ).order_by(valuation.circulating_market_cap.asc())
    df = get_fundamentals(q1, date=yesterday)
    return list(df.code)[:15]

def prepare_stock_list(context):
    g.hold_list = [position.security for position in context.portfolio.positions.values()]
    g.history_hold_list.append(g.hold_list.copy())
    
    if len(g.history_hold_list) >= g.limit_days:
        g.history_hold_list = g.history_hold_list[-g.limit_days:]
    
    temp_set = set()
    for hold_list in g.history_hold_list:
        temp_set.update(hold_list)
    g.not_buy_again_list = list(temp_set)
    
    if g.hold_list:
        panel = get_price(g.hold_list, end_date=context.previous_date, 
                         frequency='daily', fields=['close','high_limit'], 
                         count=1, skip_paused=False)
        df_close = panel['close']
        df_high_limit = panel['high_limit']
        selected_stocks = df_close[df_close == df_high_limit].dropna(axis=1)
        g.high_limit_list = selected_stocks.columns.tolist()
    else:
        g.high_limit_list = []

def weekly_adjustment(context):
    target_list = get_stock_list(context)
    target_list = filter_paused_stock(target_list)
    target_list = filter_limitup_stock(context, target_list)
    target_list = filter_limitdown_stock(context, target_list)
    target_list = target_list[:min(g.stock_num, len(target_list))]
    
    order_list = []  # 用于存储交易记录
    current_time = context.current_dt
    
    for stock in g.hold_list:
        if (stock not in target_list) and (stock not in g.high_limit_list):
            # 检查股票是否在持仓中
            if stock in context.portfolio.positions:
                # 先获取持仓数量
                position_amount = context.portfolio.positions[stock].total_amount
                if close_position(context.portfolio.positions[stock]):
                    # 记录卖出订单
                    order_dict = {
                        'pk': str(uuid.uuid1()),
                        'code': stock,
                        'tradetime': current_time,
                        'order_values': position_amount,
                        'price': 0,
                        'ordertype': '卖',
                        'if_deal': False,  # 还未被iQuant执行
                        'insertdate': current_time
                    }
                    order_list.append(order_dict)
    
    target_value = context.portfolio.cash 
    position_count = len(context.portfolio.positions)
    target_num = len(target_list)
    
    if target_num > position_count:
        value = target_value / (target_num - position_count)
        # 获取当前行情数据
        current_data = get_current_data()
        for stock in target_list:
            if context.portfolio.positions[stock].total_amount == 0:
                if open_position(stock, value):
                    # 记录买入订单
                    order_dict = {
                        'pk': str(uuid.uuid1()),
                        'code': stock,
                        'tradetime': current_time,
                        'order_values': int(value / current_data[stock].last_price),
                        'price': current_data[stock].last_price,
                        'ordertype': '买',
                        'if_deal': False,  # 还未被iQuant执行
                        'insertdate': current_time
                    }
                    order_list.append(order_dict)
                    if len(context.portfolio.positions) == target_num:
                        break
    
    # 推送交易记录到数据库
    if order_list:
        push_order_command(order_list)

def check_limit_up(context):
    if g.high_limit_list:
        now_time = context.current_dt
        order_list = []
        
        for stock in g.high_limit_list:
            current_data = get_price(stock, end_date=now_time, frequency='1m', 
                                   fields=['close','high_limit'], skip_paused=False, 
                                   fq='pre', count=1)
            if current_data.iloc[0,0] < current_data.iloc[0,1]:
                # 检查股票是否在持仓中
                if stock in context.portfolio.positions:
                    # 先获取持仓数量
                    position_amount = context.portfolio.positions[stock].total_amount
                    if close_position(context.portfolio.positions[stock]):
                        # 记录涨停板打开后的卖出订单
                        order_dict = {
                            'pk': str(uuid.uuid1()),
                            'code': stock,
                            'tradetime': now_time,
                            'order_values': position_amount,
                            'price': current_data.iloc[0,0],
                            'ordertype': '卖',
                            'if_deal': False,  # 还未被iQuant执行
                            'insertdate': now_time
                        }
                        order_list.append(order_dict)
        
        # 推送交易记录到数据库
        if order_list:
            push_order_command(order_list)

def filter_paused_stock(stock_list):
    current_data = get_current_data()
    return [s for s in stock_list if not current_data[s].paused]

def filter_st_stock(stock_list):
    current_data = get_current_data()
    return [s for s in stock_list 
           if not current_data[s].is_st 
           and 'ST' not in current_data[s].name 
           and '*' not in current_data[s].name 
           and '退' not in current_data[s].name]

def filter_kcbj_stock(stock_list):
    return [s for s in stock_list 
           if not (s.startswith('68') or s[0] in ['4', '8'])]

def filter_new_stock(context, stock_list, d):
    yesterday = context.previous_date
    return [s for s in stock_list 
           if (yesterday - get_security_info(s).start_date) >= datetime.timedelta(days=d)]

# 格式化股票代码，将聚宽格式转换为QMT格式
def format_code(code):
    code = code.replace('.XSHE','.SZ')
    code = code.replace('.XSHG','.SH')
    return code

# 清理历史数据
def clean_old_data(session, current_date):
    """清理一个月前的数据和当天的重复数据"""
    try:
        # 计算一个月前的日期
        one_month_ago = current_date - datetime.timedelta(days=30)
        
        # 删除一个月前的数据
        old_data = session.query(JoinQuantTable).filter(
            JoinQuantTable.tradetime < one_month_ago
        ).delete()
        
        # 删除当天的数据（避免重复）
        today_start = datetime.datetime.combine(current_date.date(), datetime.time.min)
        today_end = datetime.datetime.combine(current_date.date(), datetime.time.max)
        
        today_data = session.query(JoinQuantTable).filter(
            JoinQuantTable.tradetime >= today_start,
            JoinQuantTable.tradetime <= today_end
        ).delete()
        
        session.commit()
        
        if old_data > 0:
            log.info("删除了%d条一个月前的历史数据" % old_data)
        if today_data > 0:
            log.info("删除了%d条今天的重复数据" % today_data)
            
    except Exception as e:
        session.rollback()
        log.error("清理数据时出错: %s" % str(e))

# 推送订单指令到数据库
def push_order_command(order_dict_list):
    try:    
        # 数据库连接参数
        db_user = 'root'
        db_password = 'Hello2025'
        db_host = 'sh-cdb-kgv8etuq.sql.tencentcdb.com'
        db_port = 23333
        db_name = 'order'
        
        # 创建SQLAlchemy引擎 - 使用聚宽支持的mysql连接方式，指定UTF-8MB4编码
        engine = create_engine('mysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?charset=utf8mb4'.format(
            db_user=db_user,
            db_password=db_password,
            db_host=db_host,
            db_port=db_port,
            db_name=db_name
        ))
        
        # 创建表（如果不存在）
        Base.metadata.create_all(engine)
        
        # 创建会话
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # 清理历史数据和当天重复数据
        if order_dict_list:
            current_date = order_dict_list[0]['tradetime']
            clean_old_data(session, current_date)

        for order_dict in order_dict_list:
            pk = order_dict['pk']
            code = format_code(order_dict['code'])
            tradetime = order_dict['tradetime']
            order_values = order_dict['order_values']
            price = order_dict['price']
            ordertype = order_dict['ordertype']
            if_deal = order_dict['if_deal']
            insertdate = order_dict['insertdate']

            # 创建新记录
            new_record = JoinQuantTable(
                pk=pk,
                code=code,
                tradetime=tradetime,
                order_values=order_values,
                price=price,
                ordertype=ordertype,
                if_deal=if_deal,
                insertdate=insertdate
            )

            # 添加记录到会话
            session.add(new_record)

        # 提交更改到数据库
        session.commit()
        log.info("成功推送%d条订单到数据库" % len(order_dict_list))

        # 关闭会话
        session.close()
    except Exception as e:
        log.error('数据库出错: %s' % str(e))

# 推送订单到数据库的主函数
def push_orders_to_database(context):
    """在每天9:15推送当天的持仓信息到数据库"""
    order_list = []
    current_time = context.current_dt
    
    # 先清理一次数据库（即使没有新订单也要清理历史数据）
    try:
        # 创建临时连接清理数据
        db_user = 'root'
        db_password = 'Hello2025'
        db_host = 'sh-cdb-kgv8etuq.sql.tencentcdb.com'
        db_port = 23333
        db_name = 'order'
        
        engine = create_engine('mysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?charset=utf8mb4'.format(
            db_user=db_user,
            db_password=db_password,
            db_host=db_host,
            db_port=db_port,
            db_name=db_name
        ))
        
        Session = sessionmaker(bind=engine)
        session = Session()
        clean_old_data(session, current_time)
        session.close()
    except Exception as e:
        log.error("清理数据库时出错: %s" % str(e))
    
    # 获取目标持仓列表
    target_list = get_stock_list(context)
    target_list = filter_paused_stock(target_list)
    target_list = filter_limitup_stock(context, target_list)
    target_list = filter_limitdown_stock(context, target_list)
    target_list = target_list[:min(g.stock_num, len(target_list))]
    
    # 获取当前持仓
    current_positions = [position.security for position in context.portfolio.positions.values()]
    
    # 需要卖出的股票
    for stock in current_positions:
        if stock not in target_list and stock not in g.high_limit_list:
            order_dict = {
                'pk': str(uuid.uuid1()),
                'code': stock,
                'tradetime': current_time,
                'order_values': context.portfolio.positions[stock].total_amount,
                'price': 0,  # 市价单
                'ordertype': '卖',
                'if_deal': False,
                'insertdate': current_time
            }
            order_list.append(order_dict)
    
    # 需要买入的股票
    for stock in target_list:
        if stock not in current_positions:
            order_dict = {
                'pk': str(uuid.uuid1()),
                'code': stock,
                'tradetime': current_time,
                'order_values': 0,  # 由QMT根据资金计算
                'price': 0,  # 市价单
                'ordertype': '买',
                'if_deal': False,
                'insertdate': current_time
            }
            order_list.append(order_dict)
    
    # 推送到数据库
    if order_list:
        push_order_command(order_list)
        log.info("推送了%d条订单到数据库" % len(order_list))

def order_target_value_(security, value):
    log.debug("Selling out %s" % security if value == 0 else "Order %s to value %f" % (security, value))
    return order_target_value(security, value)

def open_position(security, value):
    order = order_target_value_(security, value)
    return order and order.filled > 0

def close_position(position):
    order = order_target_value_(position.security, 0)
    return order and order.status == OrderStatus.held and order.filled == order.amount

def adjust_position(context, buy_stocks, stock_num):
    for stock in list(context.portfolio.positions.keys()):
        if stock not in buy_stocks:
            close_position(context.portfolio.positions[stock])
    
    position_count = len(context.portfolio.positions)
    if stock_num > position_count:
        value = context.portfolio.cash / (stock_num - position_count)
        for stock in buy_stocks:
            if context.portfolio.positions[stock].total_amount == 0:
                if open_position(stock, value):
                    if len(context.portfolio.positions) == stock_num:
                        break

def filter_paused_stock(stock_list):
    current_data = get_current_data()
    return [s for s in stock_list if not current_data[s].paused]


def filter_st_stock(stock_list):
    current_data = get_current_data()
    return [s for s in stock_list 
           if not current_data[s].is_st 
           and 'ST' not in current_data[s].name 
           and '*' not in current_data[s].name 
           and '退' not in current_data[s].name]

def filter_limitup_stock(context, stock_list):
    last_prices = history(1, unit='1m', field='close', security_list=stock_list)
    current_data = get_current_data()
    return [stock for stock in stock_list 
            if stock in context.portfolio.positions.keys()
            or last_prices[stock][-1] < current_data[stock].high_limit]

def filter_limitdown_stock(context, stock_list):
    last_prices = history(1, unit='1m', field='close', security_list=stock_list)
    current_data = get_current_data()
    return [stock for stock in stock_list 
            if stock in context.portfolio.positions.keys()
            or last_prices[stock][-1] > current_data[stock].low_limit]

def filter_kcbj_stock(stock_list):
    return [s for s in stock_list 
           if not (s.startswith('68') or s[0] in ['4', '8'])]

def filter_new_stock(context, stock_list, d):
    yesterday = context.previous_date
    return [s for s in stock_list 
           if (yesterday - get_security_info(s).start_date) >= datetime.timedelta(days=d)]