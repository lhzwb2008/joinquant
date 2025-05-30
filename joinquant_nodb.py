# -*- coding: utf-8 -*-
from jqdata import *
from jqlib.technical_analysis import *
import numpy as np
import pandas as pd
import datetime

def initialize(context):
    set_benchmark('000001.XSHG')
    set_option('use_real_price', True)
    set_slippage(PriceRelatedSlippage(0.001))
    set_order_cost(OrderCost(open_tax=0, close_tax=0.0005, 
                   open_commission=0.0003, close_commission=0.0003, 
                   close_today_commission=0, min_commission=0), type='stock')
    log.set_level('order', 'error')
    
    g.stock_num = 12
    g.limit_days = 20
    g.limit_up_list = []
    g.hold_list = []
    g.history_hold_list = []
    g.not_buy_again_list = []
    g.high_limit_list = []

    run_daily(prepare_stock_list, time='9:05')
    run_weekly(weekly_adjustment, weekday=1, time='13:55')
    run_daily(check_limit_up, time='13:55')

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
            df = pd.concat([df, temp_df])
    
    dividend = df.fillna(0).groupby('code').sum()
    temp_list = list(dividend.index)
    
    q = query(valuation.code, valuation.market_cap).filter(valuation.code.in_(temp_list))
    cap = get_fundamentals(q, date=time1).set_index('code')
    
    DR = pd.concat([dividend, cap], axis=1)
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
    
    for stock in g.hold_list:
        if (stock not in target_list) and (stock not in g.high_limit_list):
            close_position(context.portfolio.positions[stock])
    
    target_value = context.portfolio.cash 
    position_count = len(context.portfolio.positions)
    target_num = len(target_list)
    
    if target_num > position_count:
        value = target_value / (target_num - position_count)
        for stock in target_list:
            if context.portfolio.positions[stock].total_amount == 0:
                if open_position(stock, value):
                    if len(context.portfolio.positions) == target_num:
                        break

def check_limit_up(context):
    if g.high_limit_list:
        now_time = context.current_dt
        for stock in g.high_limit_list:
            current_data = get_price(stock, end_date=now_time, frequency='1m', 
                                   fields=['close','high_limit'], skip_paused=False, 
                                   fq='pre', count=1)
            if current_data.iloc[0,0] < current_data.iloc[0,1]:
                close_position(context.portfolio.positions[stock])

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