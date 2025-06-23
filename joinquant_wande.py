# -*- coding: utf-8 -*-
from jqdata import *
from jqlib.technical_analysis import *
import numpy as np
import pandas as pd
import datetime

def initialize(context):
    """初始化函数"""
    # 设置基准
    set_benchmark('000001.XSHG')
    # 设置真实价格
    set_option('use_real_price', True)
    # 设置滑点
    set_slippage(PriceRelatedSlippage(0.001))
    # 设置手续费
    set_order_cost(OrderCost(open_tax=0, close_tax=0.0005, 
                   open_commission=0.0003, close_commission=0.0003, 
                   close_today_commission=0, min_commission=0), type='stock')
    # 设置日志级别
    log.set_level('order', 'error')
    
    # 策略参数 - 完全按照万得微盘股指数
    g.micro_cap_num = 100  # 持仓股票数量：市值最小的400只
    g.hold_list = []  # 当前持仓列表
    
    # 每日调仓 - 万得微盘股指数是每日更新成分股
    run_daily(daily_adjustment, time='14:00')

def get_micro_cap_stocks(context):
    """
    获取万得微盘股成分股：市值最小的400只股票
    剔除ST、*ST、退市整理股、首发连板未打开的标的
    """
    # 获取所有股票
    all_stocks = get_all_securities(['stock']).index.tolist()
    
    # 获取当前数据
    current_data = get_current_data()
    yesterday = context.previous_date
    
    # 过滤ST、*ST、退市股票
    filtered_stocks = []
    for stock in all_stocks:
        if (not current_data[stock].is_st and 
            'ST' not in current_data[stock].name and 
            '*' not in current_data[stock].name and 
            '退' not in current_data[stock].name):
            filtered_stocks.append(stock)
    
    # 过滤停牌股票
    filtered_stocks = [s for s in filtered_stocks if not current_data[s].paused]
    
    # 过滤科创板和北交所股票
    filtered_stocks = [s for s in filtered_stocks 
                      if not (s.startswith('68') or s.startswith('8') or s.startswith('4'))]
    
    # 过滤新股（首发连板未打开）- 简化处理：过滤上市不足20天的股票
    final_stocks = []
    for stock in filtered_stocks:
        start_date = get_security_info(stock).start_date
        if (yesterday - start_date) >= datetime.timedelta(days=20):
            final_stocks.append(stock)
    
    # 获取市值数据并排序
    if len(final_stocks) > 0:
        q = query(valuation.code, valuation.market_cap
                ).filter(valuation.code.in_(final_stocks)
                ).order_by(valuation.market_cap.asc())
        
        df = get_fundamentals(q, date=yesterday)
        
        # 返回市值最小的400只股票
        return list(df['code'])[:min(g.micro_cap_num, len(df))]
    else:
        return []

def daily_adjustment(context):
    """每日调仓函数 - 完全按照万得微盘股指数逻辑"""
    log.info('=== 开始每日调仓 ===')
    
    # 获取最新的微盘股400只
    target_list = get_micro_cap_stocks(context)
    log.info(f'微盘股数量: {len(target_list)}')
    
    # 获取当前持仓
    g.hold_list = [position.security for position in context.portfolio.positions.values()]
    
    # 找出需要卖出的股票（不在新的400只名单中）
    sell_list = [stock for stock in g.hold_list if stock not in target_list]
    
    # 找出需要买入的股票（在新的400只名单中但未持有）
    buy_list = [stock for stock in target_list if stock not in g.hold_list]
    
    log.info(f'需要卖出: {len(sell_list)}只, 需要买入: {len(buy_list)}只')
    
    # 先卖出
    for stock in sell_list:
        order_target_value(stock, 0)
        log.info(f'卖出: {stock}')
    
    # 计算每只股票的目标价值（等权重）
    if len(target_list) > 0:
        total_value = context.portfolio.total_value
        target_value = total_value / len(target_list)
        
        # 买入新股票
        for stock in buy_list:
            # 检查是否涨停（避免买不进）
            current_data = get_current_data()
            last_price = current_data[stock].last_price
            high_limit = current_data[stock].high_limit
            
            if last_price < high_limit * 0.995:  # 非涨停
                order_target_value(stock, target_value)
                log.info(f'买入: {stock}, 目标价值: {target_value:.2f}')
        
        # 调整已持有股票的仓位（保持等权重）
        for stock in target_list:
            if stock in g.hold_list and stock not in sell_list:
                current_value = context.portfolio.positions[stock].total_amount * \
                              context.portfolio.positions[stock].price
                # 如果偏离目标价值超过20%，则调整
                if abs(current_value - target_value) / target_value > 0.2:
                    order_target_value(stock, target_value)
                    log.info(f'调整仓位: {stock}, 目标价值: {target_value:.2f}')

def handle_data(context, data):
    """主函数（每分钟调用）"""
    pass

# 辅助函数
def filter_paused_stock(stock_list):
    """过滤停牌股票"""
    current_data = get_current_data()
    return [s for s in stock_list if not current_data[s].paused]

def filter_st_stock(stock_list):
    """过滤ST股票"""
    current_data = get_current_data()
    return [s for s in stock_list 
           if not current_data[s].is_st 
           and 'ST' not in current_data[s].name 
           and '*' not in current_data[s].name 
           and '退' not in current_data[s].name]

def filter_kcbj_stock(stock_list):
    """过滤科创板、北交所股票"""
    return [s for s in stock_list 
           if not (s.startswith('68') or s.startswith('8') or s.startswith('4'))]

def filter_new_stock(context, stock_list, days=60):
    """过滤新股"""
    yesterday = context.previous_date
    return [s for s in stock_list 
           if (yesterday - get_security_info(s).start_date) >= datetime.timedelta(days=days)] 