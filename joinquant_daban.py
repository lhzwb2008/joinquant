# -*- coding: utf-8 -*-
from jqdata import *
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def initialize(context):
    """初始化函数"""
    # 设置基准
    set_benchmark('000001.XSHG')
    
    # 设置股票池
    set_option('use_real_price', True)
    
    # 设置手续费
    set_slippage(PriceRelatedSlippage(0.001))
    set_order_cost(OrderCost(open_tax=0, close_tax=0.0005, 
                   open_commission=0.0003, close_commission=0.0003, 
                   close_today_commission=0, min_commission=0), type='stock')
    
    # 策略参数
    g.max_position_count = 5  # 最大持仓数量（分散风险）
    g.single_position_ratio = 0.2  # 单股仓位比例（20%）
    g.lookback_days = 30  # 历史低位回看天数（缩短周期，更敏感）
    g.low_open_min = -0.06  # 低开最小幅度 -6%
    g.low_open_max = -0.01   # 低开最大幅度 -1%（严格低开）
    g.stop_loss_ratio = -0.06  # 止损比例 -6%（放宽止损）
    g.take_profit_ratio = 0.08  # 止盈比例 +8%
    g.holding_days = 3  # 最大持有天数（缩短持有期）
    g.min_profit_hold_days = 1  # 盈利时最少持有天数
    
    # 全局变量
    g.candidate_stocks = []  # 候选股票池
    g.first_board_stocks = []  # 昨日首板股票
    g.buy_records = {}  # 买入记录
    g.daily_buy_count = 0  # 当日已买入数量
    
    # 状态标记
    g.morning_scan_done = False
    g.morning_buy_done = False
    
    # 定时任务
    run_daily(prepare_trading_day, time='09:00')  # 开盘前准备
    run_daily(scan_first_board_stocks, time='09:25')  # 扫描昨日首板股票
    run_daily(morning_buy_check, time='09:31')  # 早盘买入检查
    run_daily(stop_loss_check, time='10:30')  # 止损检查1
    run_daily(stop_loss_check, time='11:00')  # 止损检查2
    run_daily(stop_loss_check, time='13:30')  # 止损检查3
    run_daily(stop_loss_check, time='14:00')  # 止损检查4
    run_daily(end_day_sell_check, time='14:50')  # 尾盘卖出检查
    
    log.info("=== 优化版严格首板低开策略启动 ===")
    log.info(f"最大持仓: {g.max_position_count}只")
    log.info(f"单股仓位: {g.single_position_ratio*100}%")
    log.info(f"历史低位回看: {g.lookback_days}天")
    log.info(f"严格低开区间: {g.low_open_min*100}% ~ {g.low_open_max*100}%")
    log.info(f"止损线: {g.stop_loss_ratio*100}% | 止盈线: {g.take_profit_ratio*100}%")
    log.info(f"最大持有天数: {g.holding_days}天 | 盈利最少持有: {g.min_profit_hold_days}天")
    log.info("策略特点: 首板低开+动态止盈止损+灵活持仓管理")

def prepare_trading_day(context):
    """准备交易日"""
    g.morning_scan_done = False
    g.morning_buy_done = False
    g.daily_buy_count = 0
    
    # 先执行早盘卖出
    morning_sell(context)
    
    log.info("=== 新交易日准备完成 ===")

def scan_first_board_stocks(context):
    """扫描昨日首板股票"""
    try:
        log.info("=== 扫描昨日首板股票 ===")
        
        # 获取昨日交易日
        yesterday = context.previous_date
        
        # 获取所有A股
        all_stocks = list(get_all_securities(types=['stock']).index)
        
        # 过滤条件
        first_board_list = []
        scan_count = 0
        low_position_count = 0
        
        # 限制扫描数量，提高效率
        sample_size = min(1000, len(all_stocks))
        sampled_stocks = np.random.choice(all_stocks, sample_size, replace=False)
        
        for stock in sampled_stocks:
            try:
                # 过滤ST股票
                if is_st_stock(stock):
                    continue
                # 过滤科创板和北交所
                if stock.startswith('688') or stock.startswith('8') or stock.startswith('4'):
                    continue
                
                scan_count += 1
                
                # 获取昨日数据
                yesterday_data = get_price(stock, 
                                         count=2, 
                                         end_date=yesterday,
                                         fields=['close', 'high_limit'],
                                         skip_paused=True)
                
                if len(yesterday_data) < 2:
                    continue
                
                # 检查是否涨停
                yesterday_close = yesterday_data['close'].iloc[-1]
                yesterday_limit = yesterday_data['high_limit'].iloc[-1]
                
                if abs(yesterday_close - yesterday_limit) < 0.01:
                    # 检查是否为首板（前一天没有涨停）
                    pre_day_close = yesterday_data['close'].iloc[-2]
                    pre_day_limit = yesterday_data['high_limit'].iloc[-2]
                    
                    if abs(pre_day_close - pre_day_limit) >= 0.01:
                        # 检查是否处于历史低位
                        if is_at_historical_low(stock, yesterday, g.lookback_days):
                            first_board_list.append(stock)
                            low_position_count += 1
                            
            except Exception as e:
                continue
        
        g.first_board_stocks = first_board_list
        log.info(f"扫描了 {scan_count} 只股票")
        log.info(f"发现 {len(g.first_board_stocks)} 只昨日首板且处于历史低位的股票")
        
        if len(g.first_board_stocks) > 0 and len(g.first_board_stocks) <= 10:
            log.info(f"首板股票列表: {g.first_board_stocks}")
        
        g.morning_scan_done = True
        
    except Exception as e:
        log.error(f"扫描首板股票时出错: {e}")
        g.first_board_stocks = []

def is_at_historical_low(stock, date, lookback_days):
    """判断股票是否处于历史低位（中位数以下）"""
    try:
        # 获取历史数据
        hist_data = get_price(stock, 
                            count=lookback_days,
                            end_date=date,
                            fields=['close'],
                            skip_paused=True)
        
        if len(hist_data) < lookback_days * 0.8:  # 数据不足
            return False
        
        # 计算中位数
        median_price = hist_data['close'].median()
        current_price = hist_data['close'].iloc[-1]
        
        # 判断是否在中位数以下
        return current_price < median_price
        
    except Exception as e:
        return False

def morning_buy_check(context):
    """早盘买入检查"""
    try:
        if not g.morning_scan_done or not g.first_board_stocks:
            log.info("无首板股票或扫描未完成，跳过买入")
            return
        
        log.info("=== 09:31 早盘买入检查 ===")
        
        current_data = get_current_data()
        buy_candidates = []
        debug_info = []  # 用于收集调试信息
        
        # 检查首板股票的开盘情况
        for stock in g.first_board_stocks:
            try:
                # 跳过停牌
                if current_data[stock].paused:
                    continue
                
                # 跳过已持仓
                if context.portfolio.positions[stock].total_amount > 0:
                    continue
                
                # 获取今日开盘价和昨日收盘价
                today_open = current_data[stock].day_open
                
                # 获取昨日收盘价
                yesterday_data = get_price(stock,
                                         count=1,
                                         end_date=context.previous_date,
                                         fields=['close'])
                
                if len(yesterday_data) == 0:
                    continue
                    
                yesterday_close = yesterday_data['close'].iloc[-1]
                
                if today_open <= 0 or yesterday_close <= 0:
                    continue
                
                # 计算低开幅度
                open_change = (today_open - yesterday_close) / yesterday_close
                
                # 收集调试信息
                debug_info.append(f"{stock}: 开盘涨跌幅 {open_change*100:.2f}%")
                
                # 严格检查：只要首板低开条件
                if g.low_open_min <= open_change <= g.low_open_max:
                    current_price = current_data[stock].last_price
                    buy_candidates.append({
                        'stock': stock,
                        'open_change': open_change,
                        'current_price': current_price,
                        'yesterday_close': yesterday_close,
                        'type': '首板低开'
                    })
                    
            except Exception as e:
                continue
        
        # 输出所有首板股票的开盘情况
        if debug_info:
            log.info("首板股票开盘情况:")
            for info in debug_info[:10]:  # 最多显示10条
                log.info(f"  {info}")
        
        if not buy_candidates:
            log.info(f"未发现符合严格首板低开条件的股票（开盘涨跌幅需在 {g.low_open_min*100:.1f}% ~ {g.low_open_max*100:.1f}% 之间）")
            g.morning_buy_done = True
            return
        
        # 按低开幅度排序（绝对值从大到小）
        buy_candidates.sort(key=lambda x: abs(x['open_change']), reverse=True)
        
        log.info(f"发现 {len(buy_candidates)} 只符合严格首板低开条件的股票:")
        for i, candidate in enumerate(buy_candidates[:5]):
            log.info(f"  {i+1}. {candidate['stock']}: 低开 {candidate['open_change']*100:.2f}% ({candidate.get('type', '未知')})")
        
        # 计算当前持仓数量
        current_positions = len([pos for pos in context.portfolio.positions.values() 
                               if pos.total_amount > 0])
        
        # 买入股票
        buy_count = 0
        max_buy = min(len(buy_candidates), g.max_position_count - current_positions)
        
        for candidate in buy_candidates[:max_buy]:
            stock = candidate['stock']
            
            # 计算买入金额（均仓）
            total_value = context.portfolio.total_value
            target_value = total_value * g.single_position_ratio
            
            # 检查可用资金
            if context.portfolio.available_cash < target_value:
                target_value = context.portfolio.available_cash * 0.95
            
            if target_value < 1000:
                log.info(f"资金不足，停止买入")
                break
            
            # 下单买入
            order_result = order_target_value(stock, target_value)
            if order_result:
                log.info(f"买入成功: {stock} ({candidate.get('type', '未知')}), 低开 {candidate['open_change']*100:.2f}%, 金额: {target_value:.0f}")
                log.info(f"订单ID: {order_result}")
                buy_count += 1
                
                # 记录买入信息
                g.buy_records[stock] = {
                    'buy_date': context.current_dt.date(),
                    'buy_price': candidate['current_price'],
                    'open_change': candidate['open_change'],
                    'type': candidate.get('type', '未知')
                }
            else:
                log.error(f"买入失败: {stock}, 目标金额: {target_value:.0f}")
                log.error(f"可用资金: {context.portfolio.available_cash:.0f}, 总资产: {context.portfolio.total_value:.0f}")
        
        g.daily_buy_count = buy_count
        log.info(f"早盘共买入 {buy_count} 只严格首板低开股票")
        g.morning_buy_done = True
        
    except Exception as e:
        log.error(f"早盘买入检查时出错: {e}")
        g.morning_buy_done = True

def stop_loss_check(context):
    """止损止盈检查"""
    try:
        current_time = context.current_dt
        log.info(f"=== {current_time.strftime('%H:%M')} 止损止盈检查 ===")
        
        current_data = get_current_data()
        sell_count = 0
        
        for stock in list(context.portfolio.positions.keys()):
            position = context.portfolio.positions[stock]
            if position.total_amount <= 0:
                continue
            
            # 检查是否有可卖数量
            if position.closeable_amount <= 0:
                continue
            
            try:
                current_price = current_data[stock].last_price
                if current_price <= 0:
                    continue
                
                # 计算收益率
                cost_price = position.avg_cost
                return_rate = (current_price - cost_price) / cost_price
                
                # 获取持有天数
                buy_info = g.buy_records.get(stock, {})
                buy_date = buy_info.get('buy_date')
                holding_days = (context.current_dt.date() - buy_date).days if buy_date else 0
                
                sell_reason = None
                
                # 止损检查
                if return_rate <= g.stop_loss_ratio:
                    sell_reason = f"止损"
                # 止盈检查
                elif return_rate >= g.take_profit_ratio:
                    sell_reason = f"止盈"
                # 盈利但未达到止盈线，检查是否满足最少持有天数
                elif return_rate > 0 and holding_days >= g.min_profit_hold_days:
                    # 如果盈利超过3%且持有超过1天，也可以考虑卖出
                    if return_rate >= 0.03:
                        sell_reason = f"盈利锁定"
                
                if sell_reason:
                    order_result = order_target(stock, 0)
                    if order_result:
                        log.info(f"{sell_reason}卖出: {stock}, 成本: {cost_price:.2f}, "
                               f"现价: {current_price:.2f}, 收益率: {return_rate*100:.2f}%, 持有{holding_days}天")
                        sell_count += 1
                        
                        # 清除买入记录
                        if stock in g.buy_records:
                            del g.buy_records[stock]
                    else:
                        log.error(f"{sell_reason}卖出失败: {stock}")
                        
            except Exception as e:
                log.error(f"处理股票 {stock} 止损止盈时出错: {e}")
                continue
        
        if sell_count > 0:
            log.info(f"止损止盈卖出 {sell_count} 只股票")
        else:
            log.info("无需止损止盈")
            
    except Exception as e:
        log.error(f"止损止盈检查时出错: {e}")

def end_day_sell_check(context):
    """尾盘卖出检查"""
    try:
        log.info("=== 14:50 尾盘卖出检查 ===")
        
        current_data = get_current_data()
        sell_count = 0
        
        for stock in list(context.portfolio.positions.keys()):
            position = context.portfolio.positions[stock]
            if position.total_amount <= 0:
                continue
            
            # 检查是否有可卖数量
            if position.closeable_amount <= 0:
                continue
            
            try:
                # 获取买入记录
                buy_info = g.buy_records.get(stock, {})
                if not buy_info:
                    continue
                
                buy_date = buy_info.get('buy_date')
                if not buy_date:
                    continue
                
                # 计算持有天数
                holding_days = (context.current_dt.date() - buy_date).days
                
                current_price = current_data[stock].last_price
                if current_price <= 0:
                    continue
                
                # 计算收益率
                cost_price = position.avg_cost
                return_rate = (current_price - cost_price) / cost_price
                
                sell_reason = None
                
                # 卖出条件判断（更加灵活）
                if holding_days >= g.holding_days:
                    sell_reason = f"持有{holding_days}天到期"
                elif return_rate < -0.02 and holding_days >= 2:
                    # 亏损超过2%且持有超过2天
                    sell_reason = f"持有期亏损{return_rate*100:.2f}%"
                elif return_rate > 0.05:
                    # 盈利超过5%，尾盘获利了结
                    sell_reason = f"尾盘获利了结{return_rate*100:.2f}%"
                
                if sell_reason:
                    order_result = order_target(stock, 0)
                    if order_result:
                        log.info(f"尾盘卖出: {stock}, {sell_reason}, "
                               f"成本: {cost_price:.2f}, 现价: {current_price:.2f}")
                        sell_count += 1
                        
                        # 清除买入记录
                        if stock in g.buy_records:
                            del g.buy_records[stock]
                    else:
                        log.error(f"尾盘卖出失败: {stock}")
                        
            except Exception as e:
                log.error(f"处理股票 {stock} 尾盘卖出时出错: {e}")
                continue
        
        if sell_count > 0:
            log.info(f"尾盘卖出 {sell_count} 只股票")
        else:
            log.info("尾盘无需卖出")
            
    except Exception as e:
        log.error(f"尾盘卖出检查时出错: {e}")

def is_st_stock(stock):
    """判断是否为ST股票"""
    try:
        current_data = get_current_data()
        return current_data[stock].is_st
    except:
        return False

def handle_data(context, data):
    """分钟级运行函数 - 本策略主要使用定时任务，此函数留空"""
    pass

def before_trading_start(context):
    """开盘前运行"""
    pass

def after_trading_end(context):
    """收盘后运行"""
    # 统计当日交易
    positions = [stock for stock, pos in context.portfolio.positions.items() 
                if pos.total_amount > 0]
    
    log.info("=== 收盘统计 ===")
    log.info(f"当日买入: {g.daily_buy_count} 只")
    log.info(f"当前持仓: {len(positions)} 只")
    
    if positions:
        total_value = 0
        for stock in positions:
            position = context.portfolio.positions[stock]
            value = position.total_amount * position.price
            total_value += value
            
            buy_info = g.buy_records.get(stock, {})
            if buy_info:
                buy_date = buy_info.get('buy_date')
                holding_days = (context.current_dt.date() - buy_date).days if buy_date else 0
                return_rate = (position.price - position.avg_cost) / position.avg_cost * 100
                
                log.info(f"  {stock}: 持有{holding_days}天, 收益率: {return_rate:.2f}%")
        
        log.info(f"持仓总市值: {total_value:.0f}")
    else:
        log.info("当前空仓")

def morning_sell(context):
    """早盘卖出（T+1）"""
    try:
        log.info("=== 09:30 早盘卖出检查 ===")
        
        current_data = get_current_data()
        sell_count = 0
        
        for stock in list(context.portfolio.positions.keys()):
            position = context.portfolio.positions[stock]
            if position.total_amount <= 0:
                continue
            
            # 检查是否有可卖数量（T+1限制）
            if position.closeable_amount <= 0:
                log.info(f"T+1限制: {stock} 今日无法卖出")
                continue
            
            try:
                current_price = current_data[stock].last_price
                if current_price <= 0:
                    continue
                
                # 检查是否涨停，涨停不卖
                high_limit = current_data[stock].high_limit
                if abs(current_price - high_limit) < 0.01:
                    log.info(f"涨停持有: {stock}")
                    continue
                
                # 获取买入记录
                buy_info = g.buy_records.get(stock, {})
                buy_date = buy_info.get('buy_date')
                
                # 如果是昨天买入的，今天可以卖出
                if buy_date and (context.current_dt.date() - buy_date).days >= 1:
                    order_result = order_target(stock, 0)
                    if order_result:
                        cost_price = position.avg_cost
                        return_rate = (current_price - cost_price) / cost_price * 100
                        log.info(f"T+1卖出: {stock}, 成本: {cost_price:.2f}, 现价: {current_price:.2f}, 收益率: {return_rate:.2f}%")
                        sell_count += 1
                        
                        # 清除买入记录
                        if stock in g.buy_records:
                            del g.buy_records[stock]
                    else:
                        log.error(f"卖出失败: {stock}")
                        
            except Exception as e:
                log.error(f"处理股票 {stock} 时出错: {e}")
                continue
        
        if sell_count > 0:
            log.info(f"早盘卖出 {sell_count} 只股票")
        else:
            log.info("早盘无股票需要卖出")
            
    except Exception as e:
        log.error(f"早盘卖出检查时出错: {e}") 