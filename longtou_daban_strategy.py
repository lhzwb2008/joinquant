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
    g.max_position_count = 5  # 最大持仓数量
    g.single_position_ratio = 0.2  # 单股最大仓位比例（20%）
    g.candidate_pool_size = 200  # 候选股票池大小
    g.max_daily_buy_count = 5  # 每日最大买入股票数
    g.lookback_days = 3  # 回看天数
    g.min_pullback_ratio = 0.95  # 最小回调比例（当前价格/爆发前价格）
    
    # 全局变量
    g.candidate_stocks = []  # 候选股票池
    g.historical_thresholds = {}  # 存储历史阈值数据
    g.buy_records = {}  # 买入记录
    g.burst_records = {}  # 记录三日内的爆发情况
    
    # 状态标记
    g.stocks_prepared = False
    g.thresholds_calculated = False
    g.bursts_scanned = False
    g.morning_sell_done = False
    
    # 定时任务 - 只保留必要的每日运行一次的任务
    run_daily(prepare_stocks, time='09:25')     # 开盘前准备
    run_daily(reset_daily_flags, time='09:20')  # 重置每日标记
    
    log.info("=== 爆发回调买入策略启动 ===")
    log.info(f"最大持仓: {g.max_position_count}只")
    log.info(f"单股仓位: {g.single_position_ratio*100}%")
    log.info(f"回看天数: {g.lookback_days}天")
    log.info("策略逻辑: 买入三日内曾爆发但已回调的股票")

def reset_daily_flags(context):
    """重置每日标记"""
    g.morning_sell_done = False
    g.thresholds_calculated = False
    g.bursts_scanned = False
    log.info("=== 重置每日标记 ===")

def handle_data(context, data):
    """分钟级运行函数"""
    current_time = context.current_dt
    hour = current_time.hour
    minute = current_time.minute
    
    # 09:35 执行早盘卖出
    if hour == 9 and minute == 35 and not g.morning_sell_done:
        morning_sell(context)
        g.morning_sell_done = True
    
    # 09:40 计算阈值
    elif hour == 9 and minute == 40 and not g.thresholds_calculated:
        calculate_thresholds(context)
        g.thresholds_calculated = True
    
    # 09:45 扫描历史爆发（可选）
    elif hour == 9 and minute == 45 and not g.bursts_scanned:
        # 暂时跳过历史扫描，直接使用简单策略
        # scan_historical_bursts(context)
        g.bursts_scanned = True
        log.info("跳过历史爆发扫描，使用简单动量策略")
    
    # 10:00 进行一次详细的调试检查
    elif hour == 10 and minute == 0:
        debug_stock_data(context)
        simple_momentum_buy(context)
    
    # 10:05-14:45 每5分钟检查一次
    elif hour >= 10 and minute % 5 == 0:
        if hour < 14 or (hour == 14 and minute <= 45):
            # 优先使用简单动量策略
            simple_momentum_buy(context)

def prepare_stocks(context):
    """准备候选股票池"""
    try:
        # 获取所有A股
        all_stocks = list(get_all_securities(types=['stock']).index)
        
        # 基础过滤
        stocks = []
        for stock in all_stocks:
            # 过滤ST股票
            if is_st_stock(stock):
                continue
            # 过滤科创板和北交所
            if stock.startswith('688') or stock.startswith('8') or stock.startswith('4'):
                continue
            stocks.append(stock)
        
        # 随机选择候选股票
        if len(stocks) > g.candidate_pool_size:
            stocks = np.random.choice(stocks, g.candidate_pool_size, replace=False).tolist()
        
        g.candidate_stocks = stocks
        log.info(f"准备候选股票池: {len(g.candidate_stocks)}只")
        
    except Exception as e:
        log.error(f"准备股票池出错: {e}")
        g.candidate_stocks = []

def morning_sell(context):
    """早盘卖出（T+1，卖出昨日买入的股票）"""
    try:
        current_data = get_current_data()
        positions = context.portfolio.positions
        
        log.info("=== 09:35 早盘卖出检查 ===")
        
        sell_count = 0
        hold_count = 0
        
        for stock in positions:
            position = positions[stock]
            if position.total_amount <= 0:
                continue
            
            # 检查是否有可卖数量（T+1限制）
            if position.closeable_amount <= 0:
                log.info(f"T+1限制: {stock} 今日无法卖出，持有数量: {position.total_amount}")
                continue
            
            try:
                current_price = current_data[stock].last_price
                if current_price <= 0:
                    continue
                
                # 检查是否涨停
                high_limit = current_data[stock].high_limit
                if abs(current_price - high_limit) < 0.01:
                    log.info(f"涨停持有: {stock}")
                    hold_count += 1
                    continue
                
                # 计算收益率
                cost_price = position.avg_cost
                return_rate = (current_price - cost_price) / cost_price * 100
                
                # 卖出条件：
                # 1. 亏损超过5%止损
                # 2. 盈利超过10%止盈
                # 3. 其他情况也卖出（简化策略）
                sell_reason = ""
                if return_rate < -5:
                    sell_reason = "止损"
                elif return_rate > 10:
                    sell_reason = "止盈"
                else:
                    sell_reason = "策略卖出"
                
                # 执行卖出
                order_result = order_target_percent(stock, 0)
                if order_result:
                    log.info(f"{sell_reason}: {stock}, 成本: {cost_price:.2f}, 现价: {current_price:.2f}, 收益率: {return_rate:.2f}%")
                    sell_count += 1
                    
                    # 清除买入记录
                    if stock in g.buy_records:
                        del g.buy_records[stock]
                else:
                    log.error(f"早盘卖出失败: {stock}")
                        
            except Exception as e:
                log.error(f"处理股票 {stock} 早盘卖出时出错: {e}")
                continue
        
        if sell_count > 0:
            log.info(f"早盘卖出 {sell_count} 只股票")
        if hold_count > 0:
            log.info(f"继续持有 {hold_count} 只涨停股票")
        if sell_count == 0 and hold_count == 0:
            log.info("早盘无股票需要处理")
        
    except Exception as e:
        log.error(f"早盘卖出检查时出错: {e}")

def calculate_thresholds(context):
    """计算当日各时段的动态阈值"""
    try:
        log.info("=== 设置交易时段阈值 ===")
        
        # 直接设置各时段的阈值，不再计算历史数据
        thresholds = {}
        
        # 生成所有交易时段
        for hour in [10, 11, 13, 14]:
            for minute in [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]:
                if hour == 14 and minute > 45:
                    continue
                    
                time_str = f"{hour:02d}:{minute:02d}"
                
                # 根据时段设置不同的阈值
                if hour < 11:
                    thresholds[time_str] = 0.5  # 早盘0.5%
                elif hour < 14:
                    thresholds[time_str] = 0.3  # 午盘0.3%
                else:
                    thresholds[time_str] = 0.2  # 尾盘0.2%
        
        g.historical_thresholds = thresholds
        
        # 输出阈值统计信息
        log.info(f"阈值设置完成 - 早盘(10-11点): 0.5%, 午盘(11-14点): 0.3%, 尾盘(14-15点): 0.2%")
        
    except Exception as e:
        log.error(f"设置阈值时出错: {e}")
        # 设置默认阈值
        g.historical_thresholds = {f"{h:02d}:{m:02d}": 0.3 
                                  for h in [10,11,13,14] 
                                  for m in [0,5,10,15,20,25,30,35,40,45,50,55] 
                                  if not (h == 14 and m > 45)}

def calculate_historical_max_speed(stocks, date, time_str):
    """计算指定日期和时间的最高上涨速度"""
    try:
        # 简化逻辑，直接返回固定的较低阈值
        # 根据时段设置不同的阈值
        hour = int(time_str.split(':')[0])
        
        # 早盘阈值较高，午后阈值较低
        if hour < 11:
            return 0.5  # 早盘0.5%
        elif hour < 14:
            return 0.3  # 午盘0.3%
        else:
            return 0.2  # 尾盘0.2%
        
    except Exception as e:
        log.error(f"计算阈值出错: {e}")
        return 0.3

def scan_historical_bursts(context):
    """扫描过去三天内的爆发记录"""
    try:
        log.info("=== 扫描历史爆发记录 ===")
        
        # 清空之前的记录
        g.burst_records = {}
        
        # 获取过去三个交易日
        end_date = context.previous_date
        trading_days = get_trade_days(end_date=end_date, count=g.lookback_days)
        
        if len(trading_days) < g.lookback_days:
            log.warning(f"交易日不足{g.lookback_days}天")
            return
        
        log.info(f"扫描日期: {[d.strftime('%Y-%m-%d') for d in trading_days]}")
        
        # 对每只候选股票扫描历史爆发
        burst_count = 0
        total_bursts = 0
        scan_errors = 0
        
        for i, stock in enumerate(g.candidate_stocks[:50]):  # 减少扫描数量到50只
            try:
                bursts = scan_stock_bursts(stock, trading_days)
                if bursts:
                    g.burst_records[stock] = bursts
                    burst_count += 1
                    total_bursts += len(bursts)
                    
                    # 记录前几个爆发的详细信息
                    if burst_count <= 3:
                        log.info(f"股票 {stock} 发现 {len(bursts)} 次爆发")
                        for j, burst in enumerate(bursts[:2]):
                            log.info(f"  爆发{j+1}: {burst['date'].strftime('%m-%d')} {burst['time']} "
                                   f"涨幅{burst['instant_speed']:.2f}%")
                    
                # 每10只股票输出一次进度
                if (i + 1) % 10 == 0:
                    log.info(f"已扫描 {i + 1} 只股票，发现 {burst_count} 只有爆发记录，错误 {scan_errors} 次")
                    
            except Exception as e:
                scan_errors += 1
                if scan_errors <= 5:  # 只记录前5个错误
                    log.error(f"扫描股票 {stock} 时出错: {e}")
                continue
        
        log.info(f"扫描完成: 共扫描 {len(g.candidate_stocks[:50])} 只股票")
        log.info(f"发现 {burst_count} 只股票有爆发记录，总计 {total_bursts} 次爆发")
        log.info(f"扫描错误 {scan_errors} 次")
        
    except Exception as e:
        log.error(f"扫描历史爆发时出错: {e}")

def scan_stock_bursts(stock, trading_days):
    """扫描单只股票的历史爆发记录"""
    bursts = []
    
    for date in trading_days:
        try:
            # 构造当天的开始和结束时间字符串
            start_date_str = date.strftime('%Y-%m-%d') + ' 09:30:00'
            end_date_str = date.strftime('%Y-%m-%d') + ' 15:00:00'
            
            # 获取当天的分钟数据
            price_data = get_price(stock,
                                 start_date=start_date_str,
                                 end_date=end_date_str,
                                 frequency='1m',
                                 fields=['close'],
                                 skip_paused=True)
            
            if len(price_data) < 10:  # 数据太少，跳过
                continue
            
            # 使用更简单的方法：计算5分钟涨幅
            for i in range(5, len(price_data), 5):  # 每5分钟检查一次
                if i >= len(price_data):
                    break
                    
                start_price = price_data['close'].iloc[i-5]
                end_price = price_data['close'].iloc[i]
                
                if start_price <= 0:
                    continue
                
                # 计算5分钟涨幅
                five_min_change = (end_price - start_price) / start_price * 100
                
                # 获取对应时段的阈值
                time_str = price_data.index[i].strftime('%H:%M')
                threshold = g.historical_thresholds.get(time_str, 0.3)
                
                # 如果5分钟涨幅超过阈值，记录爆发
                if five_min_change >= threshold:
                    bursts.append({
                        'date': date,
                        'time': time_str,
                        'pre_burst_price': start_price,
                        'burst_price': end_price,
                        'instant_speed': five_min_change,
                        'threshold': threshold,
                        'type': '5min_burst'
                    })
                    
                    # 每只股票每天最多记录3次爆发
                    if len([b for b in bursts if b['date'] == date]) >= 3:
                        break
                    
        except Exception as e:
            # 记录具体错误
            if 'get_price' in str(e):
                pass  # 数据获取失败，忽略
            else:
                log.error(f"扫描股票 {stock} 在 {date} 的数据时出错: {e}")
            continue
    
    return bursts

def check_pullback_and_buy(context):
    """检查回调并买入符合条件的股票"""
    if not g.burst_records or not g.historical_thresholds:
        return
    
    try:
        current_time = context.current_dt
        time_str = f"{current_time.hour:02d}:{current_time.minute:02d}"
        
        # 计算当前持仓数量
        current_positions = len([pos for pos in context.portfolio.positions.values() 
                               if pos.total_amount > 0])
        
        if current_positions >= g.max_position_count:
            return  # 持仓已满
        
        current_data = get_current_data()
        buy_candidates = []
        
        # 只在整5分钟时输出日志
        if current_time.minute % 5 == 0:
            log.info(f"=== {time_str} 回调检查 ===")
            log.info(f"爆发记录股票数: {len(g.burst_records)}")
        
        # 检查有爆发记录的股票是否已回调
        checked_count = 0
        for stock, bursts in g.burst_records.items():
            try:
                # 跳过已持仓股票
                if context.portfolio.positions[stock].total_amount > 0:
                    continue
                
                # 跳过停牌股票
                if current_data[stock].paused:
                    continue
                
                current_price = current_data[stock].last_price
                if current_price <= 0:
                    continue
                
                checked_count += 1
                
                # 检查是否有符合条件的回调
                for burst in bursts:
                    pre_burst_price = burst['pre_burst_price']
                    
                    # 判断是否回调到爆发前价格附近
                    pullback_ratio = current_price / pre_burst_price
                    
                    if pullback_ratio <= 1.02:  # 放宽到爆发前价格的102%以下
                        days_ago = (context.current_dt.date() - burst['date']).days
                        buy_candidates.append({
                            'stock': stock,
                            'current_price': current_price,
                            'pre_burst_price': pre_burst_price,
                            'burst_info': burst,
                            'pullback_ratio': pullback_ratio,
                            'days_ago': days_ago,
                            'score': burst['instant_speed'] / (days_ago + 1)  # 爆发强度/天数作为评分
                        })
                        break  # 一只股票只需要一个符合条件的爆发记录
                        
            except Exception as e:
                continue
        
        if current_time.minute % 5 == 0 and checked_count > 0:
            log.info(f"检查了 {checked_count} 只有爆发记录的股票")
        
        if not buy_candidates:
            return
        
        # 按评分排序，优先买入近期爆发强度大的
        buy_candidates.sort(key=lambda x: x['score'], reverse=True)
        
        log.info(f"发现 {len(buy_candidates)} 只回调股票")
        for i, candidate in enumerate(buy_candidates[:3]):
            burst = candidate['burst_info']
            log.info(f"  {i+1}. {candidate['stock']}: "
                    f"{candidate['days_ago']}天前爆发{burst['instant_speed']:.2f}%, "
                    f"现已回调{(1-candidate['pullback_ratio'])*100:.2f}%")
        
        # 买入股票
        buy_count = 0
        max_buy = min(g.max_daily_buy_count, g.max_position_count - current_positions)
        
        for candidate in buy_candidates:
            if buy_count >= max_buy:
                break
            
            stock = candidate['stock']
            current_price = candidate['current_price']
            
            # 避免追涨停
            high_limit = current_data[stock].high_limit
            if abs(current_price - high_limit) < 0.01:
                continue
            
            # 计算买入金额
            total_value = context.portfolio.total_value
            target_value = total_value * g.single_position_ratio
            
            # 检查可用资金
            if context.portfolio.available_cash < target_value:
                target_value = context.portfolio.available_cash * 0.8
            
            if target_value < 1000:
                log.info(f"资金不足，无法买入 {stock}")
                continue
            
            # 下单买入
            order_result = order_target_value(stock, target_value)
            if order_result:
                burst = candidate['burst_info']
                log.info(f"回调买入: {stock}, "
                        f"{candidate['days_ago']}天前爆发{burst['instant_speed']:.2f}%, "
                        f"回调比例{(1-candidate['pullback_ratio'])*100:.2f}%")
                buy_count += 1
                
                # 记录买入信息
                g.buy_records[stock] = {
                    'buy_time': current_time,
                    'buy_price': current_price,
                    'burst_info': burst,
                    'pullback_ratio': candidate['pullback_ratio']
                }
            else:
                log.error(f"买入失败: {stock}")
        
        if buy_count > 0:
            log.info(f"成功买入 {buy_count} 只回调股票")
            
    except Exception as e:
        log.error(f"检查回调买入时出错: {e}")

def is_st_stock(stock):
    """判断是否为ST股票"""
    try:
        current_data = get_current_data()
        return current_data[stock].is_st
    except:
        return False

def before_trading_start(context):
    """开盘前运行"""
    pass

def after_trading_end(context):
    """收盘后运行"""
    # 统计持仓
    positions = [stock for stock, pos in context.portfolio.positions.items() 
                if pos.total_amount > 0]
    if positions:
        log.info(f"收盘持仓: {len(positions)}只股票")
        for stock in positions:
            buy_info = g.buy_records.get(stock, {})
            if buy_info:
                burst = buy_info.get('burst_info', {})
                pullback = buy_info.get('pullback_ratio', 0)
                log.info(f"  {stock}: 爆发速度={burst.get('instant_speed', 0):.2f}%, "
                        f"回调比例={(1-pullback)*100:.2f}%")
    else:
        log.info("收盘无持仓")

def simple_momentum_buy(context):
    """简单动量买入策略 - 买入当日涨幅较大的股票"""
    try:
        current_time = context.current_dt
        time_str = f"{current_time.hour:02d}:{current_time.minute:02d}"
        
        # 计算当前持仓数量 - 修复持仓检查逻辑
        current_positions = len([stock for stock in context.portfolio.positions 
                               if context.portfolio.positions[stock].total_amount > 0])
        
        if current_positions >= g.max_position_count:
            if current_time.minute % 15 == 0:
                log.info(f"持仓已满: {current_positions}/{g.max_position_count}")
            return  # 持仓已满
        
        # 每15分钟输出一次日志
        if current_time.minute % 15 == 0:
            log.info(f"=== {time_str} 简单动量策略检查 ===")
            log.info(f"当前持仓: {current_positions}/{g.max_position_count}")
            log.info(f"可用资金: {context.portfolio.available_cash:.0f}")
        
        current_data = get_current_data()
        buy_candidates = []
        checked_count = 0
        valid_count = 0
        
        # 获取已持仓的股票列表，避免重复检查
        held_stocks = set(context.portfolio.positions.keys())
        
        # 检查候选股票的当日涨幅
        for stock in g.candidate_stocks[:30]:  # 减少到30只，提高成功率
            try:
                # 跳过已持仓股票 - 使用更高效的方法
                if stock in held_stocks and context.portfolio.positions[stock].total_amount > 0:
                    continue
                
                checked_count += 1
                
                # 跳过停牌股票
                if current_data[stock].paused:
                    continue
                
                current_price = current_data[stock].last_price
                if current_price <= 0:
                    continue
                
                # 简化：获取昨日收盘价
                try:
                    hist_data = get_price(stock, count=2, end_date=current_time, fields=['close'])
                    if len(hist_data) < 2:
                        continue
                    pre_close = hist_data['close'].iloc[-2]
                except:
                    continue
                
                if pre_close <= 0:
                    continue
                
                valid_count += 1
                
                # 计算当日涨幅
                day_change = (current_price - pre_close) / pre_close * 100
                
                # 进一步放宽条件：涨幅0.1%-15%
                if 0.1 <= day_change <= 15.0:
                    # 避免涨停跌停
                    high_limit = current_data[stock].high_limit
                    low_limit = current_data[stock].low_limit
                    
                    if abs(current_price - high_limit) < 0.01 or abs(current_price - low_limit) < 0.01:
                        continue
                        
                    buy_candidates.append({
                        'stock': stock,
                        'current_price': current_price,
                        'day_change': day_change,
                        'score': day_change  # 简单使用涨幅作为评分
                    })
                    
            except Exception as e:
                # 忽略单个股票的错误，继续处理下一个
                continue
        
        if current_time.minute % 15 == 0:
            log.info(f"检查了 {checked_count} 只股票，有效数据 {valid_count} 只")
        
        if not buy_candidates:
            if current_time.minute % 15 == 0:
                log.info("未发现符合条件的股票")
            return
        
        # 按涨幅排序
        buy_candidates.sort(key=lambda x: x['score'], reverse=True)
        
        log.info(f"发现 {len(buy_candidates)} 只候选股票")
        for i, candidate in enumerate(buy_candidates[:5]):
            log.info(f"  {i+1}. {candidate['stock']}: 涨幅 {candidate['day_change']:.2f}%")
        
        # 买入股票
        buy_count = 0
        max_buy = min(3, g.max_position_count - current_positions)  # 每次最多买3只
        
        for candidate in buy_candidates[:10]:  # 考虑前10个候选
            if buy_count >= max_buy:
                break
            
            stock = candidate['stock']
            current_price = candidate['current_price']
            
            # 计算买入金额
            total_value = context.portfolio.total_value
            target_value = total_value * g.single_position_ratio
            
            # 检查可用资金
            if context.portfolio.available_cash < target_value:
                target_value = context.portfolio.available_cash * 0.95
            
            if target_value < 500:  # 降低最小买入金额
                log.info(f"资金不足，无法买入，可用资金: {context.portfolio.available_cash:.0f}")
                break
            
            # 下单买入
            order_result = order_target_value(stock, target_value)
            if order_result:
                log.info(f"动量买入成功: {stock}, 当日涨幅: {candidate['day_change']:.2f}%, 金额: {target_value:.0f}")
                buy_count += 1
                
                # 记录买入信息
                g.buy_records[stock] = {
                    'buy_time': current_time,
                    'buy_price': current_price,
                    'day_change': candidate['day_change']
                }
            else:
                log.error(f"买入失败: {stock}")
        
        if buy_count == 0:
            log.info("本次检查未成功买入任何股票")
                
    except Exception as e:
        log.error(f"简单动量买入时出错: {e}")

def debug_stock_data(context):
    """调试股票数据获取"""
    try:
        log.info("=== 调试股票数据 ===")
        current_data = get_current_data()
        
        # 检查前10只候选股票的数据
        valid_count = 0
        rising_count = 0
        
        for i, stock in enumerate(g.candidate_stocks[:10]):
            try:
                if current_data[stock].paused:
                    log.info(f"股票 {stock} 停牌")
                    continue
                
                current_price = current_data[stock].last_price
                
                # 使用get_price获取昨日收盘价
                hist_data = get_price(stock, count=2, end_date=context.current_dt, fields=['close'])
                if len(hist_data) < 2:
                    log.info(f"股票 {stock} 历史数据不足")
                    continue
                
                pre_close = hist_data['close'].iloc[-2]  # 昨日收盘价
                
                if current_price <= 0 or pre_close <= 0:
                    log.info(f"股票 {stock} 价格数据异常: 现价={current_price}, 昨收={pre_close}")
                    continue
                
                valid_count += 1
                day_change = (current_price - pre_close) / pre_close * 100
                
                if day_change > 0:
                    rising_count += 1
                
                log.info(f"股票 {stock}: 现价={current_price:.2f}, 昨收={pre_close:.2f}, 涨幅={day_change:.2f}%")
                
            except Exception as e:
                log.error(f"检查股票 {stock} 数据时出错: {e}")
        
        log.info(f"调试结果: 有效股票 {valid_count}/10, 上涨股票 {rising_count}/10")
        
    except Exception as e:
        log.error(f"调试股票数据时出错: {e}") 