# 导入函数库
import jqdata
import pandas as pd
from pandas import Series, DataFrame
import numpy as np
import statsmodels.api as sm
import scipy.stats as scs
import matplotlib.pyplot as plt

factors = ['PE', 'PB', 'PS', 'EPS', 'BM','ROE', 'ROA', 'gross_profit_margin', 'inc_net_profit_year_on_year', 'inc_net_profit_annual', 
'inc_operation_profit_year_on_year', 'inc_operation_profit_annual', 'GPR', 'PR',
'net_profit', 'operating_revenue', 'capitalization', 'circulating_cap', 'market_cap', 'circulating_market_cap','LA', 'FAP','turnover_ratio']

# 初始化函数，设定基准等等
def initialize(context)
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    # 输出内容到日志 log.info()
    log.info('初始函数开始运行且全局只运行一次')
    # 过滤掉order系列API产生的比error级别低的log
    # log.set_level('order', 'error')
    
    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')
    
    ## 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）
      # 开盘前运行
    run_daily(before_market_open, time='before_open', reference_security='000300.XSHG') 
 
# 月初取出因子值
def get_factors(fdate, factors)
    stock_set = get_index_stocks('000001.XSHG', fdate)
    q = query(
        valuation.code,
        balance.total_owner_equitiesvaluation.market_cap100000000,
        valuation.pe_ratio,
        valuation.pb_ratio,
        valuation.ps_ratio,
        income.basic_eps,
        indicator.roe,
        indicator.roa,
        indicator.gross_profit_margin,
        indicator.inc_net_profit_year_on_year,
        indicator.inc_net_profit_annual,
        indicator.inc_operation_profit_year_on_year,
        indicator.inc_operation_profit_annual,
        income.total_profitincome.operating_revenue,
        income.net_profitincome.operating_revenue,
        income.net_profit,
        income.operating_revenue,
        valuation.capitalization,
        valuation.circulating_cap,
        valuation.market_cap,
        valuation.circulating_market_cap,
        balance.total_liabilitybalance.total_assets,
        balance.fixed_assetsbalance.total_assets,
        valuation.turnover_ratio
        ).filter(
        valuation.code.in_(stock_set),
        valuation.circulating_market_cap
    )
    fdf = get_fundamentals(q, date=fdate)
    fdf.index = fdf['code']
    fdf.columns = ['code'] + factors
    return fdf.iloc[,-23]

def calculate_port_monthly_return(port, item_date_list, circulating_market_cap)
    start_year = item_date_list[0]
    start_month = item_date_list[1]
    one_startdate = str(start_year) + '-' + str(start_month) + '-01'
    one_enddate = str(start_year) + '-' + str(start_month) + '-28'
    if 12 == start_month
        next_start = str(start_year + 1) + '-' + str(01) + '-01'
        next_end = str(start_year + 1) + '-' + str(01) + '-28'
    else
        next_start = str(start_year) + '-' + str(start_month + 1) + '-01'
        next_end = str(start_year) + '-' + str(start_month + 1) + '-28'
    close1 = get_price(port, one_startdate, one_enddate, 'daily', ['close'])
    close2 = get_price(port, next_start, next_end, 'daily', ['close'])
    weighted_m_return = ((close2['close'].ix[0,]close1['close'].ix[0,]-1)circulating_market_cap).sum()(circulating_market_cap.ix[port].sum())
    return weighted_m_return

def calculate_benchmark_monthly_return(item_date_list)
    start_year = item_date_list[0]
    start_month = item_date_list[1]
    one_startdate = str(start_year) + '-' + str(start_month) + '-01'
    one_enddate = str(start_year) + '-' + str(start_month) + '-28'
    if 12 == start_month
        next_start = str(start_year + 1) + '-' + str(01) + '-01'
        next_end = str(start_year + 1) + '-' + str(01) + '-28'
    else
        next_start = str(start_year) + '-' + str(start_month + 1) + '-01'
        next_end = str(start_year) + '-' + str(start_month + 1) + '-28'
    close1 = get_price(['000001.XSHG'], one_startdate,one_enddate,'daily',['close'])['close']
    close2 = get_price(['000001.XSHG'],next_start, next_end, 'daily',['close'])['close']
    benchmark_return = (close2.ix[0,]close1.ix[0,]-1).sum()
    return benchmark_return
    
## 开盘前运行函数     
def before_market_open(context)
    # 输出运行时间
    #one_return_test()
    #get_panel_data()
    get_stock_factors('2018-08-01')
    
    
def one_return_test()
    fdf = get_factors('2018-01-01', factors)
    score = fdf['circulating_market_cap'].order()
    df = {}
    circulating_market_cap = fdf['circulating_market_cap']
    port1 = list(score.index)[ len(score)5]
    port2 = list(score.index)[ len(score)5 2len(score)5]
    port3 = list(score.index)[ 2len(score)5 -2len(score)5]
    port4 = list(score.index)[ -2len(score)5 -len(score)5]
    port5 = list(score.index)[ -len(score)5 ]
    item_date_list = [2017,01]
    calculate_benchmark_monthly_return(item_date_list)
    df['port1'] = calculate_port_monthly_return(port1,item_date_list, fdf['circulating_market_cap'])
    df['port2'] = calculate_port_monthly_return(port2,item_date_list, fdf['circulating_market_cap'])
    df['port3'] = calculate_port_monthly_return(port3,item_date_list, fdf['circulating_market_cap'])
    df['port4'] = calculate_port_monthly_return(port4,item_date_list, fdf['circulating_market_cap'])
    df['port5'] = calculate_port_monthly_return(port5,item_date_list, fdf['circulating_market_cap'])
    ret_se = Series(df)
    log.info(ret_se)

def get_date_calc_list(last_date_list, month_num)
    ret_list = []
    last_year = last_date_list[0]
    last_month = last_date_list[1]
    for i in range(month_num)
        item_list = []
        if i  last_month
            item_list = [last_year, last_month - i]
        else
            calc_year = last_year - ((i - last_month)12 + 1)
            calc_month = 12 - (i - last_month)%12
            item_list = [calc_year, calc_month]
        ret_list.append(item_list)
    ret_list.reverse()
    return ret_list

def get_panel_data()
    factors = ['PE', 'PB', 'PS', 'EPS', 'BM',
           'ROE', 'ROA', 'gross_profit_margin', 'inc_net_profit_year_on_year', 'inc_net_profit_annual', 
                     'inc_operation_profit_year_on_year', 'inc_operation_profit_annual', 'GPR', 'PR',
           'net_profit', 'operating_revenue', 'capitalization', 'circulating_cap', 'market_cap', 'circulating_market_cap',
                     'LA', 'FAP',
           'turnover_ratio']
    #因为研究模块取fundamental数据默认date为研究日期的前一天。所以要自备时间序列。按月取
    result = {}
    last_date_list = [2018,5]
    month_num = 12
    ret_date_list = get_date_calc_list(last_date_list, month_num)
    for i,item_date_list in enumerate(ret_date_list)
        # print 'time %s'%startdate
        startdate = str(item_date_list[0]) + '-' + str(item_date_list[1]) + '-01'
        fdf = get_factors(startdate,factors)
        circulating_market_cap = fdf['circulating_market_cap']
        #5个组合，23个因子
        df = DataFrame(np.zeros(623).reshape(6,23),index = ['port1','port2','port3','port4','port5','benchmark'],columns = factors)
        for fac in factors
            score = fdf[fac].order()
            port1 = list(score.index)[ len(score)5]
            port2 = list(score.index)[ len(score)5+1 2len(score)5]
            port3 = list(score.index)[ 2len(score)5+1 -2len(score)5]
            port4 = list(score.index)[ -2len(score)5+1 -len(score)5]
            port5 = list(score.index)[ -len(score)5+1 ]
            df.ix['port1',fac] = calculate_port_monthly_return(port1,item_date_list,circulating_market_cap)
            df.ix['port2',fac] = calculate_port_monthly_return(port2,item_date_list,circulating_market_cap)
            df.ix['port3',fac] = calculate_port_monthly_return(port3,item_date_list,circulating_market_cap)
            df.ix['port4',fac] = calculate_port_monthly_return(port4,item_date_list,circulating_market_cap)
            df.ix['port5',fac] = calculate_port_monthly_return(port5,item_date_list,circulating_market_cap)
            df.ix['benchmark',fac] = calculate_benchmark_monthly_return(item_date_list)
        # print 'factor %s'%fac
        result[i+1]=df
        log.info(i)
    monthly_return = pd.Panel(result)
    get_effect_result(factors, monthly_return)
    
def get_effect_result(factors, monthly_return)
    total_return = {}
    annual_return = {}
    excess_return = {}
    win_prob = {}
    loss_prob = {}
    effect_test = {}
    MinCorr = 0.3
    Minbottom = -0.05
    Mintop = 0.05
    for fac in factors
        effect_test[fac] = {}
        monthly = monthly_return[,,fac]
        total_return[fac] = (monthly+1).T.cumprod().iloc[-1,]-1
        annual_return[fac] = (total_return[fac]+1)(1.6)-1
        excess_return[fac] = annual_return[fac] - annual_return[fac][-1]
        #判断因子有效性
        #1.年化收益与组合序列的相关性 大于 阈值
        effect_test[fac][1] = annual_return[fac][05].corr(Series([1,2,3,4,5],index = annual_return[fac][05].index))
        #2.高收益组合跑赢概率
        #因子小，收益小，port1是输家组合，port5是赢家组合
        if total_return[fac][0]  total_return[fac][-2]
            loss_excess = monthly.iloc[0,]-monthly.iloc[-1,]
            loss_prob[fac] = loss_excess[loss_excess0].count()float(len(loss_excess))
            win_excess = monthly.iloc[-2,]-monthly.iloc[-1,]
            win_prob[fac] = win_excess[win_excess0].count()float(len(win_excess))
            
            effect_test[fac][3] = [win_prob[fac],loss_prob[fac]]
            
            #超额收益
            effect_test[fac][2] = [excess_return[fac][-2]100,excess_return[fac][0]100]
                        
        #因子小，收益大，port1是赢家组合，port5是输家组合
        else
            loss_excess = monthly.iloc[-2,]-monthly.iloc[-1,]
            loss_prob[fac] = loss_excess[loss_excess0].count()float(len(loss_excess))
            win_excess = monthly.iloc[0,]-monthly.iloc[-1,]
            win_prob[fac] = win_excess[win_excess0].count()float(len(win_excess))
            
            effect_test[fac][3] = [win_prob[fac],loss_prob[fac]]
            
            #超额收益
            effect_test[fac][2] = [excess_return[fac][0]100,excess_return[fac][-2]100]

    #由于选择的因子较多，test标准选取适当严格一些
    #effect_test[1]记录因子相关性，0.7或-0.7合格
    #effect_test[2]记录【赢家组合超额收益，输家组合超额收益】
    #effect_test[3]记录赢家组合跑赢概率和输家组合跑输概率。【0.6,0.4】合格 (因实际情况，跑输概率暂时不考虑)
    effect_pd = DataFrame(effect_test).T
    ret_pd1 = effect_pd[abs(effect_pd[1])  0.6]
    log.info(ret_pd1)
    
def get_stock_factors(fdate)
    #财务指标
    test_factors = ['PE', 'PB', 'PS', 'ROE', 'ROA', 'gross_profit_margin','ocf']
    #stock_set = {'000999.XSHE', '600085.XSHG', '002424.XSHE', '300026.XSHE', '000623.XSHE'}    # 医药
    #stock_set = {'300236.XSHE', '002049.XSHE', '603986.XSHG', '300613.XSHE'}    #芯片
    stock_set = {'601800.XSHG','601186.XSHG','601669.XSHG','601668.XSHG', '601766.XSHG'}     #基建
    q = query(
        valuation.code,
        valuation.pe_ratio,
        valuation.pb_ratio,
        valuation.ps_ratio,
        indicator.roe,
        indicator.roa,
        indicator.gross_profit_margin,
        indicator.ocf_to_revenue,
        ).filter(
        valuation.code.in_(stock_set),
        valuation.circulating_market_cap
    )
    fdf = get_fundamentals(q, date=fdate)
    fdf.index = fdf['code']
    fdf.columns = ['code'] + test_factors
    log.info(fdf)
    write_file('fact.csv', fdf.to_csv(), append=False)
        

