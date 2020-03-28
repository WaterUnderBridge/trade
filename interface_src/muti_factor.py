# 导入函数库
from jqdata import *
from pandas import DataFrame,Series
import pandas as pd
# 初始化函数，设定基准等等
def initialize(context):
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
      # 开盘时运行
    run_daily(market_open, time='open', reference_security='000300.XSHG')
      # 收盘后运行
    run_daily(after_market_close, time='after_close', reference_security='000300.XSHG')

## 开盘前运行函数
def before_market_open(context):
    pass

## 开盘时运行函数
def market_open(context):
    pass

group_num = 10
def _gen_stocks_groups(in_se):
    len_se = len(in_se)
    one_group_len = int(len_se/group_num)
    ret_list = []
    for i in range(group_num):
        if i < group_num - 1:
            ret_list.append(in_se[i*one_group_len:(i+1)*one_group_len])
        else:
            ret_list.append(in_se[i*one_group_len:])
    return ret_list

def trans_one_df(in_df):
    new_df = DataFrame({'close':in_df.iloc[0,:]},index = in_df.columns)
    return new_df

g_start_time = '2018-07-28'
def _get_on_group_profit(in_se, m_end_date, f_info_df):
    end_price_df = get_price(list(in_se), count = 1, end_date=m_end_date, frequency='daily', fields=['close'])['close']
    start_price_df = get_price(list(in_se), count = 1, end_date=g_start_time, frequency='daily', fields=['close'])['close']
    trans_end = trans_one_df(end_price_df)
    trans_start = trans_one_df(start_price_df)
    profit_df = (trans_end - trans_start)/trans_start
    profit_df = profit_df.dropna()
    profit_se = profit_df['close']
    new_f_info = f_info_df.set_index(f_info_df['code'])
    market_se = new_f_info.loc[list(profit_df.index)]['market_cap']
    wt_profit = profit_se*market_se/market_se.sum()
    return wt_profit.sum()
    
## 收盘后运行函数
def after_market_close(context):
    ret_list_50 = get_index_stocks('000016.XSHG')
    ret_list_300 = get_index_stocks('000300.XSHG')
    ret_list = ret_list_50 + ret_list_300
    #log.info(ret)
    m_q_50 = query(
        valuation
        ).filter(
            valuation.code.in_(ret_list_50),
            valuation.pe_ratio > 0,
            )
    m_q_300 = query(
        valuation
        ).filter(
            valuation.code.in_(ret_list_300),
            valuation.pe_ratio > 0,
            )
    ret_df_50 = get_fundamentals(m_q_50, '2019-7-15')
    ret_df_300 = get_fundamentals(m_q_300, '2019-7-15')
    ret_df = pd.concat([ret_df_50, ret_df_300])
    ret_df = ret_df.drop_duplicates(['code'])
    #print(ret_df.columns)
    sort_pe_df = ret_df.sort_values(ascending = True,by = 'pe_ratio')
    sort_pb_df = ret_df.sort_values(ascending = True,by = 'pb_ratio')
    sort_ps_df = ret_df.sort_values(ascending = True,by = 'ps_ratio')
    sort_turnover_df = ret_df.sort_values(ascending = True,by = 'turnover_ratio')
    pe_code_df = sort_pe_df.loc[:,['code','pe_ratio']]
    pe_code_se = pe_code_df['code']
    pb_code_df = sort_pb_df.loc[:,['code','pb_ratio']]
    pb_code_se = pb_code_df['code']
    ret_pe_group = _gen_stocks_groups(pe_code_se)
    end_date = context.current_dt
    pe_w_profit_list = []
    for i,group_pd in enumerate(ret_pe_group):
        val = _get_on_group_profit(group_pd, end_date, ret_df)
        pe_w_profit_list.append(val)
    index_list = []
    for i in range(1,len(pe_w_profit_list)+1):
        index_list.append('port'+str(i))
    port_pe_pf = DataFrame({'pe':pe_w_profit_list},index = index_list)
    #log.info(port_pf)
    ret_pb_group = _gen_stocks_groups(pb_code_se)
    end_date = context.current_dt
    pb_w_profit_list = []
    for i,group_pd in enumerate(ret_pb_group):
        val = _get_on_group_profit(group_pd, end_date, ret_df)
        pb_w_profit_list.append(val)
    index_list = []
    for i in range(1,len(pb_w_profit_list)+1):
        index_list.append('port'+str(i))
    port_pb_pf = DataFrame({'pb':pb_w_profit_list},index = index_list)
    port_df = pd.concat([port_pe_pf, port_pb_pf],axis = 1)
    se_cmp = Series(range(1,len(pb_w_profit_list)+1),index = index_list)
    log.info(port_df.corrwith(se_cmp))
        
    
    
    
    
    
    
    
