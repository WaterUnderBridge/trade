# 导入聚宽函数库
import jqdata
import time
import numpy as np
import pandas as pd
from pandas import DataFrame,Series


# 初始化函数，设定要操作的股票、基准等等
def initialize(context):
    # 定义一个全局变量, 保存要操作的股票
    # 000001(股票:平安银行)
    g.security = '000001.XSHE'
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)

# 每个单位时间(如果按天回测,则每天调用一次,如果按分钟,则每分钟调用一次)调用一次
def handle_data(context, data):
    pass

def deco_time(in_func):
    def calc_time(*args, **kwargv):
        time0 = time.time()
        result = in_func(*args, **kwargv)
        diff_time = time.time() - time0
        log.info("[time = %.8f] %s"%(diff_time, in_func.__name__))
        return result
    return calc_time

@deco_time
def var_test():
    # 正态分布概率表，标准差倍数以及置信率
    # 1.96, 95%; 2.06, 96%; 2.18, 97%; 2.34, 98%; 2.58, 99%; 5, 99.9999%
    stock_weight_dict = {"600886.XSHG":0.4, "000999.XSHE":0.6}
    #stock_weight_dict = {"000999.XSHE":1}
    stock_weight_series = Series(stock_weight_dict)
    val_dict = {}
    one_day_val_dict = {}
    var_ratio_dict_his = {}
    data_num = 10
    day_inter = 1
    format_his_data(data_num, day_inter, stock_weight_series.index, val_dict, one_day_val_dict, var_ratio_dict_his)
    stock_close_pd = pd.DataFrame(val_dict, columns = stock_weight_series.index)
    one_day_stock_close_pd = pd.DataFrame(one_day_val_dict, columns = stock_weight_series.index)
    var_ratio_series_his = Series(var_ratio_dict_his, index = stock_weight_series.index)
    #log.info(stock_close_pd)
    his_var = get_comb_var_by_his_info(stock_weight_series, var_ratio_series_his)
    cov_var = get_coff_by_var_and_cov(stock_weight_series, stock_close_pd)
    monte_var = get_monte_var(stock_weight_series, one_day_stock_close_pd, day_inter)
    
def format_his_data(data_num, day_inter, in_index, val_dict, one_day_val_dict, var_ratio_dict_his):
    for stock in in_index:
        # 获取股票的收盘价
        stock_close_pd_init = attribute_history(stock, data_num, '1d', ['close'])
        #log.info(stock_close_pd_init)
        stock_close_pd = stock_close_pd_init[::day_inter]
        #log.info(stock_close_pd)
        close_data_shift = stock_close_pd.shift(1)
        rate_df = (stock_close_pd - close_data_shift)/close_data_shift
        new_df = rate_df.fillna(0)
        stock_rate_series = new_df['close']
        val_dict[stock] = stock_rate_series
        var_ratio_dict_his[stock] = get_stock_var_by_his(stock_rate_series)
        one_day_rate_df_tmp = (stock_close_pd_init - stock_close_pd_init.shift(1))/stock_close_pd_init.shift(1)
        one_day_rate_df = one_day_rate_df_tmp.fillna(0)
        one_day_rate_series = one_day_rate_df['close']
        one_day_val_dict[stock] = one_day_rate_series
    
def get_comb_var_by_his_info(stock_weight_series, var_ratio_dict_series):
    his_var = get_dot_val_by_series(stock_weight_series, var_ratio_dict_series)
    log.info("his_var = %f"%(his_var))
    return his_var
    
def get_dot_val_by_series(a_series,b_series):
    sum_series = a_series * b_series
    return sum_series.sum()

def get_stock_var_by_his(stock_rate_series):
    sort_rate = sort(stock_rate_series)
    int_index = int(len(stock_rate_series) * 0.05)
    f_var = sort_rate[int_index]
    log.info("int_index = %d, his_var = %f"%(int_index, f_var))
    return f_var
    
def get_coff_by_var_and_cov(stock_weight_series, stock_close_pd):
    miu, rate_std = get_miu_and_cov(stock_weight_series, stock_close_pd)
    stat_var = miu - rate_std * 1.96
    log.info("miu = %f, rate_std = %f, coff_var = %f"%(miu, rate_std, stat_var))
    return (stat_var, miu, rate_std)

def get_miu_and_cov(stock_weight_series, stock_close_pd):
    stock_u_series = stock_close_pd.mean()
    stock_cov_pd = stock_close_pd.cov()
    miu = get_dot_val_by_series(stock_weight_series, stock_u_series)
    weight_array = stock_weight_series.values
    cov_array = stock_cov_pd.values
    dot_tmp = np.dot(weight_array.T, cov_array)
    rate_var = np.dot(dot_tmp, weight_array)
    rate_std = np.sqrt(rate_var)
    return (miu, rate_std)

def get_monte_var(stock_weight_series, one_day_stock_close_pd, sample_num):
    miu, sigma = get_miu_and_cov(stock_weight_series, one_day_stock_close_pd)
    monte_cnt = 20000
    ret_list = []
    np.random.seed(0)
    for i in range(monte_cnt):
        s = np.random.normal(miu, sigma, sample_num)
        m_ret = 1
        #log.info(s)
        for rate_item in s:
            if rate_item < -0.1:
                rate_item = -0.1
            elif rate_item > 0.1:
                rate_item = 0.1
            m_ret *= (1 + rate_item)
        ret_list.append(m_ret)
    int_index = int(monte_cnt * 0.05)
    sort_list = sort(ret_list)
    monte_var = sort_list[int_index] - 1
    log.info("monte_var = %f"%(monte_var))

def write_test():
    df = attribute_history('300236.XSHE', 300, '1d') #获取DataFrame表
    write_file('df.csv', df.to_csv(), append=False) #写到文件中

def after_trading_end(context):
    write_test()
    #var_test()
    
    