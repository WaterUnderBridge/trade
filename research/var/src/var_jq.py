# 导入聚宽函数库
import jqdata
import time
import numpy as np
import pandas as pd


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
        log.info("[%.8f] %s"%(diff_time, in_func.__name__))
        return result
    return calc_time

@deco_time
def var_test():
    # 正态分布概率表，标准差倍数以及置信率
    # 1.96, 95%; 2.06, 96%; 2.18, 97%; 2.34, 98%; 2.58, 99%; 5, 99.9999%
    stock_weight_dict = {"000999.XSHE":0.6, "600886.XSHG":0.4}
    #stock_weight_dict = {"000999.XSHE":1}
    val_dict = {}
    var_ratio_dict_his = {}
    data_num = 100
    for stock in stock_weight_dict.keys():
        # 获取股票的收盘价
        stock_close_pd = attribute_history(stock, data_num, '1d', ['close'])
        close_data_shift = stock_close_pd.shift(1)
        rate_df = (stock_close_pd - close_data_shift)/close_data_shift
        new_df = rate_df.fillna(0)
        stock_rate_series = new_df['close']
        val_dict[stock] = stock_rate_series
        var_ratio_dict_his[stock] = get_stock_var_by_his(stock_rate_series)
    stock_close_pd = pd.DataFrame(val_dict)
    #log.info(stock_close_pd)
    his_var = get_comb_var_by_his_info(stock_weight_dict, var_ratio_dict_his)
    log.info("his_var = %f"%(his_var))
    get_coff_by_var_and_cov(stock_weight_dict, stock_close_pd)

def get_comb_var_by_his_info(stock_weight_dict, var_ratio_dict_his):
    return get_dot_val_by_dicts(stock_weight_dict, var_ratio_dict_his)
    
def get_dot_val_by_dicts(a_dict,b_dict):
    a_list = []
    b_list = []
    for key in a_dict.keys():
        a_list.append(a_dict[key])
        b_list.append(b_dict[key])
    a_array = np.array(a_list)
    b_array = np.array(b_list)
    result = np.dot(a_array, b_array)
    return result

def get_stock_var_by_his(stock_rate_series):
    sort_rate = sort(stock_rate_series)
    int_index = int(len(stock_rate_series) * 0.05)
    f_var = sort_rate[int_index]
    #log.info("int_index = %d, his_var = %f"%(int_index, f_var))
    return f_var
    
def get_coff_by_var_and_cov(stock_weight_dict, stock_close_pd):
    stock_u = stock_close_pd.mean()
    stock_cov_pd = stock_close_pd.cov()
    stock_index = stock_cov_pd.index
    miu = get_dot_val_by_dicts(stock_weight_dict, stock_u)
    weight_list = []
    for stock in stock_index:
        weight_list.append(stock_weight_dict[stock])
    weight_array = np.array(weight_list)
    cov_array = stock_cov_pd.values
    dot_tmp = np.dot(weight_array, cov_array)
    rate_var = np.dot(dot_tmp, weight_array.T)
    rate_std = np.sqrt(rate_var)
    stat_var = miu - rate_std * 1.96
    log.info("miu = %f, rate_std = %f, coff_var = %f"%(miu, rate_std, stat_var))
    return stat_var

def after_trading_end(context):
    var_test()
    
    