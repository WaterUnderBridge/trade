from math import isnan
import time
import datetime
from jqdata import *
from pandas import DataFrame, Series
from enum import Enum
import talib
import numpy as np
from scipy import optimize
import copy
import random
from jqdata import jy
from numpy import NaN
import matplotlib.pyplot as plt

class DragonType(Enum):
    MODEL_RISK = 0
    MODEL_LIMIT_UP = 1

g.b_calc_today = False
#g.b_calc_today = True
g.total_day = 960

g.m_end_date = 0

g.model_flag = DragonType.MODEL_LIMIT_UP
g.model_flag = DragonType.MODEL_RISK

def calc_time(_in_func):
    def _func(*args, **kwargs):
        time1 = time.time()
        ret = _in_func(*args, **kwargs)
        time2 = time.time()
        log.info('%s need time : %f'%(_in_func.__name__,time2 - time1))
        return ret
    return _func

class TimeTool:
    def __init__(self):
        pass
    def get_end_data(self, m_today):
        m_year = str(m_today.year)
        m_month = str(m_today.month)
        m_day = str(m_today.day)
        if 1 == len(m_month):
            m_month = '0' + m_month
        if 1 == len(m_day):
            m_day = '0' + m_day
        return m_year+'-'+m_month+'-'+m_day

class CalcCrisis:
    def __init__(self):
        self.last_price = 0
        self.today_price = 0
        self.fall_stop_times = 0
        self.limit_up_times = 0
        self.max_day = 0
        self.current_day = 0
    
def get_m_end_date(context):
    timeTool = TimeTool()
    m_today = context.current_dt
    g.m_end_date = timeTool.get_end_data(m_today)

#分析股票***********************************************************************************************
class AnalyStock:
    def __init__(self):
        pass
    #@calc_time
    def dragon_main(self):
        stocks_list = self.__get_all_risk_stock_list()
        #log.info(stocks_list)
        #stocks_list = ['300508.XSHE']
        for stock in stocks_list:
            if True == self._filter_paused_and_st_stock(stock):
                continue
            self.__deal_dragon_stock(stock)
        #log.info('inter = %d, total = %d'%(g.inter_stock, g.total_stock))
    def __get_all_stock_list(self):
        #log.info(get_all_securities())
        return list(get_all_securities(['stock']).index)
    #获取股票池
    def __get_all_risk_stock_list(self):
        stock_list = []
        stock_df = get_fundamentals(query(
            valuation.code, valuation.market_cap, valuation.pe_ratio, valuation.pb_ratio,income.total_operating_revenue
        ).filter(
            valuation.pe_ratio > 0,
            valuation.market_cap < 30,
     #       income.total_operating_revenue > 2e10
        ).order_by(
            # 按市值降序排列
            valuation.market_cap.desc()
        ).limit(
            # 最多返回10000个
            10000
        ), None)
        tmp_list = list(stock_df['code'])
        for m_item in tmp_list:
            stock_list.append(m_item)
        return stock_list
    def _filter_paused_and_st_stock(self, stock):
        current_data = get_current_data()
        if current_data[stock].paused:
            return True
        if current_data[stock].is_st or 'ST' in current_data[stock].name or '*' in current_data[stock].name or '退' in current_data[stock].name:
            return True
        return False
    def _filter_su_board_stock(self, stock):
        if '3' == stock[0]:
            return True
        return False
        
    def __deal_dragon_stock(self, stock):
        last_info_tmp = get_price(stock, count = g.total_day,end_date=g.m_end_date, frequency='daily', fields=['close'])['close']
        if True == self._is_new_stock(last_info_tmp[0]):
            if last_info_tmp[-1] < last_info_tmp.max() * 0.17:
                log.info(stock, last_info_tmp[-1], last_info_tmp.max())
            return
        else:
            return
        his_df = self.get_stock_history_info(stock)
        #log.info(his_df)
        if 0 == len(his_df.index):
            retur
        if True == self._is_crisis(his_df['close']):
            log.info(stock)
    #获取历史数据
    def get_stock_history_info(self, m_stock):
        tmp_end_date = g.m_end_date
        if True == g.b_calc_today:
            tmp_end_date = time.strftime("%Y-%m-%d", time.localtime())
        last_info_tmp = get_price(m_stock, count = g.total_day,end_date=tmp_end_date, frequency='daily', fields=['close','money','high','low','open'])
        his_df = last_info_tmp.sort_index(ascending = False)
        return his_df
    def _is_new_stock(self,val):
        return isnan(val)
    def _deal_limit_up(self, close_se, stock):
        if True == self._is_limit_up(close_se):
            if True == self._is_new_stock(0):
                log.info(stock + " new")###########################################################################################################
                pass
            else:
                log.info(stock)###########################################################################################################
            pass
    def _is_limit_up(self, close_se):
        if (close_se[0] - close_se[1])/float(close_se[1]) > 0.099:
            return False
        calc_limit = CalcCrisis()
        calc_limit.last_price = close_se[0]
        calc_limit.max_day = len(close_se)
        calc_limit.current_day = 0
        return self._judge_limit_up(close_se, calc_limit)
    def _judge_limit_up(self, close_se, calc_limit):
        if calc_limit.current_day >= calc_limit.max_day:
            return False
        if calc_limit.limit_up_times >= 5:
            return True
        calc_limit.today_price = close_se[calc_limit.current_day]
        if (calc_limit.last_price - calc_limit.today_price)/float(calc_limit.today_price) > 0.099:
            calc_limit.limit_up_times += 1
        else:
            calc_limit.limit_up_times = 0
        calc_limit.last_price = close_se[calc_limit.current_day]
        calc_limit.current_day += 1
        return self._judge_limit_up(close_se, calc_limit)
    def _is_crisis(self, close_se):
        if (close_se[1] - close_se[0])/float(close_se[1]) > 0.099:
            return False
        calc_crisis = CalcCrisis()
        calc_crisis.last_price = close_se[0]
        calc_crisis.max_day = len(close_se)
        calc_crisis.current_day = 0
        return self._judge_crisis(close_se, calc_crisis)
    
    def _judge_crisis(self, close_se, calc_crisis):
        if calc_crisis.current_day >= calc_crisis.max_day:
            return False
        if calc_crisis.fall_stop_times >= 5:
            return True
        calc_crisis.today_price = close_se[calc_crisis.current_day]
        if (calc_crisis.today_price - calc_crisis.last_price)/float(calc_crisis.today_price) > 0.099:
            calc_crisis.fall_stop_times += 1
        else:
            calc_crisis.fall_stop_times = 0
        calc_crisis.last_price = close_se[calc_crisis.current_day]
        calc_crisis.current_day += 1
        return self._judge_crisis(close_se, calc_crisis)

class RunContainer:
    def __init__(self):
        pass
    def before_trading_action(self, context):
        get_m_end_date(context)
        analyStock = AnalyStock()
        analyStock.dragon_main()
    def trading_action(self, context, data):
        pass
    def after_trading_action(self, context):
        pass
        
def initialize(context):
    g.RunContainer = RunContainer()
    g.str_send = ""
    g.b_send_flag = False
    g.b_has_dragon = False

def get_ma5(in_se):
    calc_len = 5
    new_list = []
    for index in range(len(in_se)):
        if index < len(in_se) - calc_len:
            new_list.append(in_se[index:index + calc_len].mean()) 
    return Series(new_list)
    
def judge_money_increase(money_se, date_list):
    calc_len = 5
    for m_index,single_money in enumerate(money_se):
        if m_index < len(money_se) - calc_len:
            if money_se[m_index] > money_se[m_index + calc_len]*1.6:
                print(date_list[m_index])
    
def before_trading_start(context):
    df = attribute_history('600783.XSHG', 190, '1d') #获取DataFrame表
    write_file('df.csv', df.to_csv(), append=False) #写到文件中
    df_sort = df.sort_index(ascending = False)
    money_se = get_ma5(df_sort['money'])
    judge_money_increase(money_se,df_sort.index)
    g.RunContainer.before_trading_action(context)
def after_trading_end(context):
    g.RunContainer.after_trading_action(context)
def handle_data(context, data):
    g.RunContainer.trading_action(context, data)
    