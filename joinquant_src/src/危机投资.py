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

class DragonType(Enum):
    MODEL_RISK = 0
    MODEL_LIMIT_UP = 1

g.total_day = 80

g.m_end_date = 0

#g.test_close = True
g.test_close = False
#g.model_flag = DragonType.MODEL_LIMIT_UP
g.model_flag = DragonType.MODEL_RISK
global g_send_str
g_send_str = ""

global b_send_limit_two
#b_send_limit_two = True
b_send_limit_two = False

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
        if DragonType.MODEL_RISK == g.model_flag:
            stocks_list = self.__get_all_risk_stock_list()
        else:
            stocks_list = self.__get_all_stock_list()
        #stocks_list = ['600518.XSHG']
        for stock in stocks_list:
            if True == self._filter_paused_and_st_stock(stock) or\
            True == self._filter_su_board_stock(stock):
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
            valuation.pb_ratio > 0,
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
        return False
        if '3' == stock[0]:
            return True
        return False
        
    def __deal_dragon_stock(self, stock):
        global g_send_str
        his_df = self.get_stock_history_info(stock)
        #log.info(his_df)
        if 0 == len(his_df.index):
            return
        if True == b_send_limit_two:
            self._add_limit_up_two_day(his_df, stock)    #阶段性行情才能开启
        if DragonType.MODEL_RISK == g.model_flag:
            if True == self._is_crisis(his_df):
                g_send_str += stock
                g_send_str += " "
        elif DragonType.MODEL_LIMIT_UP == g.model_flag:
            self._deal_limit_up(his_df['close'], stock)
    #获取历史数据
    def get_stock_history_info(self, m_stock):
        tmp_end_date = g.m_end_date
        if True == g.test_close:
            last_info_tmp = get_price(m_stock, count = g.total_day,end_date=tmp_end_date, frequency='daily', fields=['close','money', 'open', 'low'])
        else:
            last_info_tmp = attribute_history(m_stock, g.total_day, '1d', ('close', 'volume', 'open', 'low'))
        his_df = last_info_tmp.sort_index(ascending = False)
        '''
        if True == self.__is_new_stock(his_df['close'][-1]):
            return DataFrame()
        '''
        return his_df
    def _is_new_stock(self,m_stock):
        last_info_tmp = get_price(m_stock, count = 1300,end_date=g.m_end_date, frequency='daily', fields=['close'])['close']
        return isnan(last_info_tmp[0])
    def _is_down_stock(self,m_stock):
        last_info_tmp = get_price(m_stock, count = 360,end_date=g.m_end_date, frequency='daily', fields=['close'])['close']
        max_val = last_info_tmp.max()
        min_val = last_info_tmp[-60:-1].min()
        #log.info(max_val, min_val)
        if (max_val - min_val)/max_val > 0.7:
            pass
            #return True
        return False
    def _deal_limit_up(self, close_se, stock):
        if True == self._is_limit_up(close_se):
            if True == self._is_new_stock(stock):
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
    
    def _is_crisis(self, his_df):
        close_se = his_df['close']
        volume_se = his_df['volume']
        low_se = his_df['low']
        ret_flag = False
        mean_volume = volume_se[1:4].mean()
        #跌停板放量
        if (close_se[1] - low_se[0])/float(close_se[1]) > 0.098:    #
            if volume_se[0]/mean_volume > 6:    #昨天放量
                ret_flag = True
        if (close_se[2] - close_se[1])/float(close_se[2]) > 0.098:    #前一天跌停
            #if (close_se[0] - close_se[1])/float(close_se[1]) > 0.098:   #昨天涨停
            if (close_se[0] - close_se[1])/float(close_se[1]) > 0.038:   #昨天大涨
                ret_flag = True
        if False == ret_flag:
            return False
        calc_crisis = CalcCrisis()
        calc_crisis.last_price = close_se[1]
        calc_crisis.max_day = len(close_se)
        calc_crisis.current_day = 0
        return self._judge_crisis(close_se[1:-1], calc_crisis)
    
    def _judge_crisis(self, close_se, calc_crisis):
        if calc_crisis.current_day >= calc_crisis.max_day - 1:
            return False
        if calc_crisis.fall_stop_times >= 4:
            return True
        calc_crisis.last_price = close_se[calc_crisis.current_day + 1]
        calc_crisis.today_price = close_se[calc_crisis.current_day]
        if (calc_crisis.last_price - calc_crisis.today_price)/float(calc_crisis.last_price) > 0.098:
            calc_crisis.fall_stop_times += 1
        else:
            calc_crisis.fall_stop_times = 0
            return False
        calc_crisis.current_day += 1
        return self._judge_crisis(close_se, calc_crisis)
    def _add_limit_up_two_day(self, his_data, stock):
        if g.test_close:
            vol_key = 'money'
        else:
            vol_key = 'volume'
        close_se = his_data['close']
        #log.info(close_se)
        open_se = his_data['open']
        vol_se = his_data[vol_key]
        min_close = close_se[2:-1].min()
        early_min_close = close_se[2:15].min()
        vol_mean = vol_se[2:-1].mean()
        vol_mean_10 = vol_se[2:10].mean()
        max_open = open_se[2:-1].max()
        max_close = close_se[2:-1].max()
        max_val = max_open if max_open > max_close else max_close
        #log.info('mean = %f, curr = %f'%(vol_mean, vol_se[0]))
        if (close_se[0] - close_se[1])/float(close_se[1]) > 0.098 \
        and (close_se[1] - close_se[2])/float(close_se[2]) > 0.098 \
        and (close_se[2] - close_se[3])/float(close_se[3]) < 0.098:
            #log.info('here')
            if (close_se[0] == open_se[0]):
                if vol_se[0] < vol_se[1]:
                    #log.info(1)
                    return
            if vol_se[0] < vol_mean*0.7 or vol_se[1] < vol_mean*0.7:
                #log.info(2)
                return
            '''
            if vol_se[0] > vol_mean_10 * 4 or vol_se[1] > vol_mean_10 * 4:
                log.info(3)
                return
            '''
            #log.info(max_val, close_se[0])
            #log.info(min_close,min_close*1.5, close_se[1], early_min_close)
            if max_val < close_se[0]*1.2 and close_se[1] < min_close*1.5 and close_se[1] < early_min_close * 1.25:
                if True == self._is_new_stock(stock):
                    #log.info(4)
                    return
                #log.info('here2')
                #剔除超跌
                if True == self._is_down_stock(stock):
                    #log.info(5)
                    return
                g.limit_up_two_list.append(stock)
            #log.info('here3')

class RunContainer:
    def __init__(self):
        pass
    def before_trading_action(self, context):
        get_m_end_date(context)
        analyStock = AnalyStock()
        analyStock.dragon_main()
        send_msg_to_weixin()
    def trading_action(self, context, data):
        pass
    def after_trading_action(self, context):
        pass

def send_msg_to_weixin():
    if False == g.b_send_flag:
        g.b_send_flag = True
        send_str1 = ""
        send_str2 = ""
        if 0 != len(g.limit_up_two_list):
            send_str1 = "!!!两连扳 stocks: "
            for item in g.limit_up_two_list:
                send_str1 += item
                send_str1 += " "
        if "" != g_send_str:
            send_str2 = "跌停放量 stock %s!"%(g_send_str)
        if "" != send_str1 or "" != send_str2:
            send_str = send_str1 + "\n" + send_str2
            log.info(send_str)
            send_message(send_str)
        
def initialize(context):
    g.RunContainer = RunContainer()
    g.b_send_flag = False
    g.b_has_dragon = False
    g.limit_up_two_list = []
    
def before_trading_start(context):
    g.b_send_flag = False
    global g_send_str
    g_send_str = ""
    g.limit_up_two_list = []
    g.RunContainer.before_trading_action(context)
def after_trading_end(context):
    g.RunContainer.after_trading_action(context)
def handle_data(context, data):
    g.RunContainer.trading_action(context, data)
    
        

    

