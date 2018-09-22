import time
import numpy as np
from pandas import DataFrame, Series
import talib
from scipy import optimize
import copy
from enum import Enum

g.m_end_data = 0
g.b_calc_today = False
#g.b_calc_today = True
g.total_day = 200

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

class DataModel:
    def __init__(self, stock):
        self.m_stock = stock
        self.__init_wave_rate(stock)
    def __init_wave_rate(self, stock):
        self.dict_up_wave_rate = 0.025
        self.dict_down_wave_rate = 0.025
    def get_dict_up_wave_rate(self):
        return self.dict_up_wave_rate
    def get_dict_down_wave_rate(self):
        return self.dict_down_wave_rate
        
#判断买卖点****************************************************************************************************
class JudgeTradePoint:
    def __init__(self, stock):
        self.m_stock = stock
        self.dataCtrl = DataModel(stock)
        self._init_data_info(stock)
        self.__update_stock_info(stock)
    def _init_data_info(self, stock):
        close_data = attribute_history(stock, 1, '1d', ['close'])
        self.last_day_close_price = (close_data['close'][-1])
        self.last_trade_price = (close_data['close'][-1])
        Volume_last_3_day=history(3,'1d','money',stock).sum()[stock]
        self.last_mean_money = (Volume_last_3_day/(3*4*60))
        self.last_up_alarm_time = 0
        self.last_down_alarm_time = 0
    def __update_stock_info(self, stock):
        self.b_price_up = False
        self.b_price_down = False
    def deal_price_wave(self, data, context):
        current_price = data[self.m_stock].close
        curr_time = context.current_dt
        self.__deal_price_BreakThrough(current_price, curr_time)
        if True == self.__is_price_BreakThrough(current_price, curr_time):
            change_str = self.__rate_str(current_price, self.last_day_close_price)
            self.__send_msg_to_user(change_str)
        self.__deal_price_BreakDown(current_price, curr_time)
        if True == self.__is_price_BreakDown(current_price, curr_time):
            change_str = self.__rate_str(current_price, self.last_day_close_price)
            self.__send_msg_to_user(change_str)
    def __deal_price_BreakThrough(self, current_price, curr_time):
        if True == self.b_price_up:
            return
        if current_price > self.last_trade_price*(1 + self.dataCtrl.get_dict_up_wave_rate()):
            self.b_price_up = True
            self.last_trade_price = current_price
            self.last_up_alarm_time = curr_time
    def __is_price_BreakThrough(self, current_price, curr_time):
        if False == self.b_price_up:
            return False
        if True == self.__is_continue_alarm(curr_time, self.last_up_alarm_time):
            return False
        if current_price > self.last_trade_price:
            self.last_trade_price = current_price
            self.last_up_alarm_time = curr_time
            return False
        else:
            self.b_price_up = False
            return True
    def __deal_price_BreakDown(self, current_price, curr_time):
        if True == self.b_price_down:
            return
        if current_price < self.last_trade_price*(1 - self.dataCtrl.get_dict_down_wave_rate()):
            self.b_price_down = True
            self.last_trade_price = current_price
            self.last_down_alarm_time = curr_time
    def __is_price_BreakDown(self, current_price, curr_time):
        if False == self.b_price_down:
            return False
        if True == self.__is_continue_alarm(curr_time, self.last_down_alarm_time):
            return False
        if current_price < self.last_trade_price:
            self.last_trade_price = current_price
            self.last_down_alarm_time = curr_time
            return False
        else:
            self.b_price_down = False
            return True
    def __get_diff_second(self, time_now, last_time):
        if (last_time.hour <= 11 and time_now.hour >=13):
            hour_val = 3600*(time_now.hour - last_time.hour) - 3600*1.5
        else:
            hour_val = 3600*(time_now.hour - last_time.hour)
        diff_second = hour_val + 60*(time_now.minute - last_time.minute)\
        + (time_now.second - last_time.second)
        return diff_second
    def __is_continue_alarm(self, time_now, last_time):
        diff_second = self.__get_diff_second(time_now, last_time)
        #log.info(diff_second)
        if (diff_second < 1*60):
            return True
        else:
            return False
    def __rate_str(self, curr_val, last_val):
        m_sec = self.m_stock
        ret_str = ''
        float_rate = (curr_val - last_val)/last_val*100
        ret_str = m_sec +", price : "+ str(round(float_rate, 4))
        return ret_str
    def __send_msg_to_user(self, send_str):
        log.info(send_str)
        send_message(send_str)
        
#消息发送**************************************************************************************************
class SendMsg:
    def __init__(self,stock_list):
        self.m_security_list = copy.deepcopy(stock_list)
        self.stock_ctrl_dict = {}
        self.__init_val()
    def __init_val(self):
        self.sse_last_day_price = attribute_history('000001.XSHG', 1, '1d', ['close'])['close'][-1]
        self.sse_last_price = self.sse_last_day_price
        for stock in self.m_security_list:
            judgeTradePoint = JudgeTradePoint(stock)
            self.stock_ctrl_dict[stock] = judgeTradePoint
            
        # 获取股票前一天的收盘价
    def deal_stock_change(self, context, data):
        for m_sec in self.m_security_list:
            self.__judge_price_wave(context, m_sec, data)
    def __judge_price_wave(self, context, m_sec, data):
        self.stock_ctrl_dict[m_sec].deal_price_wave(data, context)
    def deal_sse_change(self, data):
        sse_stock = '000001.XSHG'
        current_price = data[sse_stock].close
        sse_wave_rate = 0.007
        if current_price > self.sse_last_price*(1 + sse_wave_rate) or\
        current_price < self.sse_last_price*(1 - sse_wave_rate):
            float_rate = (current_price - self.sse_last_day_price)/self.sse_last_day_price*100
            change_str = "sse price wave_rate: "+ str(round(float_rate, 4))
            print(change_str)
            send_message(change_str) 
            self.sse_last_price = current_price

def _ya_xian_zhi_last_close():
    g.sse_crash = False
    g.blue_chip_list = ['000001.XSHE','601988.XSHG','601288.XSHG','601939.XSHG']
    g.blue_close_list = []
    for blue_stock in g.blue_chip_list:
        close_data = attribute_history(blue_stock, 1, '1d', ['close'])
        g.blue_close_list.append(close_data['close'][-1])

def _ya_xian_zhi(data):
    blue_rate = 0.95
    if (True == g.sse_crash):
        return
    for index,blue_stock in enumerate(g.blue_chip_list):
        current_price = data[blue_stock].close
        last_price = g.blue_close_list[index]
        if current_price < last_price * blue_rate:
            g.sse_crash = True
            change_str = g.blue_chip_list[index] + " blue crash!!!"
            print(change_str)
            send_message(change_str)
            return
def is_zte_open(data):
    g.zte_money += data['000063.XSHE'].volume
    
    if False == g.b_zte_flag and g.zte_money > 10000000:
        send_str = "zte will open! price = %f"%(data['000063.XSHE'].close)
        log.info(send_str)
        send_message(send_str)
        g.b_zte_flag = True
    if data['000063.XSHE'].close == g.last_zte_close:
        return
    if False == g.b_zte_flag and data['000063.XSHE'].close > g.last_zte_close * 0.901:
        send_str = "open zte! price = %f"%(data['000063.XSHE'].close)
        log.info(send_str)
        send_message(send_str)
        g.b_zte_flag = True
def initialize(context):
    g.b_zte_flag = False
    g.zte_money = 0
    g.last_zte_close = attribute_history("000063.XSHE", 1, '1d', ['close'])['close'][-1]

def after_trading_end(context):
    pass

def process_initialize(context):
    # query 对象不能被 pickle 序列化, 所以不能持久保存, 所以每次进程重启时对它初始化
    # 以两个下划线开始, 系统序列化 [g] 时就会自动忽略这个变量, 更多信息, 请看 [g] 和 [模拟盘注意事项]
    pass
        
def after_code_changed(context):
    pass
        
def before_trading_start(context):
    _ya_xian_zhi_last_close()
    timeTool = TimeTool()
    m_today = context.current_dt
    g.m_end_data = timeTool.get_end_data(m_today)
    g.sendMsgClass = SendMsg(['603297.XSHG','601068.XSHG'])
        
 
# 每个单位时间(如果按天回测,则每天调用一次,如果按分钟,则每分钟调用一次)调用一次
def handle_data(context, data):
    g.sendMsgClass.deal_sse_change(data)
    g.sendMsgClass.deal_stock_change(context, data)
    #_ya_xian_zhi(data)
    #is_zte_open(data)
                
            
        
    
    