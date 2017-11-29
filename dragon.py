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
from jqdata import gta

class RunMode(Enum):
    SELECT_STOCK_MODE = 1
    SEND_MSG_MODE = 2
    BACK_TEST_MODE = 3

class PrintMode(Enum):
    PRINT_PRICE = 1
    PRINT_MONEY = 2

class PriceMode(Enum):
    INTER_SHOCK = 1
    PRICE_LOW = 2
    PRICE_HIGH = 3
    PRICE_DOWN = 4
    PRICE_ERROR = 5
    
class SSEMode(Enum):
    SSE_LOW = 1
    SSE_HIGH = 2
    SSE_MIDDLE = 3
    
class DragonType(Enum):
    DRAGON_NONE = 0
    DRAGON_LIMIT_UP = 1
    DRAGON_VOLUME = 2
    DRAGON_GAP = 3

g.b_calc_today = False
#g.b_calc_today = True
g.total_day = 200

g.m_end_date = 0

g.total_stock = 0
g.inter_stock = 0

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

#一次拟合**************************************************************************************************        
class FittK:
    def __fmax(self, x,k,b):
        return k * x + b
    def get_ma10_k(self, mean_series):
        mean_list = list(mean_series)
        mean_list.reverse()
        ma10_len = len(mean_series)
        ma10_range = range(0,ma10_len)
        x_val = np.array(ma10_range)
        x_val = x_val/10.0
        fita,fitb=optimize.curve_fit(self.__fmax,x_val,np.array(mean_list),[1,1])
        line_k = fita[0]
        return line_k
#仓位控制******************************************************************************************************
class PositionCtrl:
    def __init__(self):
        self.total_money = 110000.0
        self.posi_coff = (self.total_money/100000.0)
        self.single_speculate_money = 100000.0/3
        self.one_layer_money = 100000.0/10
    def take_posi(self, stock, p_index, data, context, b_buy_list):
        if -1 == p_index:
            return " ,pindex error "
        remaid_cash = context.subportfolios[p_index].available_cash
        current_price = data[stock].close
        one_hand_money = 100 * current_price
        if remaid_cash < self.one_layer_money:
            return " ,buy amount 0 "
        if self.one_layer_money < one_hand_money:
            return " ,buy amount 0 "
        cash = int(self.one_layer_money/one_hand_money) * 100 * current_price
        #log.info(cash)
        ret_obj = order_value(stock, cash, pindex = p_index)
        if None == ret_obj:
            log.info("take_posi error!")
            return " ,buy amount 0 "
        b_buy_list[0] = True
        return (" ,buy amount " + str(int(cash/one_hand_money)*100))
    def clear_posi(self, stock, p_index, data, context):
        if -1 == p_index:
            return " ,pindex error "
        posi_val = context.subportfolios[p_index].positions_value
        if posi_val == 0:
            return " ,clear amount 0 "
        current_price = data[stock].close
        one_hand_money = 100 * current_price
        ep_amount = 0
        if self.one_layer_money > one_hand_money:
            layer_num = int(posi_val/self.one_layer_money)
            if layer_num < 2:
                ep_amount = 0
            else:
                ep_amount = int(self.one_layer_money/one_hand_money) * 100
        else:
            ep_amount = 0
        ret_obj = order_target(stock, ep_amount, pindex = p_index)
        if None == ret_obj:
            log.info("clear_posi error!")
            return " ,clear amount 0 "
        return (" ,clear amount " + str(ep_amount))

#通过数据分析获取短线强势票************************************************************************************
class DataAnalyzeBase:
    def __init__(self):
        self.m_dragon_volume_val = 0
    def get_dragon_type(self, stock):
        his_df = self.get_stock_history_info(stock)
        #log.info(his_df)
        if 0 == len(his_df.index):
            return DragonType.DRAGON_NONE
        price_mode = self.__price_pattern_match(his_df['close'])
        if (True == self.__is_need_fliter_by_pattern(price_mode)):
            return DragonType.DRAGON_NONE
        dragon_type = self.__is_dragon_stock(his_df,stock)
        return dragon_type
    #获取历史数据
    def get_stock_history_info(self, m_stock):
        tmp_end_date = g.m_end_date
        if True == self.__is_need_filter(m_stock):
            return DataFrame()
        if True == g.b_calc_today:
            tmp_end_date = time.strftime("%Y-%m-%d", time.localtime())
        last_info_tmp = get_price (m_stock, count = g.total_day,end_date=tmp_end_date, frequency='daily', fields=['close','money','high','low','open'])
        his_df = last_info_tmp.sort_index(ascending = False)
        if True == self.__is_new_stock(his_df['close'][-1]):
            return DataFrame()
        return his_df
    def __is_new_stock(self,val):
        return isnan(val)
    def __is_carve_out_board(self, ch):
        if '3' == ch:
            return True
        else:
            return False
    def __is_need_filter(self, item):
        if '000594.XSHE' == item \
                or '000562.XSHE' == item:
            return True
        code_list = list(item)
        if self.__is_carve_out_board(code_list[0]):
            return True
        return False
    #获取股票模式
    def __price_pattern_match(self, val_list):
        fittK = FittK()
        mode_ret = PriceMode.PRICE_ERROR
        two_month_min_val = val_list[0:40].min()
        two_month_max_val = val_list[0:40].max()
        two_month_up_rate = (two_month_max_val - two_month_min_val)/two_month_min_val
        if (two_month_up_rate > 0.35):
            mode_ret = PriceMode.PRICE_HIGH
            return mode_ret
        avg_type = 10
        ret_ma = talib.MA(val_list.values, timeperiod=avg_type, matype=0)
        mean_series = ret_ma[avg_type:g.total_day]
        df_close = sort(mean_series)
        now_mean_val = (val_list.head(10).sum() - val_list.head(5).sum())/5
        min_mean_val = df_close[0:5].mean()
        max_mean_val = df_close[-5:].mean()
        if (0 == min_mean_val):
            mode_ret = PriceMode.PRICE_ERROR
            return mode_ret
        close_wave_rate = (max_mean_val - min_mean_val)/min_mean_val
        now_wave_rate = (now_mean_val - min_mean_val)/min_mean_val
        #log.info('now_mean_val = %f, min_mean_val = %f, max_mean_val = %f'%(now_mean_val, min_mean_val, max_mean_val))
        #log.info('close_wave_rate = %f, now_wave_rate = %f'%(close_wave_rate, now_wave_rate))
        if (close_wave_rate < 0.4):
            #g.inter_stock += 1
            mode_ret = PriceMode.INTER_SHOCK
        elif (now_wave_rate < 0.15):
            mode_ret = PriceMode.PRICE_LOW
        else:
            mode_ret = PriceMode.PRICE_HIGH
        self.calc_line_k = fittK.get_ma10_k(mean_series)
        if (self.calc_line_k < 0):
            mode_ret = PriceMode.PRICE_DOWN
        return mode_ret
    def __is_need_fliter_by_pattern(self, price_pattern):
        if (price_pattern == PriceMode.PRICE_HIGH or PriceMode.PRICE_DOWN == price_pattern or \
        PriceMode.PRICE_ERROR == price_pattern):
            return True
        else:
            return False
    #股票筛选
    def __is_dragon_stock(self, his_df, stock):
        if (True == self.__is_limit_up(his_df,stock)):
            return DragonType.DRAGON_LIMIT_UP
            #return DragonType.DRAGON_NONE
        if (True == self.__is_price_gap(his_df, stock)):
            return  DragonType.DRAGON_GAP
            #return DragonType.DRAGON_NONE
        #if (True == is_volume_mustang(his_df,stock,m_end_data)):
           #return DragonType.DRAGON_VOLUME
           #return DragonType.DRAGON_NONE
        return DragonType.DRAGON_NONE
    def __is_limit_up(self, his_df,stock):
        b_dragon = False
        val_list = his_df['close']
        money_list = his_df['money']
        #g.total_stock += 1
        for index in range(1,4):
            if val_list[index] > val_list[index + 1] * 1.099:    #涨停
                if True == self.__dragon_price_check(val_list, index):
                    if True == self.__dragon_money_check(money_list, index):
                        self.__get_volume_score(money_list, index)
                        b_dragon =  True
                    break
        return b_dragon
    def __dragon_price_check(self, val_list, index):
        coef_mean_val = 1.5
        coef_current_val = 1.03
        current_price = val_list[0]
        half_year_mean_val = round(val_list.mean(), 2)
        #log.info("index = %d"%index)
        #log.info("current_price = %f,  half_year_mean_val = %f, val_list[index] = %f"%(current_price, half_year_mean_val, val_list[index]))
        #半年均线比较
        if current_price <  half_year_mean_val*coef_mean_val \
        and current_price < val_list[index]*coef_current_val:    #回调
            #log.info("price true")
            return True
        return False
    def __dragon_money_check(self, money_list, limit_index):
        coef_money_after = 0.9
        coef_money_rate = 2
        money_after = money_list[limit_index - 1]
        money_limit = money_list[limit_index]
        money_mean_after = money_list[0:limit_index + 1].mean()
        money_mean_before = money_list[limit_index + 2:(limit_index + 2 + limit_index + 1)].mean()
        #log.info('money_mean_before = %f,money_after = %f'%(money_mean_before,money_after))
        if 0 == money_mean_before or 0 == money_after:
            return False
        moeny_coef = money_limit / money_after
        money_rate = money_mean_after / money_mean_before
        #log.info("moeny_coef = %f,money_rate = %f"%(moeny_coef,money_rate))
        #缩量回调
        if moeny_coef > coef_money_after and \
        money_rate > coef_money_rate:    
            #log.info("money true")
            return True
        return False
    def __get_volume_score(self, money_list, limit_index):
        set_score = 0
        if (limit_index == 0):
            money_limit = money_list[limit_index]
            money_mean_before = money_list[2:7].mean()
            if 0 == money_mean_before:
                return False
            money_rate = money_limit / money_mean_before
            if money_rate > 5:
                set_score = 100
            elif money_rate > 2:
                set_score = 50
            #log.info("name = %s, money_rate = %f"%(self.__class__.__name__, money_rate))
            self.__set_volume_val(set_score)
            return
        coef_money_rate = 2
        money_after = money_list[limit_index - 1]
        money_limit = money_list[limit_index]
        money_mean_after = money_list[0:limit_index + 1].mean()
        money_mean_before = money_list[limit_index + 2:(limit_index + 2 + limit_index + 1)].mean()
        #log.info('money_mean_before = %f,money_after = %f'%(money_mean_before,money_after))
        if 0 == money_mean_before or 0 == money_after:
            return False
        moeny_coef = money_limit / money_after
        money_rate = money_mean_after / money_mean_before
        #log.info("moeny_coef = %f,money_rate = %f"%(moeny_coef,money_rate))
        set_score = 0
        #放量涨停
        if money_rate > coef_money_rate:
            #缩量回调
            if moeny_coef > 1.3:
                set_score = 100
            elif moeny_coef > 0.9:
                set_score = 50
        self.__set_volume_val(set_score)
    def __is_price_gap(self, his_df, stock):
        #log.info(his_df)
        open_list = his_df['open']
        close_list = his_df['close']
        high_list = his_df['high']
        low_list = his_df['low']
        coef_money_rate = 1.17
        gap_rate = 1.02
        upper_shadow_rate = 1.03
        open_today = open_list[0]
        close_limit = close_list[0]
        low_today = low_list[0]
        high_today = high_list[0]
        high_last = high_list[1]
        close_mean_before = close_list[2:7].mean()
        #log.info('close_limit = %f,close_mean_before = %f'%(close_limit,close_mean_before))
        if close_limit > close_mean_before*coef_money_rate:    
            return False
        #log.info('open_today = %f,close_limit = %f,low_today = %f, high_last = %f'%(open_today,close_limit,low_today, high_last))
        if low_today < high_last*gap_rate:
            return False
        if low_today == high_today:    #一字板
            return False
        if high_today > close_limit*upper_shadow_rate:    #上影线
            return False
        if open_today > close_limit*1.005:
            return False
        self.__get_volume_score(his_df['money'], 0)
        return True
    def __set_volume_val(self, val):
        self.m_dragon_volume_val = val

#股票筛选类
class JudgeDragon(DataAnalyzeBase):
    def __init__(self):
        DataAnalyzeBase.__init__(self)

#获取股票打分************************************************************************************************
class GetStockScore(DataAnalyzeBase):
    def __init__(self):
        self.now_market_score = 0
        DataAnalyzeBase.__init__(self)
    #净利润小于0得零分。
    #市盈率大于70得零分。
    #增长率为负,得50分。
    #增长率为正,EPS大于1,得60分；
    #增长率为正,EPS小于1,市盈率小于20，得100分；
    #其它得80分。
    #成交量打分。
    #市值打分，小于80亿得100分，80~200得50分，其它不得分。
    #基本面、成交量、市值加权平均，权重为0.2,0.4,0.4
    def get_stock_score(self, stock):
        self.__fill_volume_score(stock)
        fundamental_score = self.__get_fundamental_and_market_score(stock)
        #log.info("%f, %f, %f"%(fundamental_score, self.m_dragon_volume_val, self.now_market_score))
        return (0.3 * fundamental_score + 0.3 * self.m_dragon_volume_val + 0.4*self.now_market_score)
    def __fill_volume_score(self, stock):
        self.m_dragon_volume_val = 0
        self.get_dragon_type(stock)
    def __get_fundamental_and_market_score(self, stock):
        q_income = query(
            income
            ).filter(
                income.code == stock
                )        
        q_valuation = query(
            valuation
            ).filter(
                valuation.code == stock
                )
        date_list = []
        last_date_list = []
        self.__get_format_date(date_list, last_date_list)
        if (0 == len(date_list)):
            return 0
        now_pe = get_fundamentals(q_valuation, statDate  = date_list[-1])['pe_ratio'][0]
        now_market_cap = get_fundamentals(q_valuation, statDate  = date_list[-1])['market_cap'][0]
        self.__get_market_score(now_market_cap)
        if now_pe > 50 or now_pe < 0:
            return 0
        now_net = 0.0
        for date_item in date_list:
            now_net += get_fundamentals(q_income, statDate  = date_item)['net_profit'][0]
            pass
        if now_net < 0:
            return 0
        last_net = 0.0
        for last_date_item in last_date_list:
            try:
                last_net += get_fundamentals(q_income, statDate  = last_date_item)['net_profit'][0]
            except:
                log.info("get_fundamentals error!")
                return 0
        net_grouth_rate = (now_net - last_net) / last_net
        ret_rate_int = 0
        #log.info("now_pe = %f, now_net = %f, last_net = %f, net_grouth_rate = %f"%(now_pe, now_net, last_net, net_grouth_rate))
        now_eps = now_pe/(net_grouth_rate * 100)
        #log.info("now_pe = %f"%(now_pe))
        if (net_grouth_rate < 0):
            ret_rate_int = 50
        else:
            if (now_eps > 1):
                ret_rate_int = 60
            else:
                if (now_pe < 20):
                    ret_rate_int = 100
                else:
                    ret_rate_int = 80
        return ret_rate_int
    def __get_format_date(self, date_list, last_date_list):
        date_str = g.m_end_date.strip().split('-')
        year_str = date_str[0]
        year_int = int(year_str)
        last_year_str = str(year_int - 1)
        previous_year_tr = str(year_int - 2)
        month_str = date_str[1]
        month_int = int(month_str)
        ret_str = ""
        if (month_int <= 4):
            date_list.append(last_year_str + "q1")
            date_list.append(last_year_str + "q2")
            date_list.append(last_year_str + "q3")
            date_list.append(last_year_str + "q4")
            last_date_list.append(previous_year_str + "q1")
            last_date_list.append(previous_year_str + "q2")
            last_date_list.append(previous_year_str + "q3")
            last_date_list.append(previous_year_str + "q4")
            
        elif month_int > 4 and month_int <= 7:
            date_list.append(year_str + "q1")
            last_date_list.append(last_year_str + "q1")
        elif month_int > 7 and month_int <= 10:
            date_list.append(year_str + "q1")
            date_list.append(year_str + "q2")
            last_date_list.append(last_year_str + "q1")
            last_date_list.append(last_year_str + "q2")
        elif month_int > 10 and month_int <= 12:
            date_list.append(year_str + "q1")
            date_list.append(year_str + "q2")
            date_list.append(year_str + "q3")
            last_date_list.append(last_year_str + "q1")
            last_date_list.append(last_year_str + "q2")
            last_date_list.append(last_year_str + "q3")
    def __get_market_score(self, market):
        ret_score = 0
        if (market < 80):
            ret_score = 100
        elif (market < 200):
            ret_score = 50
        self.now_market_score = ret_score
        
#数据模型类*********************************************************************************
class DataModel:
    def __init__(self, context, stock_list):
        self.m_posi_stock_list = stock_list
        self.last_trade_price = {}
        self.last_day_close_price = {}
        self.last_mean_money = {}
        self.dict_up_wave_rate = {}
        self.dict_down_wave_rate = {}
        self.last_up_alarm_time = {}
        self.last_down_alarm_time = {}
        self.last_money_wave_alarm_time = {}
        self.dict_breakdown_times = {}
        self.__set_val_dict(context, stock_list)
        self.__init_wave_rate(stock_list)
    def __set_val_dict(self, context, stock_list):
        for m_single_sec in stock_list:
            close_data = attribute_history(m_single_sec, 1, '1d', ['close'])
            self.last_day_close_price[m_single_sec] = (close_data['close'][-1])
            self.last_trade_price[m_single_sec] = (close_data['close'][-1])
            Volume_last_3_day=history(3,'1d','money',m_single_sec).sum()[m_single_sec]
            self.last_mean_money[m_single_sec] = (Volume_last_3_day/(3*4*60))
            self.last_up_alarm_time[m_single_sec] = context.current_dt
            self.last_down_alarm_time[m_single_sec] = context.current_dt
            self.last_money_wave_alarm_time[m_single_sec] = context.current_dt
    def __init_wave_rate(self, stock_list):
        tmp_getStockScore = GetStockScore()
        for stock in stock_list:
            stock_score = tmp_getStockScore.get_stock_score(stock)
            if stock_score >= 80:
                self.dict_up_wave_rate[stock] = 0.035
                self.dict_down_wave_rate[stock] = 0.010
            elif stock_score >= 60:
                self.dict_up_wave_rate[stock] = 0.020
                self.dict_down_wave_rate[stock] = 0.015
            else:
                self.dict_up_wave_rate[stock] = 0.015
                self.dict_down_wave_rate[stock] = 0.020
            if True == self.__is_amplitude_anomaly(stock):
                self.dict_up_wave_rate[stock] = 0.015
                self.dict_down_wave_rate[stock] = 0.03
            self.dict_breakdown_times[stock] = 0
    def __is_amplitude_anomaly(self, stock):
        close_data = attribute_history(stock, 10, '1d', ['close'])
        max_close = close_data['close'].max()
        min_close = close_data['close'].min()
        amplitude = (max_close - min_close)/min_close
        if amplitude > 0.25:
            return True
        return False
    def get_pindex_by_stock(self, stock):
        if 0 == len(self.m_posi_stock_list):
            return -1
        if stock in self.m_posi_stock_list:
            return self.m_posi_stock_list.index(stock)
        else:
            return -1
    def get_last_trade_price(self, stock):
        return self.last_trade_price[stock]
    def set_last_trade_price(self, stock, val):
        self.last_trade_price[stock] = val
    def get_last_day_close_price(self, stock):
        return self.last_day_close_price[stock]
    def set_last_day_close_price(self, stock, val):
        self.last_day_close_price[stock] = val
    def get_last_mean_money(self, stock):
        return self.last_mean_money[stock]
    def set_last_mean_money(self, stock, val):
        self.last_mean_money[stock] = val
    def get_dict_up_wave_rate(self, stock):
        return self.dict_up_wave_rate[stock]
    def set_dict_up_wave_rate(self, stock, val):
        self.dict_up_wave_rate[stock] = val
    def get_dict_down_wave_rate(self, stock):
        return self.dict_down_wave_rate[stock]
    def set_dict_down_wave_rate(self, stock, val):
        self.dict_down_wave_rate[stock] = val
    def get_last_up_alarm_time(self, stock):
        return self.last_up_alarm_time[stock]
    def set_last_up_alarm_time(self, stock, val):
        self.last_up_alarm_time[stock] = val
    def get_last_down_alarm_time(self, stock):
        return self.last_down_alarm_time[stock]
    def set_last_down_alarm_time(self, stock, val):
        self.last_down_alarm_time[stock] = val
    def get_last_money_wave_alarm_time(self, stock):
        return self.last_money_wave_alarm_time[stock]
    def set_last_money_wave_alarm_time(self, stock, val):
        self.last_money_wave_alarm_time[stock] = val
    def add_breakdown_times(self, stock):
        self.dict_breakdown_times[stock] += 1
    def is_breakdown_times_right(self, stock):
        if self.dict_breakdown_times[stock] <= 2:
            return True
        else:
            return False
        
#判断买卖点****************************************************************************************************
class JudgeTradePoint:
    def __init__(self, posiClass, dataModel, stock_list, stocksPosiInfo):
        self.b_up_hook = False
        self.b_down_hook = False
        self.b_send_buy_msg = {}
        self.b_send_sell_msg = {}
        self.b_price_up = {}
        self.b_price_down = {}
        self.b_money_wave = {}
        self.dataCtrl = dataModel
        self.positionCtrl = posiClass
        self.stocksMng = stocksPosiInfo
        self.__update_stock_dict_info(stock_list)
    def __update_stock_dict_info(self, stock_list):
        for m_single_sec in stock_list:
            self.b_send_buy_msg[m_single_sec] = False
            self.b_send_sell_msg[m_single_sec] = False
            self.b_price_up[m_single_sec] = False
            self.b_price_down[m_single_sec] = False
            self.b_money_wave[m_single_sec] = False
    def deal_price_wave(self, m_sec, data, context):
        self.__deal_price_BreakThrough(data, context, m_sec)
        self.__deal_price_BreakDown(data, context, m_sec)
        if True == self.__is_price_BreakThrough(data, context, m_sec):
            current_price = data[m_sec].close
            change_str = self.__rate_str(PrintMode.PRINT_PRICE, m_sec, current_price, self.dataCtrl.get_last_day_close_price(m_sec))
            if self.b_send_buy_msg[m_sec] == False:
                change_str += self.positionCtrl.clear_posi(m_sec, self.dataCtrl.get_pindex_by_stock(m_sec), data, context)
                self.b_send_buy_msg[m_sec] = True
            self.__send_msg_to_user(change_str)
        if True == self.__is_price_BreakDown(data, context, m_sec):
            current_price = data[m_sec].close
            change_str = self.__rate_str(PrintMode.PRINT_PRICE, m_sec, current_price, self.dataCtrl.get_last_day_close_price(m_sec))
            if False == self.b_send_buy_msg[m_sec]:
                b_buy_list = [False]
                change_str += self.positionCtrl.take_posi(m_sec, self.dataCtrl.get_pindex_by_stock(m_sec), data, context, b_buy_list)
                if True == b_buy_list[0]:
                    self.stocksMng.modify_stock_trade_state(m_sec)
                self.b_send_buy_msg[m_sec] = True
            if True == self.dataCtrl.is_breakdown_times_right(m_sec):
                self.__send_msg_to_user(change_str)
    def __deal_price_BreakThrough(self, data, context, stock):
        if True == self.b_price_up[stock]:
            return
        current_price = data[stock].close
        if current_price > self.dataCtrl.get_last_trade_price(stock)*(1 + self.dataCtrl.get_dict_up_wave_rate(stock)):
            self.b_price_up[stock] = True
            self.dataCtrl.set_last_trade_price(stock, current_price)
            self.dataCtrl.set_last_up_alarm_time(stock, context.current_dt)
    def __deal_price_BreakDown(self, data, context, stock):
        if True == self.b_price_down[stock]:
            return
        current_price = data[stock].close
        if current_price < self.dataCtrl.get_last_trade_price(stock)*(1 - self.dataCtrl.get_dict_down_wave_rate(stock)):
            self.b_price_down[stock] = True
            self.dataCtrl.set_last_trade_price(stock, current_price)
            self.dataCtrl.set_last_down_alarm_time(stock, context.current_dt)
    def __is_price_BreakThrough(self, data, context, m_sec):
        if False == self.b_price_up[m_sec]:
            return False
        time_now = context.current_dt
        if True == self.__is_continue_alarm(time_now, m_sec, self.dataCtrl.get_last_up_alarm_time(m_sec)):
            return False
        current_price = data[m_sec].close
        if current_price > self.dataCtrl.get_last_trade_price(m_sec):
            self.dataCtrl.set_last_up_alarm_time(m_sec, context.current_dt)
            self.dataCtrl.set_last_trade_price(m_sec, current_price)
            return False
        else:
            self.b_price_up[m_sec] = False
            return True
    def __is_price_BreakDown(self, data, context, m_sec):
        if False == self.b_price_down[m_sec]:
            return False
        time_now = context.current_dt
        if True == self.__is_continue_alarm(time_now, m_sec, self.dataCtrl.get_last_down_alarm_time(m_sec)):
            return False
        current_price = data[m_sec].close
        if current_price < self.dataCtrl.get_last_trade_price(m_sec):
            self.dataCtrl.set_last_down_alarm_time(m_sec, context.current_dt)
            self.dataCtrl.set_last_trade_price(m_sec, current_price)
            return False
        else:
            self.b_price_down[m_sec] = False
            self.dataCtrl.add_breakdown_times(m_sec)
            return True
    #量能比前三日放量，价格波动满足条件，触发买卖点
    def deal_money_wave(self, context, m_stock, data):
        time_now = context.current_dt
        if (9 == time_now.hour and time_now.minute < 40):
            money_rate = 10
        else:
            money_rate = 10
        wave_rate = 0.01   #提高价格波动要求
        volume_last_1_min = history(1,'1m','money',m_stock).sum()[m_stock]
        current_price = data[m_stock].close
        close_last_10 = history(10,'1m','close',m_stock).sum()[m_stock]
        close_last_5 = history(5,'1m','close',m_stock).sum()[m_stock]
        close_mean = (close_last_10 - close_last_5)/5
        if volume_last_1_min > self.dataCtrl.get_last_mean_money(m_stock) * money_rate:    #量能是前三日均量的money_rate倍
            #log.info('current_price = %f, close_5_mean = %f'%(current_price,close_mean))
            if (False == self.__is_money_head_eight(m_stock, time_now, volume_last_1_min)):
                return
            current_price = data[m_stock].close
            last_price = self.dataCtrl.get_last_day_close_price(m_stock)
            if current_price > last_price*(1 + wave_rate) or\
            current_price < last_price*(1 - wave_rate):    #波动率
                time_now = context.current_dt
                if True == self.__is_continue_alarm(time_now, m_stock, self.dataCtrl.get_last_money_wave_alarm_time(m_stock)):
                    return False
                price_str = self.__rate_str(PrintMode.PRINT_PRICE, m_stock, current_price, self.dataCtrl.get_last_day_close_price(m_stock))
                money_str = self.__rate_str(PrintMode.PRINT_MONEY, m_stock, volume_last_1_min, self.dataCtrl.get_last_mean_money(m_stock))
                change_str = money_str + price_str
                if True == self.b_send_buy_msg[m_stock] or True == self.b_send_sell_msg[m_stock] or \
                True == self.b_money_wave[m_stock]:
                    pass
                self.__send_msg_to_user(change_str)
                self.b_money_wave[m_stock] = True
                self.dataCtrl.set_last_money_wave_alarm_time(m_stock, context.current_dt)
    def __get_money_count(self, time_now):
        if (time_now.hour >= 13):
            hour_val = 60*(time_now.hour - 9) - 60*1.5
        else:
            hour_val = 60*(time_now.hour - 9)
        diff_count = hour_val + (time_now.minute - 30)
        return int(diff_count)
    def __is_money_head_eight(self, m_stock, time_now, volume_last_1_min):
        count = self.__get_money_count(time_now)
        if (0 == count):
            return True
        money_df = history(count,'1m','money',m_stock)[m_stock]
        result_series = money_df.order().tail(8)
        if (volume_last_1_min > result_series[0]):
            return True
        else:
            return False
    def __get_diff_second(self, time_now, last_time):
        if (last_time.hour <= 11 and time_now.hour >=13):
            hour_val = 3600*(time_now.hour - last_time.hour) - 3600*1.5
        else:
            hour_val = 3600*(time_now.hour - last_time.hour)
        diff_second = hour_val + 60*(time_now.minute - last_time.minute)\
        + (time_now.second - last_time.second)
        return diff_second
    def __is_continue_alarm(self, time_now, m_stock, last_time):
        diff_second = self.__get_diff_second(time_now, last_time)
        #log.info(diff_second)
        if (diff_second < 5*60):
            return True
        else:
            return False
    def __rate_str(self, ptype, m_sec, curr_val, last_val):
        tmp_getStockScore = GetStockScore()
        ret_str = ''
        if (PrintMode.PRINT_PRICE == ptype):
            float_rate = (curr_val - last_val)/last_val*100
            ret_str = m_sec +", price : "+ str(round(float_rate, 4)) + (" ,score: "+str(tmp_getStockScore.get_stock_score(m_sec)))
        elif(PrintMode.PRINT_MONEY == ptype):
            float_rate = (curr_val - last_val)/last_val
            ret_str = "money : "+ str(round(float_rate, 4)) + ", "
        return ret_str
    def __send_msg_to_user(self, send_str):
        log.info(send_str)
        send_message(send_str)

#分析股票***********************************************************************************************
class AnalyStock:
    def __init__(self, stocksManage):
        self.calc_line_k = 0
        self.m_stocksManage = stocksManage
        self.judgeDragon = JudgeDragon()
        stock_list = []    #TODO
    @calc_time
    def dragon_main(self):
        self.__judge_sse_mode()
        stocks_list = []
        self.__get_all_stock_list(stocks_list)
        #stocks_list = ['600136.XSHG']
        for stock in stocks_list:
            self.__deal_dragon_stock(stock)
        #log.info('inter = %d, total = %d'%(g.inter_stock, g.total_stock))
    def __deal_dragon_stock(self, stock):
        dragon_type = self.judgeDragon.get_dragon_type(stock)
        #log.info(dragon_type)
        if (DragonType.DRAGON_NONE == dragon_type):
            return
        self.__add_stock_to_trade_list(stock)
        self.__print_dragon_info(stock, dragon_type)
    def __judge_sse_mode(self):
        tmp_getStockScore = GetStockScore()
        m_stock = '000001.XSHG'
        his_df = tmp_getStockScore.get_stock_history_info(m_stock)
        val_list = his_df['close']
        avg_type = 10
        ret_ma = talib.MA(val_list.values, timeperiod=avg_type, matype=0)
        tmp_ma = ret_ma[avg_type:]
        mean_series = tmp_ma[0:130 - avg_type]
        df_close = sort(mean_series)
        now_mean_val = (val_list.head(10).sum() - val_list.head(5).sum())/5
        min_mean_val = (df_close[0:5].sum())/5
        max_mean_val = (df_close[-5:].sum())/5
        if (0 == min_mean_val):
            return PriceMode.PRICE_ERROR
        close_wave_rate = (max_mean_val - min_mean_val)/min_mean_val
        now_wave_rate = (now_mean_val - min_mean_val)/min_mean_val
        #print('sse_now_mean_val = %f, sse_min_mean_val = %f, sse_max_mean_val = %f'%(now_mean_val, min_mean_val, max_mean_val))
        #print('sse_close_wave_rate = %f, sse_now_wave_rate = %f'%(close_wave_rate, now_wave_rate))
        if (now_wave_rate > close_wave_rate*0.8):
            return SSEMode.SSE_HIGH
        elif (now_wave_rate < close_wave_rate*0.3):
            return SSEMode.SSE_LOW
        else:
            return SSEMode.SSE_MIDDLE
#获取股票池
    def __get_all_stock_list(self,stock_list):
        stock_df = get_fundamentals(query(
            valuation.code, valuation.market_cap, valuation.pe_ratio, valuation.pb_ratio,income.total_operating_revenue
        ).filter(
            valuation.pe_ratio < 100,
            valuation.pe_ratio > 0,
            valuation.pb_ratio < 8,
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
    def __add_stock_to_trade_list(self, stock):
        self.m_stocksManage.add_g_stock_list([stock, g.m_end_date, False])
    def __print_dragon_info(self, stock, dragon_type):
        pre_str = ''
        if (DragonType.DRAGON_LIMIT_UP == dragon_type):
            pre_str = '涨停:'
        elif (DragonType.DRAGON_VOLUME == dragon_type):
            pre_str = '放量:'    
        elif (DragonType.DRAGON_GAP == dragon_type):
            pre_str = '跳空:' 
        #log.info("line_k = %f"%(self.calc_line_k))
        tmp_getStockScore = GetStockScore()
        show_str = pre_str + stock + ' 综合评分: ' + str(tmp_getStockScore.get_stock_score(stock))
        log.info(show_str)
        #add_info_to_df(stock,his_df)
        #show_df1 = his_df.drop('close',axis = 1)
        #show_df2 = show_df1.drop('money', axis = 1)
        #log.info(show_df2.head())

def add_info_to_df(stock, in_df):
    m_fields = ['net_pct_main']
    #in_df['change_pct'] = 0
    return_df = get_money_flow([stock], end_date = in_df.index[0], fields = m_fields, count = 100).sort_index(ascending = False)
    return_df.index = in_df.index[0:len(return_df.index)]
    #log.info(return_df)
    for field_item in m_fields:
        in_df[field_item] = return_df[field_item]
    mf = pd.DataFrame(index = in_df.index[0:], columns = ['MF'])
    for date_index in in_df.index:
        price_df = get_price(stock,end_date = date_index,frequency='1m', fields=['close','volume','money'],count = 240)
        
        price_df["MF"] = price_df['money']*(price_df['close']/price_df['close'].shift(1) - 1)
        #log.info((price_df['close']/price_df['close'].shift(1) - 1))
        mf_value = price_df['MF'].sum()/10000
        mf.ix[date_index, 'MF'] = mf_value
    true_mf = mf['MF'].shift(1)
    ret_m_his_money = history(240,'1m','money',stock)
    ret_m_his_close = history(240,'1m','close',stock)
    today_mf = ret_m_his_money*(ret_m_his_close/ret_m_his_close.shift(1) - 1)
    totay_val = today_mf.sum()/10000
    in_df['MF'] = true_mf
    
def get_m_end_date(context):
    timeTool = TimeTool()
    m_today = context.current_dt
    g.m_end_date = timeTool.get_end_data(m_today)

class PosiInfoInterface:
    def __init__(self):
        pass
    def add_g_stock_list(self, stock_list):
        pass
    def return_trade_stocks(self):
        pass
    def modify_stock_trade_state(self, stock):
        pass
    def delete_trade_stock(self, context):
        pass
    def get_pindex_by_stock(self, stock):
        pass
    def print_stocks(self):
        pass
    def before_trading_posi_action(self, context):
        pass
    def sync_firm_position(self, context, data):
        pass

class StocksMngBackTest(PosiInfoInterface):
    def __init__(self):
        PosiInfoInterface.__init__(self)
        self.m_stocks_struct_list = []
        self.__init_my_cash()
    def __init_my_cash(self):
        # 最多10个仓位
        init_cash = 100000.0/3.0
        set_subportfolios([SubPortfolioConfig(cash=init_cash, type='stock'),\
        SubPortfolioConfig(cash=init_cash, type='stock'),\
        SubPortfolioConfig(cash=init_cash, type='stock'),\
        SubPortfolioConfig(cash=init_cash, type='stock'),\
        SubPortfolioConfig(cash=init_cash, type='stock'),\
        SubPortfolioConfig(cash=init_cash, type='stock'),\
        SubPortfolioConfig(cash=init_cash, type='stock'),\
        SubPortfolioConfig(cash=init_cash, type='stock'),\
        SubPortfolioConfig(cash=init_cash, type='stock'),\
        SubPortfolioConfig(cash=init_cash, type='stock')])
    def add_g_stock_list(self, stock_list):
        if True == self.__is_stock_in_trade_list(stock_list[0]):
            return
        self.m_stocks_struct_list.append(stock_list)
    def __is_date_outgoing(self, date1, date2):
        date1_list = date1.split('-')
        timeTuple1 = datetime.datetime(int(date1_list[0]), int(date1_list[1]), int(date1_list[2]))
        #date2_list = date2.split('-')
        timeTuple2 = datetime.datetime(date2.year, date2.month, date2.day)
        diff_tuple = timeTuple2 - timeTuple1
        if diff_tuple.days > 3:
            return True
        else:
            return False
    def return_trade_stocks(self):
        trade_list = [stock_list[0] for stock_list in self.m_stocks_struct_list]
        self.m_posi_stock_list = trade_list
        return trade_list
    def __is_stock_in_trade_list(self, stock):
        trade_list = self.return_trade_stocks()
        if stock in trade_list:
            return True
        else:
            return False
    def __is_posi_clear(self, p_index, context):
        if 0 == context.subportfolios[p_index].positions_value:
            return True
        else:
            return False
    def modify_stock_trade_state(self, stock):
        for index, stock_list in enumerate(self.m_stocks_struct_list):
            if stock in stock_list:
                stock_list[2] = True
                self.m_stocks_struct_list[index] = stock_list
                return
    def delete_trade_stock(self, context):
        if 0 == len(self.m_stocks_struct_list):
            return
        for pindex, stock_list in enumerate(self.m_stocks_struct_list):
            if True == stock_list[2]:
                if True == self.__is_posi_clear(pindex, context):
                    self.m_stocks_struct_list.remove(stock_list)
            else:
                if True == self.__is_date_outgoing(stock_list[1], context.current_dt):
                    self.m_stocks_struct_list.remove(stock_list)
    def before_trading_posi_action(self, context):
        self.delete_trade_stock(context)
    def get_pindex_by_stock(self, stock):
        if 0 == len(self.m_posi_stock_list):
            return -1
        if stock in self.m_posi_stock_list:
            return self.m_posi_stock_list.index(stock)
        else:
            return -1
    def print_stocks(self):
        log.info(self.m_stocks_struct_list)

#实盘交易**************************************************************************************************
class FirmBargain:
    def __init__(self, context, stock_list, stocksPosiInfo):
        self.m_security = stock_list
        self.dataClass = DataModel(context, stock_list)
        self.posiClass = PositionCtrl()
        self.judgeTradePoint = JudgeTradePoint(self.posiClass, self.dataClass, stock_list, stocksPosiInfo)
        self.__init_val(context)
    def __init_val(self, context):
        self.sse_last_day_price = attribute_history('000001.XSHG', 1, '1d', ['close'])['close'][-1]
        self.sse_last_price = self.sse_last_day_price
        # 获取股票前一天的收盘价
    def deal_stock_change(self, context, data):
        for m_sec in self.m_security:
            self.__judge_price_wave(context, m_sec, data)
            #self.__judge_money_wave(context, m_sec, data)
    def __judge_price_wave(self, context, m_sec, data):
        self.judgeTradePoint.deal_price_wave(m_sec, data, context)
    def __judge_money_wave(self, context, m_sec, data):
        self.judgeTradePoint.deal_money_wave(context, m_sec, data)
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

class StocksMngFirm(PosiInfoInterface):
    def __init__(self):
        PosiInfoInterface.__init__(self)
        self.__init_my_cash()
        self.b_need_sync_firm = True
    def __init_my_cash(self):
        # 最多10个仓位
        init_cash = 100000.0/3.0
        inventory1 = 30000
        inventory2 = 30000
        set_subportfolios([SubPortfolioConfig(cash=inventory1, type='stock'),\
        SubPortfolioConfig(cash=inventory2, type='stock'),\
        SubPortfolioConfig(cash=init_cash, type='stock'),\
        SubPortfolioConfig(cash=init_cash, type='stock'),\
        SubPortfolioConfig(cash=init_cash, type='stock'),\
        SubPortfolioConfig(cash=init_cash, type='stock'),\
        SubPortfolioConfig(cash=init_cash, type='stock'),\
        SubPortfolioConfig(cash=init_cash, type='stock'),\
        SubPortfolioConfig(cash=init_cash, type='stock'),\
        SubPortfolioConfig(cash=init_cash, type='stock')])
    def sync_firm_position(self, context, data):
        if True == self.b_need_sync_firm:
            pindex1 = 0
            remain_stock_num1 = 2200
            self.sync_stock_num(pindex1, remain_stock_num1, context, data)
            pindex2 = 1
            remain_stock_num2 = 3000
            #self.sync_stock_num(pindex2, remain_stock_num2, context, data)
    def sync_stock_num(self, p_index, remain_stock_num, context, data):
        if -1 == p_index:
            return " ,pindex error "
        remaid_cash = context.subportfolios[p_index].available_cash
        stock_list = self.return_trade_stocks()
        stock = stock_list[p_index]
        current_price = data[stock].close
        cash = remain_stock_num * current_price
        if remaid_cash < cash:
            log.info("sync firm cash error!")
            return
        #log.info(cash)
        ret_obj = order_value(stock, cash, pindex = p_index)
        if None == ret_obj:
            log.info("sync firm error!")
        self.show_posi_info(p_index, context, current_price)
        self.b_need_sync_firm = False
        return
    def show_posi_info(self, p_index, context, current_price):
        remaid_cash = context.subportfolios[p_index].available_cash
        posi_val = context.subportfolios[p_index].positions_value
        stock_amount = posi_val/current_price
        log.info("remaid_cash = %f, stock_amount = %f"%(remaid_cash, stock_amount))
    def after_trading_posi_action(self):
        pass
        #self.show_posi_info(0)
#实盘股票列表*******************************************************************************************************
    def return_trade_stocks(self):
        #return ['603167.XSHG']
        return ['601222.XSHG']

class RunContainer:
    def __init__(self, stocks_posi_info):
        self.stocksMng = stocks_posi_info
    def before_trading_action(self, context):
        get_m_end_date(context)
        self.stocksMng.before_trading_posi_action(context)
        self.sendMsgClass = FirmBargain(context, self.stocksMng.return_trade_stocks(), self.stocksMng)
    def trading_action(self, context, data):
        if RunMode.SEND_MSG_MODE == g.run_mode or RunMode.BACK_TEST_MODE == g.run_mode:
            self.sendMsgClass.deal_sse_change(data)
            self.sendMsgClass.deal_stock_change(context, data)
            self.stocksMng.sync_firm_position(context, data)
    def after_trading_action(self, context):
        if RunMode.SELECT_STOCK_MODE == g.run_mode or RunMode.BACK_TEST_MODE == g.run_mode:
            analyStock = AnalyStock(self.stocksMng)
            analyStock.dragon_main()
        self.stocksMng.print_stocks()

#g.run_mode = RunMode.SELECT_STOCK_MODE
g.run_mode = RunMode.SEND_MSG_MODE
#g.run_mode = RunMode.BACK_TEST_MODE
                
def initialize(context):
    g.stocksMngBackTet = StocksMngBackTest()
    g.stocksMngFirm = StocksMngFirm()
    if RunMode.SELECT_STOCK_MODE == g.run_mode or RunMode.BACK_TEST_MODE == g.run_mode:
        g.RunContainer = RunContainer(g.stocksMngBackTet)
    elif RunMode.SEND_MSG_MODE == g.run_mode:
        g.RunContainer = RunContainer(g.stocksMngFirm)
    
def before_trading_start(context):
    g.RunContainer.before_trading_action(context)
def after_trading_end(context):
    g.RunContainer.after_trading_action(context)
def handle_data(context, data):
    g.RunContainer.trading_action(context, data)
    
        

    

