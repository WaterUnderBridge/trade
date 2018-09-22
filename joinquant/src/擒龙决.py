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
    DRAGON_GAP = 2

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
#通过数据分析获取短线强势票************************************************************************************
class DataAnalyzeBase:
    def __init__(self):
        pass
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
            #return True
            return False
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
        '''
        self.calc_line_k = fittK.get_ma10_k(mean_series)
        if (self.calc_line_k < 0):
            mode_ret = PriceMode.PRICE_DOWN
        '''
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
        return True


#股票筛选类
class JudgeDragon(DataAnalyzeBase):
    def __init__(self):
        DataAnalyzeBase.__init__(self)

#获取股票打分************************************************************************************************
class GetStockScore(DataAnalyzeBase):
    def __init__(self):
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
        '''
        try:
            self.__get_format_date(date_list, last_date_list)
        except:
            log.info("__get_format_date error!")
            return 0
        '''
        if (0 == len(date_list)):
            return 0
        now_pe = get_fundamentals(q_valuation, statDate  = date_list[-1])['pe_ratio'][0]
        now_market_cap = get_fundamentals(q_valuation, statDate  = date_list[-1])['market_cap'][0]
        if now_pe > 50 or now_pe < 0:
            return 0
        now_net = 0.0
        for date_item in date_list:
            try:
                now_net += get_fundamentals(q_income, statDate  = date_item)['net_profit'][0]
            except:
                log.info("get net_profit error!")
                return 0
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
        year_str = '2017'
        year_int = int(year_str)
        last_year_str = str(year_int - 1)
        previous_year_tr = str(year_int - 2)
        month_str = date_str[1]
        month_int = int(month_str)
        month_int = 12
        ret_str = ""
        if (month_int <= 4):
            log.info(previous_year_tr, month_int)
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
        
#分析股票***********************************************************************************************
class AnalyStock:
    def __init__(self):
        self.calc_line_k = 0
        self.judgeDragon = JudgeDragon()
        stock_list = []    #TODO
    #@calc_time
    def dragon_main(self):
        self.__judge_sse_mode()
        stocks_list = []
        self.__get_all_stock_list(stocks_list)
        #stocks_list = ['002382.XSHE']
        for stock in stocks_list:
            self.__deal_dragon_stock(stock)
        #log.info('inter = %d, total = %d'%(g.inter_stock, g.total_stock))
    def __deal_dragon_stock(self, stock):
        dragon_type = self.judgeDragon.get_dragon_type(stock)
        #log.info(dragon_type)
        if (DragonType.DRAGON_NONE == dragon_type):
            return
        self.__print_dragon_info(stock, dragon_type)
    def __judge_sse_mode(self):
        '''计算大盘10日均线,根据均线计算大盘波动幅度（最大值-最小值）/最小值，计算当前值的波动幅度（当前值-最小值）/最小值 
        如果当前值的波动幅度 > 大盘波动幅度 * 0.8,则大盘模式为高；如果当前值的波动幅度 < 大盘波动幅度 * 0.3,则大盘模式为低；
        其它情况大盘模式为中。
        '''
        dataAnalyze = DataAnalyzeBase()
        m_stock = '000001.XSHG'
        his_df = dataAnalyze.get_stock_history_info(m_stock)
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
        max_wave_rate = (max_mean_val - min_mean_val)/min_mean_val
        now_wave_rate = (now_mean_val - min_mean_val)/min_mean_val
        #print('sse_now_mean_val = %f, sse_min_mean_val = %f, sse_max_mean_val = %f'%(now_mean_val, min_mean_val, max_mean_val))
        #print('sse_close_wave_rate = %f, sse_now_wave_rate = %f'%(close_wave_rate, now_wave_rate))
        if (now_wave_rate > max_wave_rate*0.8):
            return SSEMode.SSE_HIGH
        elif (now_wave_rate < max_wave_rate*0.3):
            return SSEMode.SSE_LOW
        else:
            return SSEMode.SSE_MIDDLE
#获取股票池
    def __get_all_stock_list(self,stock_list):
        stock_df = get_fundamentals(query(
            valuation.code, valuation.market_cap, valuation.pe_ratio, valuation.pb_ratio,income.total_operating_revenue
        ).filter(
            valuation.pe_ratio < 50,
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
    def __print_dragon_info(self, stock, dragon_type):
        pre_str = ''
        if (DragonType.DRAGON_LIMIT_UP == dragon_type):
            pre_str = '涨停:'
        elif (DragonType.DRAGON_GAP == dragon_type):
            pre_str = '跳空:' 
        #log.info("line_k = %f"%(self.calc_line_k))
        show_str = pre_str + stock
        g.str_send += show_str
        g.str_send += " ; "
        g.b_has_dragon = True
    
def get_m_end_date(context):
    timeTool = TimeTool()
    m_today = context.current_dt
    g.m_end_date = timeTool.get_end_data(m_today)


class RunContainer:
    def __init__(self):
        pass
    def before_trading_action(self, context):
        get_m_end_date(context)
        analyStock = AnalyStock()
        analyStock.dragon_main()
    def trading_action(self, context, data):
        if False == g.b_send_flag and True == g.b_has_dragon:
            log.info(g.str_send)
            send_message(g.str_send)
            g.b_send_flag = True
    def after_trading_action(self, context):
        pass
        
                
def initialize(context):
    g.RunContainer = RunContainer()
    g.str_send = ""
    g.b_send_flag = False
    g.b_has_dragon = False
    
def before_trading_start(context):
    g.RunContainer.before_trading_action(context)
def after_trading_end(context):
    g.RunContainer.after_trading_action(context)
def handle_data(context, data):
    g.RunContainer.trading_action(context, data)
    
        

    

