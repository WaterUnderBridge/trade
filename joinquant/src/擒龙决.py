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
import statsmodels.api as sm
import math
from jqdata import jy

class MyPara:
    def __init__(self):
        self.total_day = 200
        ##
        self.test_stock_list = []
        ##
        self.show_chip = False    #筹码信息显示
        ##
        ##价格模式
        self.price_mode_two_week_high_rate = 0.2    #半月涨太高
        self.price_mode_inner_shock_rate = 0.40      #区间震荡
        self.price_mode_high_del_rate = 0.65      #过滤倒·V
        self.price_v_low_rate = 0.25      #区间震荡
        self.INTER_SECTION_DAYS = 45
        #
        self.up_chip_high_rate = 20      #筹码发完阈值，单位%
        self.k_day = 100    #计算斜率的天数
        self.K_MIN_RATE = -0.015
        self.K_MAX_RATE = 0.05
        self.price_mean_RATE = 0.002
        self.STD_RATE = 0.030
        self.COFF_VARIANT = 0.092
my_para = MyPara()
def float_div(a,b):
    return float(a)/float(b)

def float_is_equal(a,b):
    if math.fabs(a - b) < 0.05:
        return True
    else:
        return False

class ChipAnalysis:
    def __init__(self,in_stock):
        self.m_stock = in_stock
        self.up_chip = 0
    def _get_df_current(self,m_stock):
        return DataFrame()
    def _get_df_days_before(self,m_stock,before_days):
        return DataFrame()
    def _create_init_ratio_se(self, data_df):
        start_price = data_df['low'].min()*0.9
        end_price = data_df['high'].max()*1.1
        section_div = 30
        price_div = section_div + 1    #点的个数
        section_num = section_div + 1    #增加一个区间包含最大价格(该区间也占用筹码)
        price_num = price_div + 1;    #32
        inter_len = float_div(end_price - start_price, section_num)    #区间长度
        inter_len = round(inter_len,4)
        price_ratio_dict = {}
        for i in range(price_num):
            if i == price_num - 1:
                price_ratio_dict[start_price + inter_len*i] = 0    #终点用于计算
            else:
                price_ratio_dict[start_price + inter_len*i] = float_div(1, section_num)  #每个区间起点用于存储数值
        price_ratio_se = Series(price_ratio_dict)    #价格区间以起始值作为代表
        m_sum = price_ratio_se.sum()
        if False == float_is_equal(1,m_sum):
            print("stock = %s, init_sum = %f"%(self.m_stock, m_sum))
        assert(float_is_equal(1,m_sum))
        return price_ratio_se
    def _update_ratio_se_everyday(self, data_df, price_ratio_se):
        close_se = data_df['close']
        start_price = data_df['low'].min()
        end_price = data_df['high'].max()
        volume_index = data_df.index
        ratio_index = price_ratio_se.index
        for one_day in volume_index:    #计算每日筹码转移
            high_price = data_df.loc[one_day,'high']
            low_price = data_df.loc[one_day,'low']
            one_ratio = data_df.loc[one_day,'turn_over']
            if one_ratio >= 1:
                print('one_ratio error')
            if high_price > end_price or low_price < start_price:
                print("price scope error!high_price = %.4f, low_price = %.4f, end_price = %.4f, start_price = %.4f"%(high_price, low_price, end_price, start_price))
            scope_dict = {}
            for i,single_price in enumerate(ratio_index):    #计算每日筹码转入占比
                if i + 1 < len(ratio_index):
                    down_price = ratio_index[i]
                    up_price = ratio_index[i + 1]
                    calc_down = max(down_price, low_price)
                    calc_up = min(up_price, high_price)
                    scope_len = max(0, calc_up - calc_down)
                    diff_high_low = high_price - low_price
                    make_up = 0
                    if 0 != diff_high_low:
                        make_up = float_div(scope_len, high_price - low_price)
                    else:
                        if down_price <= low_price and up_price >= high_price:
                            make_up = 1
                        else:
                            make_up = 0
                    scope_dict[single_price] = make_up
            scope_dict[ratio_index[-1]] = 0
            scope_se = Series(scope_dict)
            if False == float_is_equal(scope_se.sum(), 1):
                print(scope_se)
                print("high_price = %f, low_price = %f"%(high_price, low_price))
                print(ratio_index)
                print("stock = %s, scope_se error!, sum = %f"%(self.m_stock, scope_se.sum()))
            scope_ratio = scope_se * one_ratio
            price_ratio_se = price_ratio_se*(1 - one_ratio)
            price_ratio_se = price_ratio_se + scope_ratio
        if False == float_is_equal(price_ratio_se.sum(), 1):    #一致性约束
            print("stock = %s,price_ratio_se error!,calc_after_sum = %f"%(self.m_stock, price_ratio_se.sum()))
        return price_ratio_se
    def get_up_chip(self):
        return self.up_chip
    def _calc_current_ratio(self, now_price, price_ratio_se):
        ratio_index = price_ratio_se.index
        now_index = 0
        for i,single_price in enumerate(ratio_index):
            if i + 1 < len(ratio_index):
                down_price = ratio_index[i]
                up_price = ratio_index[i + 1]
                if down_price <= now_price and up_price >= now_price:
                    now_index = i
                    break
        now_ratio = price_ratio_se.iloc[i]
        before_ratio = price_ratio_se.iloc[:i].sum()
        after_ratio = price_ratio_se.iloc[i+1:].sum()
        self.up_chip = after_ratio*100
        if True == my_para.show_chip:
            print("筹码分布:当前价格区间%.2f--%.2f, 当前筹码%.3f%%, 下部筹码%.3f%%, 上部筹码%.3f%%"%(ratio_index[i], ratio_index[i+1], now_ratio*100, before_ratio*100, after_ratio*100))
        if False == float_is_equal(1,now_ratio + before_ratio + after_ratio):
            print("stock = %s, now_ratio = %f,before_ratio = %f,after_ratio = %f"%(self.m_stock, now_ratio, before_ratio, after_ratio))
        assert(float_is_equal(1,now_ratio + before_ratio + after_ratio))  #一致性检查
    def _chip_focus(self, price_ratio_se):
        low_price_list = []
        upper_price_list = []
        sum_ratio_list = []
        target_ratio = 0.85
        max_ratio = 0
        sec_len = 0
        ratio_index = price_ratio_se.index
        while max_ratio < target_ratio:
            sec_len += 1
            for i in range(len(ratio_index)):
                if i + sec_len < len(ratio_index):
                    low_price_list.append(ratio_index[i])
                    upper_price_list.append(ratio_index[i+sec_len])
                    sum_ratio_list.append(price_ratio_se.iloc[i:i+sec_len].sum())
            sec_df = DataFrame({'low':low_price_list,'upper':upper_price_list,'radio_sum':sum_ratio_list})
            max_line = sec_df['radio_sum'].idxmax()
            max_ratio = sec_df.ix[max_line]['radio_sum']
            if max_ratio >= target_ratio:
                if True == my_para.show_chip:
                    print("筹码集中度:集中度%.2f%%, 区间%.2f~%.2f"%(max_ratio*100, sec_df.ix[max_line]['low'],sec_df.ix[max_line]['upper']))
                return sec_df.ix[max_line]['low']
    '''
    def _chip_distribution(self, in_stock):
        data_df = self._get_df_current(in_stock)
        low_price_now = self._calc_chip(data_df)
        before_days = 30
        data_before = self._get_df_days_before(in_stock, before_days)
        low_price_before = self._calc_chip(data_before)
        print("low_price_now = %.3f, low_price_before = %.3f,low_percent = %.3f%%"%(low_price_now, low_price_before, (low_price_before - low_price_now)/low_price_before*100))
    '''
    def calc_chip(self, in_df):
        price_ratio_se = self._create_init_ratio_se(in_df)
        price_ratio_se = self._update_ratio_se_everyday(in_df, price_ratio_se)
        now_price = in_df['close'][-1]
        self._calc_current_ratio(now_price, price_ratio_se)
        low_price = self._chip_focus(price_ratio_se)
        #print(price_ratio_se)
        return low_price

########################################################
class PrintMode(Enum):
    PRINT_PRICE = 1
    PRINT_MONEY = 2

class PriceMode(Enum):
    INTER_SHOCK = 1   #震荡区间40%
    PRICE_V = 2     #目前价格比最低价高15%以内
    PRICE_HIGH = 3    #两月上涨35%
    PRICE_DOWN = 4
    PRICE_ERROR = 5
    PRICE_LESS_HIGH = 6
    PRICE_TWO_WEEK_HIGH = 7    #两月上涨35%
    
class SSEMode(Enum):
    SSE_LOW = 1
    SSE_HIGH = 2
    SSE_MIDDLE = 3
    
class DragonType(Enum):
    DRAGON_NONE = 0
    DRAGON_LIMIT_UP = 1
    DRAGON_GAP = 2

#g.b_calc_today = False
#g.b_calc_today = True

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
    '''
    入参:时间结构体
    出参:格式化时间字符串
    '''
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
        mean_list.reverse()    #?为何输入后转置
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
    def get_dragon_type(self, stock, fund_df):
        his_df = self.get_single_stock_history_info(stock)
        market_cap = fund_df.loc[stock,'market_cap']*100000000
        his_df['turn_over'] = his_df['money']/market_cap
        if True == his_df.empty:
            return DragonType.DRAGON_NONE
        if True == self.__is_new_stock(his_df['close'][-1]):
            return DragonType.DRAGON_NONE
        dragon_type = self.__is_dragon_stock(his_df,stock)
        if dragon_type != DragonType.DRAGON_NONE:
            price_mode = self.__price_pattern_match(his_df)
            #log.info(price_mode)
            if (True == self.__is_need_fliter_by_pattern(price_mode)):
                return DragonType.DRAGON_NONE
            if PriceMode.PRICE_V == price_mode:
                variant_coff = self._get_variant_coff(his_df[self.up_index:-1]['close'])
                #log.info(variant_coff)
                if variant_coff > my_para.COFF_VARIANT:
                    return DragonType.DRAGON_NONE
            chip_df = his_df[self.up_index:-1].sort_index(ascending = True)
            chip_tool = ChipAnalysis(stock)
            chip_tool.calc_chip(chip_df)
            up_chip = chip_tool.get_up_chip()
            (fit_k, mean, std) = self._fit_price_rate(chip_df['close'][-my_para.k_day:-1 - 5])
            if PriceMode.PRICE_LESS_HIGH == price_mode or PriceMode.PRICE_TWO_WEEK_HIGH:    #高价只选突破
                if fit_k < my_para.K_MIN_RATE:
                    return DragonType.DRAGON_NONE
            if up_chip < my_para.up_chip_high_rate:
                if fit_k < my_para.K_MIN_RATE or fit_k > my_para.K_MAX_RATE or std > my_para.STD_RATE or mean > my_para.price_mean_RATE:
                    return DragonType.DRAGON_NONE
            print("stock = %s,up_chip = %f, k = %.4f, mean = %.4f, std = %f"%(stock, up_chip, fit_k, mean, std))
        return dragon_type
    def _get_variant_coff(self,close_se):
        mean = close_se.mean()
        std = close_se.std()
        return std/mean
        
    def _get_k_of_se(self, in_se):
        #log.info(in_se)
        calc_len = len(in_se.index)
        x_data = range(calc_len)
        x_array = np.array(x_data)
        x_sm = sm.add_constant(x_array)
        y_sm = np.array(in_se.values)
        model = sm.OLS(y_sm, x_sm)
        results = model.fit()
        #print(results.params[1])
        return results.params[1]
    def _fit_price_rate(self, in_se):
        shift_pd = in_se.shift()
        ratio_se = (in_se - shift_pd)/shift_pd
        ratio_se = ratio_se.dropna()
        k = self._get_k_of_se(ratio_se)*100
        #print(ratio_se)
        mean = ratio_se.mean()
        std = ratio_se.std()
        return (k,mean, std)
    def _fit_price(self, in_se):
        sort_se = Series(sort(in_se))
        print(sort_se)
        k = self._get_k_of_se(sort_se)*100
        std = sort_se.std()
        return (k,std)
        
    def _in_get_price_stake(self, m_stock):
        m_count = my_para.total_day
        tmp_end_date = g.m_end_date
        m_frequency ='daily'
        m_fields = ['close','money','high','low','open','volume']
        return get_price(m_stock, count = m_count, end_date = tmp_end_date, frequency = m_frequency, fields = m_fields)
    #获取历史数据
    def get_single_stock_history_info(self, m_stock):
        if True == self.__is_need_filter(m_stock):
            return DataFrame()
        '''
        if True == g.b_calc_today:
            tmp_end_date = time.strftime("%Y-%m-%d", time.localtime())
        '''
        last_info_tmp = self._in_get_price_stake(m_stock)
        his_df = last_info_tmp.sort_index(ascending = False)    #按时间降序
        '''
        if True == self.__is_new_stock(his_df['close'][-1]):
            return DataFrame()
        '''
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
        '''
        code_list = list(item)
        if self.__is_carve_out_board(code_list[0]):
            return True
        '''
        return False
    #获取股票模式
    def __price_pattern_match(self, his_df):
        val_list = his_df['close'][self.up_index+1:-1]
        high_list = his_df['high'][self.up_index+1:-1]
        low_list = his_df['low'][my_para.INTER_SECTION_DAYS:-1]
        val_list = val_list.dropna()
        high_list = high_list.dropna()
        low_list = low_list.dropna()
        fittK = FittK()
        mode_ret = PriceMode.PRICE_ERROR
        two_week_mode_ret = PriceMode.PRICE_ERROR
        two_week_min_val = low_list[0:15].min()
        two_week_max_val = high_list[0:15].max()
        two_month_up_rate = (two_week_max_val - two_week_min_val)/two_week_min_val
        #log.info("two_month_up_rate = %f, two_week_max_val = %f, two_week_min_val = %f"%(two_month_up_rate, two_week_max_val, two_week_min_val))
        if (two_month_up_rate > my_para.price_mode_two_week_high_rate):    #半个月上涨20%
            two_week_mode_ret = PriceMode.PRICE_TWO_WEEK_HIGH
        high_se = sort(high_list)
        low_se = sort(low_list)    #前一个月算低点
        now_mean_val = val_list[0:my_para.INTER_SECTION_DAYS].min()
        min_mean_val = low_se[0]
        max_mean_val = high_se[-1]
        if (0 == min_mean_val):
            mode_ret = PriceMode.PRICE_ERROR
            return mode_ret
        close_wave_rate = (max_mean_val - min_mean_val)/min_mean_val
        now_wave_rate = (now_mean_val - min_mean_val)/min_mean_val
        #log.info('now_mean_val = %f, min_mean_val = %f, max_mean_val = %f'%(now_mean_val, min_mean_val, max_mean_val))
        #log.info('close_wave_rate = %f, now_wave_rate = %f'%(close_wave_rate, now_wave_rate))
        if close_wave_rate > my_para.price_mode_high_del_rate:
            mode_ret = PriceMode.PRICE_HIGH
        elif (close_wave_rate < my_para.price_mode_inner_shock_rate):    #区间震荡
            mode_ret = PriceMode.INTER_SHOCK
        elif now_wave_rate < my_para.price_v_low_rate:
            mode_ret = PriceMode.PRICE_V
        else:
            mode_ret = PriceMode.PRICE_LESS_HIGH
        if  PriceMode.PRICE_TWO_WEEK_HIGH == two_week_mode_ret:
            if PriceMode.PRICE_HIGH == mode_ret or PriceMode.PRICE_V == mode_ret:
                return mode_ret
            else:
                return PriceMode.PRICE_TWO_WEEK_HIGH
        else:
            return mode_ret
    def __is_need_fliter_by_pattern(self, price_pattern):
        if (price_pattern == PriceMode.PRICE_HIGH or\
        PriceMode.PRICE_ERROR == price_pattern):
            return True
        else:
            return False
    #股票筛选
    def __is_dragon_stock(self, his_df, stock):
        if (True == self.__is_limit_up(his_df,stock)):
            return DragonType.DRAGON_LIMIT_UP
            #return DragonType.DRAGON_NONE
        '''
        if (True == self.__is_price_gap(his_df, stock)):
            return  DragonType.DRAGON_GAP
            #return DragonType.DRAGON_NONE
        '''
        return DragonType.DRAGON_NONE
    def __is_limit_up(self, his_df,stock):
        b_dragon = False
        val_list = his_df['close']
        money_list = his_df['money']
        #g.total_stock += 1
        for index in range(1,4):
            if val_list[index] > val_list[index + 1] * 1.098:    #涨停
                if True == self.__dragon_price_check(val_list, index):
                    if True == self.__dragon_money_check(money_list, index):
                        b_dragon =  True
                        self.up_index = index
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
        self.up_index = 0
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
        self.d_stock_dict = {}    #TODO
    #@calc_time
    def dragon_main(self):
        self.__judge_sse_mode()
        self.fund_df = self.__get_all_stock_list()
        stocks_list = list(self.fund_df.index)
        if len(my_para.test_stock_list):
            stocks_list = my_para.test_stock_list
        for stock in stocks_list:
            if stock in g.msg_stock_dict.keys():
                continue
            self.__deal_dragon_stock(stock)
        #log.info('inter = %d, total = %d'%(g.inter_stock, g.total_stock))
    def __deal_dragon_stock(self, stock):
        dragon_type = self.judgeDragon.get_dragon_type(stock,self.fund_df)
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
        his_df = dataAnalyze.get_single_stock_history_info(m_stock)
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
    def __get_all_stock_list(self):
        stock_df = get_fundamentals(query(
            valuation.code, valuation.market_cap, valuation.pe_ratio, valuation.pb_ratio
        ).filter(
            valuation.pe_ratio < 150,
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
        stock_df = stock_df.set_index('code')
        return stock_df
    def __print_dragon_info(self, stock, dragon_type):
        self.d_stock_dict[stock] = dragon_type
    def get_d_stocks_dict(self):
        return self.d_stock_dict
    
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
        show_dict = analyStock.get_d_stocks_dict()
        if 0 != len(show_dict):
            self.do_print(show_dict)
        
    def _format_show_dict(self, init_dict):
        show_dict = init_dict
        del_list = []
        for key in show_dict.keys():
            if key in g.msg_stock_dict.keys():
                del_list.append(key)
            else:
                g.msg_stock_dict[key] = 0
        for item in del_list:
             show_dict.pop(item)
        return show_dict
    def _format_send_str(self, show_dict):
        limit_up_list = []
        gap_list = []
        for key in show_dict:
            if DragonType.DRAGON_LIMIT_UP == show_dict[key]:
                limit_up_list.append(key)
            elif DragonType.DRAGON_GAP == show_dict[key]:
                gap_list.append(key)
        send_str = ''
        if 0 != len(limit_up_list):
            send_str = send_str + '涨停: '
            for item in limit_up_list:
                send_str = send_str + item + ' ,'
            send_str = send_str + '\n'
        if 0 != len(gap_list):
            send_str = send_str + '跳空: '
            for item in gap_list:
                send_str = send_str + item + ' ,'
            send_str = send_str + '\n'
        return send_str
    def do_print(self, init_dict):
        show_dict = self._format_show_dict(init_dict)
        if 0 != len(show_dict):
            send_str = self._format_send_str(show_dict)
            log.info(send_str)
            send_message(send_str)
    def after_trading_action(self, context):
        pass
def  _init_msg_stock_dict():
    pop_list = []
    for key in g.msg_stock_dict:
        if g.msg_stock_dict[key] > 15:
            pop_list.append(key)
        else:
            g.msg_stock_dict[key] = g.msg_stock_dict[key] + 1
    pop_tu = tuple(pop_list)
    pop_list = list(pop_tu)
    for item in pop_list:
        g.msg_stock_dict.pop(item)
        
                
def initialize(context):
    g.RunContainer = RunContainer()
    g.msg_stock_dict = {}
    
def before_trading_start(context):
    _init_msg_stock_dict()
    g.RunContainer.before_trading_action(context)
def after_trading_end(context):
    g.RunContainer.after_trading_action(context)
def handle_data(context, data):
    pass
    
    
        

    

