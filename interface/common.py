#encoding=utf8
import numpy as np
import pandas as pd
from pandas import Series,DataFrame
import matplotlib.pyplot as plt
import statsmodels.api as sm
import math
import tushare as ts

global g_sz_file
g_sz_file = '../../git_file/szdata/szstudy.csv'

global g_lxct_file
g_lxct_file = '../../git_file/lxct/lxct.csv'

def add(a,b):
	return (a+b)


def get_ma5(in_se):
    calc_len = 5
    new_list = []
    for index in range(len(in_se)):
        if index < len(in_se) - calc_len:
            new_list.append(in_se[index:index + calc_len].mean()) 
    return Series(new_list)
    
def get_max_money_mean(in_se):
	new_se = in_se.sort_values(ascending = False)
	return new_se[0:3].mean()

def get_min_money_mean(in_se):
	new_se = in_se.sort_values(ascending = True)
	return new_se[0:3].mean()

def judge_money_increase(money_se, date_list):
    calc_len = 10
    for m_index in range(0, len(money_se), calc_len):
    	if m_index >= calc_len:
    		max_money = get_max_money_mean(money_se[m_index:m_index+calc_len])
    		min_money = get_min_money_mean(money_se[m_index - calc_len:m_index])
    		money_gain = max_money/min_money
    		if money_gain > 2.5:
    			print(money_gain, m_index, date_list[m_index])
     
#量能放大
def test_money():
	df = pd.read_csv(g_sz_file)
	money_se = get_ma5(df['money'])
	judge_money_increase(money_se,df['date'])

#高开或低开后，收盘同向次数
def close_open_test():
	df = pd.read_csv(g_sz_file)
	df_shift = df.shift()
	open_se = df['open']
	close_se = df['close']
	close_shift = df_shift['close']
	close_gain = (close_se - close_shift)/close_shift
	open_gain = (open_se - close_shift)/close_shift
	gain_df = DataFrame({'close_gain':close_gain,'open_gain':open_gain})
	gain_df.index = df['date'].values
	query_df = gain_df.query('(close_gain >= 0 and open_gain >= 0) or (close_gain <= 0 and open_gain <= 0)')
	print(query_df)

#收盘价拟合
def close_fit():
	calc_len = 50
	df = pd.read_csv(g_sz_file)
	close_se = df['close']
	k_list = []
	plt1 = plt.subplot(2,1,1)
	plt2 = plt.subplot(2,1,2)
	for index in range(0, len(close_se), calc_len):
		if index < len(close_se) - calc_len:
			x_data = range(index, index + calc_len, 1)
			x_array = np.array(x_data)
			x_sm = sm.add_constant(x_array)
			y_sm = np.array(close_se[index:index + calc_len].values)
			model = sm.OLS(y_sm, x_sm)
			results = model.fit()
			k_list.append(results.params[1])
			y_fitted = results.fittedvalues
			plt1.plot(x_array, y_sm,label = 'data')
			plt1.plot(x_array, y_fitted, 'b--', label = 'ols')
	x_list = list(range(len(k_list)))
	plt2.plot(x_list, k_list, 'go')
	plt2.set_xlabel('num')
	plt2.set_ylabel('values')
	plt2.set_title('k_fit')
	plt2.legend(loc = 'best')
	plt.show()

def float_div(a,b):
	return float(a)/float(b)

def float_is_equal(a,b):
	if math.fabs(a - b) < 0.000001:
		return True
	else:
		return False

class StockAnalysis:
	def __init__(self, m_stock):
		self.now_stock = m_stock
		self._get_market_value(m_stock)
		self._get_df()
	def _get_market_value(self,m_stock):
		ret = ts.get_stock_basics()
		self.mk_val = ret['outstanding'][m_stock]*100000000/100#(手)
	def _get_df(self):
		ret_pd = ts.get_hist_data(self.now_stock,start = '2018-06-16')
		new_pd = ret_pd.sort_index(ascending = True)
		self.day_pd = new_pd
	def _chip_distribution(self):
		df = self.day_pd
		df['volume'] = df['volume']/self.mk_val
		close_se = df['close']
		start_price = close_se.min()*0.75
		end_price = close_se.max()*1.5
		price_num = 30    #点的个数
		section_num = price_num - 1    #price_num - 1为区间的个数
		inter_len = float_div(end_price - start_price, section_num)    #区间长度
		inter_len = round(inter_len,2)
		price_ratio_dict = {}
		for i in range(price_num):
			if i == price_num - 1:
				price_ratio_dict[start_price + inter_len*i] = 0    #终点用于计算
			else:
				price_ratio_dict[start_price + inter_len*i] = float_div(1, section_num)
		price_ratio_se = Series(price_ratio_dict)    #价格区间以起始值作为代表
		m_sum = price_ratio_se.sum()
		#print(price_ratio_se)
		volume_index = df.index
		ratio_index = price_ratio_se.index
		for one_day in volume_index:    #计算每日筹码转移
			high_price = df.loc[one_day,'high']
			low_price = df.loc[one_day,'low']
			one_ratio = df.loc[one_day,'volume']
			if one_ratio >= 1:
				print('one_ratio error')
			if high_price > end_price or low_price < start_price:
				print("price scope error!")
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
				print("scope_se error!")
			scope_ratio = scope_se * one_ratio
			price_ratio_se = price_ratio_se*(1 - one_ratio)
			price_ratio_se = price_ratio_se + scope_ratio
		if False == float_is_equal(price_ratio_se.sum(), 1):    #一致性约束
				print("price_ratio_se error!")
		now_price = close_se[-1]
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
		print("筹码分布:当前价格区间%.2f--%.2f, 当前筹码%.3f%%, 下部筹码%.3f%%, 上部筹码%.3f%%"%(ratio_index[i], ratio_index[i+1], now_ratio*100, before_ratio*100, after_ratio*100))
		assert(float_is_equal(1,now_ratio + before_ratio + after_ratio))  #一致性检查
		low_price_list = []
		upper_price_list = []
		sum_ratio_list = []
		target_ratio = 0.85
		max_ratio = 0
		sec_len = 0
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
				print("筹码集中度:集中度%.2f%%, 区间%.2f~%.2f"%(max_ratio*100, sec_df.ix[max_line]['low'],sec_df.ix[max_line]['upper']))
		price_ratio_se.plot('bar')
		plt.show()
		#print(price_ratio_se)

def stock_analysis_test():
	analysis_tool = StockAnalysis('600518')
	analysis_tool._chip_distribution()

def ts_test():
	ret_pd = ts.get_hist_data('600518',start = '2018-06-16')
	new_pd = ret_pd.sort_index(ascending = True)
	ret = ts.get_stock_basics()
	print(ret['outstanding']['600518'])
	#print(ret)

if __name__ == '__main__':
	print("hello world!")
	#test_money()
	#close_open_test()
	#close_fit()
	stock_analysis_test()
	#ts_test()
	