#encoding=utf8
import numpy as np
import pandas as pd
from pandas import Series,DataFrame
import matplotlib.pyplot as plt
import statsmodels.api as sm

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
	df = pd.read_csv('szstudy.csv')
	money_se = get_ma5(df['money'])
	judge_money_increase(money_se,df['date'])

#高开或低开后，收盘同向次数
def close_open_test():
	df = pd.read_csv('szstudy.csv')
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
	df = pd.read_csv('szstudy.csv')
	close_se = df['close']
	k_list = []
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
			plt.plot(x_array, y_sm,label = 'data')
			plt.plot(x_array, y_fitted, 'b--', label = 'ols')
	#plt.legend(loc = 'best')
	plt.show()
	x_list = list(range(len(k_list)))
	plt.plot(x_list, k_list, 'go')
	plt.show()


if __name__ == '__main__':
	print("hello world!")
	#test_money()
	#close_open_test()
	close_fit()