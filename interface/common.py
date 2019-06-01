#encoding=utf8
import numpy as np
import pandas as pd
from pandas import Series,DataFrame
import matplotlib.pyplot as plt

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
     

def test_func():
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



if __name__ == '__main__':
	print("hello world!")
	#test_func()
	close_open_test()