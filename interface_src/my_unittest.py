#encoding=utf8
import numpy as np
import pandas as pd
from pandas import Series,DataFrame
import matplotlib.pyplot as plt
import math
from common import *

def is_equal(a,b):
	if math.fabs(a - b) < 0.000001:
		return True
	else:
		return False

def test_equal(a,b):
	if a != b:
		print("equal error!")

def test_not_equal(a,b):
	if a == b:
		print("not equal error!")

def se_sort_test():
	se_test = Series([3,8,4,2,6])
	sort_se = se_test.sort_values(ascending = True)
	assert(2 == sort_se.iloc[0])
	assert(8 == sort_se.iloc[4])
	assert(3 == sort_se[0:3].mean())
	assert(3 == se_test.iloc[0])
	assert(6 == se_test.iloc[4])
	assert(5 == se_test[0:3].mean())
	sort_se2 = se_test.sort_values(ascending = False)
	assert(8 == sort_se2.iloc[0])
	assert(2 == sort_se2.iloc[4])
	assert(6 == sort_se2[0:3].mean())

def pd_shift_test():
	pd_test = DataFrame({'a':[1,2,3],'b':[4,5,6]}, index = ['line1','line2','line3'])
	pd_shift = pd_test.shift()
	assert(True == is_equal(pd_shift['a'][1],1))
	assert(True == is_equal(pd_test.loc['line2','b'],5))
	assert(True == is_equal(pd_test.iloc[1,1],5))

def pd_query_test():
	pd_test = DataFrame({'a':[1,-2,3],'b':[4,5,-6]}, index = ['line1','line2','line3'])
	pd_query = pd_test.query('a > 0')
	assert(True == is_equal(pd_test.iloc[1,0],-2))
	assert(True == is_equal(pd_query.iloc[1,0],3))
	max_label = pd_test['b'].idxmax()
	assert('line2' == max_label)

def add_test():
	test_equal(3, add(1,2))
	test_not_equal(4, add(1,2))

def StockAnalysis_test():
	analysis_tool = StockAnalysis()

def test_cases():
	add_test()
	se_sort_test()
	pd_shift_test()
	pd_query_test()
	StockAnalysis_test()

if __name__ == '__main__':
	test_cases()

