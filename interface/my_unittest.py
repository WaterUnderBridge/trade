#encoding=utf8
import numpy as np
import pandas as pd
from pandas import Series,DataFrame
import matplotlib.pyplot as plt
from common import *

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

def add_test():
	test_equal(3, add(1,2))
	test_not_equal(4, add(1,2))

def test_cases():
	add_test()
	se_sort_test()

if __name__ == '__main__':
	test_cases()

