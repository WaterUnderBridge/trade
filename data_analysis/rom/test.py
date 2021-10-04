import xlrd
import xlwt
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
mpl.rcParams['font.sans-serif'] = ['KaiTi']
mpl.rcParams['font.serif'] = ['KaiTi']

def pd_test():
	work_book = xlrd.open_workbook('test.xls')
	names = work_book.sheet_names()
	ret_df = pd.DataFrame()
	for data_name in names:
		df = pd.read_excel(r"test.xls",sheet_name = data_name)
		df.index = df['content']
		df = df.drop('content', axis = 1)
		init_col = list(df.columns)
		if ("QT" in init_col and "QM" in init_col):
			df['Q3'] = df['QT'] - df['QM']
		if ("QM" in init_col and "Q1" in init_col):
			df['Q2'] = df['QM'] - df['Q1']
		if ("QN" in init_col and "QT" in init_col):
			df['Q4'] = df['QN'] - df['QT']
		if "QN" in init_col:
			df = df.drop('QN', axis = 1)
		if "QM" in init_col:
			df = df.drop('QM', axis = 1)
		if "QT" in init_col:
			df = df.drop('QT', axis = 1)
		order = ['Q4','Q3','Q2','Q1']
		df = df[order]
		pre_name = data_name + "_"
		format_col = [pre_name + index for index in list(df.columns)]
		df.columns = format_col
		df.loc['计算营业利润'] = df.loc['营业总收入'] - df.loc['营业总成本'] + df.loc['公允价值变动收益'] + df.loc['投资收益'] + df.loc['资产处置收益'] + df.loc['其他收益']
		df.loc['计算营业总成本'] = df.loc['营业成本'] + df.loc['营业税金及附加'] + df.loc['销售费用'] + df.loc['管理费用'] + df.loc['研发费用'] + df.loc['财务费用']+ df.loc['资产减值损失'] + df.loc['信用减值损失']
		df.loc['计算毛利润'] = df.loc['营业总收入'] - df.loc['营业总成本']
		df.loc['计算扣非净利润'] = df.loc['营业总收入'] - df.loc['营业总成本']- df.loc['所得税费用']+df.loc['信用减值损失']+df.loc['资产减值损失']-df.loc['少数股东损益']
		#print(df)
		if 0 == len(ret_df.index):
			ret_df = df
		else:
			ret_df = pd.concat([ret_df, df],axis = 1)
	with pd.plotting.plot_params.use('x_compat', True):#color=r,g,b,y,k,
		ret_df.loc['营业总收入'].plot(color = 'r',legend=True)
		ret_df.loc['营业总成本'].plot(color = 'g',legend=True)
		ret_df.loc['计算营业总成本'].plot(color = 'b',legend=True)
	'''

	with pd.plotting.plot_params.use('x_compat', True):#color=r,g,b,y,k,
		ret_df.loc['净利润'].plot(color = 'r',legend=True)
		ret_df.loc['扣非净利润'].plot(color = 'g',legend=True)
		ret_df.loc['计算扣非净利润'].plot(color = 'k',legend=True)
		ret_df.loc['营业利润'].plot(color = 'b',legend=True)
		ret_df.loc['研发费用'].plot(color = 'y',legend=True)
	'''
	ret_df["content"] = ret_df.index
	ret_df.to_excel("ret1.xls",index = False)
	plt.show()

if __name__ == "__main__":
	pd_test()





