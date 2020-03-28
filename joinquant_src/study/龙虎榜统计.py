from jqdata import *
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


A_stock_list = get_index_stocks('000985.XSHG', date=None)
billboard = get_billboard_list(stock_list = A_stock_list, start_date = '2011-08-02', end_date = '2015-02-01')

billboard_increase = billboard[billboard['abnormal_name'] == '涨幅偏离值达7%的证券']

billboard_decrease = billboard[billboard['abnormal_name'] == '跌幅偏离值达7%的证券']

billboard_amplitude = billboard[billboard['abnormal_name'] == '日价格振幅达到15%的证券']

billboard_turnover = billboard[billboard['abnormal_name'] == '换手率达20%的证券']

billboard_increase_series = billboard[billboard['abnormal_name'] == '连续三个交易日内收盘价格涨幅偏离值累计达到20%的证券']

billboard_decrease_series = billboard[billboard['abnormal_name'] == '连续三个交易日内收盘价格跌幅偏离值累计达到20%的证券']

billboard_buy = billboard[billboard['direction'] == 'BUY']
billboard_org = billboard_buy[billboard_buy['sales_depart_name'] == '机构专用']

def get_industry_code_from_security(security,date=None):
    industry_index=get_industries(name='sw_l1').index
    for i in range(0,len(industry_index)):
        try:
            index = get_industry_stocks(industry_index[i],date=date).index(security)

            return industry_index[i]
        except:
            continue
    return u'未找到'

  def industry_count(data_df):
    date_list = list(data_df['day'])
    day_list = list(set(date_list))
    industry_df = get_industries(name='sw_l1')
    industry = list(industry_df['name'])

    industry_value = list(zeros(len(industry)))
    industries_df = pd.DataFrame(industry_value, index = industry)
    for day in day_list:
        daily_stock = data_df[data_df['day'] == day]
        stock_list = list(daily_stock['code'])
        code_list = list(set(stock_list))
        for code in code_list:
            code_industry = get_industry_code_from_security(code)
            if code_industry != '未找到':
                code_industry = industry_df.loc[code_industry,'name']
                for item in industry:
                    if code_industry == item:
                        industries_df.loc[item,0] = industries_df.loc[item,0] + 1
            else:
                pass
    summation = sum(industries_df)[0]
    industries_df = industries_df / summation
    return industries_df

 def benchmark_rate_calculation(stock_list):
    industry_df = get_industries(name='sw_l1')
    industry = list(industry_df['name'])
    industry_value = list(zeros(len(industry)))
    industries_df = pd.DataFrame(industry_value, index = industry)

    for code in stock_list:
        code_industry = get_industry_code_from_security(code)
        if code_industry != '未找到':
            code_industry = industry_df.loc[code_industry,'name']
            for item in industry:
                if code_industry == item:
                    industries_df.loc[item,0] = industries_df.loc[item,0] + 1
        else:
            pass
    summation = sum(industries_df)[0]
    industries_df = industries_df / summation
    return industries_df

 def industry_plot(benchmark, industry_count_df):
    industry_df = industry_count_df - benchmark
    industry_df = industry_df.sort_values(by = 0,ascending = False)
    industry_df.plot(kind = 'bar',figsize = (24,12))

 def part_belonged(data_df):
    date_list = list(data_df['day'])
    day_list = []
    day_list.append(date_list[0])
    for item in date_list:

            if item not in day_list:
                day_list.append(item)
    length = len(day_list) - 1
    range_length = range(0,length)
    daily_list = []
    for number in range_length:
        daily_list.append(day_list[length - number])
    part_df = pd.DataFrame() 
    for day in daily_list:
        daily_stock = data_df[data_df['day'] == day]
        stock_list = list(daily_stock['code'])
        code_list = list(set(stock_list))
        MAIN = 0
        SME = 0
        GEM = 0
        for code in code_list:
            if (code[:3] in ['000', '600', '603', '601']) or (code[:3] in ['001696', '001896', '001979', '001965']):
                MAIN = MAIN + 1
            elif code[:3] == '002':
                SME = SME + 1
            elif code[:3] == '300':
                GEM = GEM + 1
        summation = MAIN + SME + GEM
        part_daily = [MAIN / summation, SME / summation, GEM / summation, summation]
        part_df[day] = part_daily
    return part_df

def monthly_part(data_df):
    year_list = [2011, 2012, 2013, 2014, 2015]
    month_list = list(range(1,13))
    index_list = list(data_df.columns)
    year_month_df = pd.DataFrame()

    for year in year_list:
        for month in month_list:
            column_name = str(year) + str(month)
            column = [0, 0, 0, 0]
            for index in index_list:
                if index.year == year and index.month == month:
                    rows = [0, 1, 2, 3]
                    for number in rows:
                        column[number] = column[number] + data_df[index][number]
            if column[0] != 0 or column[1] != 0 or column[2] != 0:
                summation = column[0] + column[1] + column[2]
                for number in [0, 1, 2]:
                    column[number] = column[number] / summation
                year_month_df[column_name] = column
    return year_month_df

def price_calculation(data_df):
    billboard = data_df[data_df['direction'] != 'ALL']
    date_list = list(billboard['day'])
    day_list = list(set(date_list))
    day_list.sort(key = date_list.index)

    trade_days = list(get_all_trade_days())
    price_df = pd.DataFrame()
    
    for day in day_list:
        billboard_daily = billboard[billboard['day'] == day]
        code_list = list(set(billboard_daily['code']))
        location = trade_days.index(day)
        
        #获取每日对应期限内基准指数收益率
        benchmark_series = get_price('000985.XSHG', start_date = day, end_date = trade_days[location+60])['close']
        benchmark_list = list(benchmark_series)
        standard_benchmark = benchmark_list[0]
        absolute_benchmark = []
        for benchmark in benchmark_list:
            new_benchmark = benchmark / standard_benchmark
            absolute_benchmark.append(new_benchmark)
        
        #找出无法交易的股票    
        unavailable_list = []
        for code in code_list:
            volume_series = get_price(code, start_date = trade_days[location+1], end_date = trade_days[location+2])['volume']
            volume = volume_series[trade_days[location+1]]
            if volume == 0:
                unavailable_list.append(code)
        
        #剔除无法交易的股票
        available_list = []
        for code in code_list:
            if code not in unavailable_list:
                available_list.append(code)
        
        #对于每只股票计算对应期限超额收益
        for code in available_list:
            name = str(day) + str(',') + str(code)
            price_series = get_price(code, start_date = day, end_date = trade_days[location+60])['close']
            price_list = list(price_series)
            standard_price = price_list[0]
            absolute_price = []
            for price in price_list:
                new_price = price / standard_price
                absolute_price.append(new_price)
            return_list = []
            label = list(range(0,len(price_list)))
            for number in label:
                excess_return = absolute_price[number] - absolute_benchmark[number]
                return_list.append(excess_return)
            price_df[name] = return_list
    return price_df        


def return_plot(data_df):
    price_df = price_calculation(data_df)
    mean_price = mean(price_df, axis = 1)
    plt.figure(figsize=(24, 12))
    plt.plot(mean_price)

def depart_count(data_df, number):
    date_list = list(billboard_buy['day'])
    day_list = list(set(date_list))
    day_list.sort(key = date_list.index)
    trade_days = list(get_all_trade_days())

    price_df = pd.DataFrame()
    
    for day in day_list:
        daily_billboard = data_df[data_df['day'] == day]
        billboard_depart = daily_billboard[daily_billboard['sales_depart_name'] == '机构专用']
        location = trade_days.index(day)
        
        benchmark_series = get_price('000985.XSHG', start_date = day, end_date = trade_days[location+60])['close']
        benchmark_list = list(benchmark_series)
        standard_benchmark = benchmark_list[0]
        absolute_benchmark = []
        for benchmark in benchmark_list:
            new_benchmark = benchmark / standard_benchmark
            absolute_benchmark.append(new_benchmark)
         
        #筛选
        stock_list = list(daily_billboard['code'])
        code_list = list(set(stock_list))
        department_list = list(billboard_depart['code'])
        depart_list = list(set(department_list))
        primary_list = []
        for code in depart_list:
            code_summation = billboard_depart[billboard_depart['code'] == code]
            ranking_list = list(code_summation['rank'])
            rank_list = list(set(ranking_list))    
            if len(rank_list) > number:
                primary_list.append(code)
        
        
        unavailable_list = []
        for code in depart_list:
            volume_series = get_price(code, start_date = trade_days[location+1], end_date = trade_days[location+2])['volume']
            volume = volume_series[trade_days[location+1]]
            if volume == 0:
                unavailable_list.append(code)
        
        available_list = []
        for code in primary_list:
            if code not in unavailable_list:
                available_list.append(code)
                
        for code in available_list:
            name = str(day) + str(',') + str(code)
            price_series = get_price(code, start_date = day, end_date = trade_days[location+60])['close']
            price_list = list(price_series)
            standard_price = price_list[0]
            absolute_price = []
            for price in price_list:
                new_price = price / standard_price
                absolute_price.append(new_price)
            return_list = []
            label = list(range(0,len(price_list)))
            for number in label:
                excess_return = absolute_price[number] - absolute_benchmark[number]
                return_list.append(excess_return)
            price_df[name] = return_list
    return price_df        

def count_mean_price(data_df,numbers):
    mean_price_df = pd.DataFrame()
    primary_mean_price = price_calculation(data_df)
    mean_price_df['-1'] = mean(primary_mean_price, axis = 1)
    for number in numbers:

        price_df = depart_count(data_df,number)
        mean_price = mean(price_df, axis = 1)
        mean_price_df[number] = mean_price
    return mean_price_df

def depart_rate(data_df, rate):
    date_list = list(billboard_buy['day'])
    day_list = list(set(date_list))
    day_list.sort(key = date_list.index)
    trade_days = list(get_all_trade_days())

    price_df = pd.DataFrame()
    
    for day in day_list:
        daily_billboard = data_df[data_df['day'] == day]
        billboard_depart = daily_billboard[daily_billboard['sales_depart_name'] == '机构专用']
        location = trade_days.index(day)
        
        #基准
        benchmark_series = get_price('000985.XSHG', start_date = day, end_date = trade_days[location+60])['close']
        benchmark_list = list(benchmark_series)
        standard_benchmark = benchmark_list[0]
        absolute_benchmark = []
        for benchmark in benchmark_list:
            new_benchmark = benchmark / standard_benchmark
            absolute_benchmark.append(new_benchmark)
        
        #股票筛选
        stock_list = list(daily_billboard['code'])
        code_list = list(set(stock_list))
        department_list = list(billboard_depart['code'])
        depart_list = list(set(department_list))
        primary_list = []
        for code in depart_list:
            code_summation = billboard_depart[billboard_depart['code'] == code]
            summation = 0
            ranking_list = list(code_summation['rank'])
            rank_list = list(set(ranking_list))
            for rank in rank_list:
                rank_mean = code_summation[code_summation['rank'] == rank]
                mean_rank = sum(list(rank_mean['buy_rate'])) / len(list(rank_mean['buy_rate']))
                summation = summation + mean_rank
            if summation > rate:
                primary_list.append(code)
        
        #取消不能交易
        unavailable_list = []
        for code in depart_list:
            volume_series = get_price(code, start_date = trade_days[location+1], end_date = trade_days[location+2])['volume']
            volume = volume_series[trade_days[location+1]]
            if volume == 0:
                unavailable_list.append(code)
        
        #剩余股票
        available_list = []
        for code in primary_list:
            if code not in unavailable_list:
                available_list.append(code)
        
        for code in available_list:
            name = str(day) + str(',') + str(code)
            price_series = get_price(code, start_date = day, end_date = trade_days[location+60])['close']
            price_list = list(price_series)
            standard_price = price_list[0]
            absolute_price = []
            for price in price_list:
                new_price = price / standard_price
                absolute_price.append(new_price)
            return_list = []
            label = list(range(0,len(price_list)))
            for number in label:
                excess_return = absolute_price[number] - absolute_benchmark[number]
                return_list.append(excess_return)
            price_df[name] = return_list
    return price_df        

def rate_mean_price(data_df,numbers):
    mean_price_df = pd.DataFrame()
    primary_mean_price = price_calculation(data_df)
    mean_price_df['-1'] = mean(primary_mean_price, axis = 1)
    for number in numbers:

        price_df = depart_rate(data_df,number)
        mean_price = mean(price_df, axis = 1)
        mean_price_df[number] = mean_price
    return mean_price_df

 https://www.joinquant.com/view/community/detail/09d7f92e204dd1ce62b3315218811436?type=best
 K线放荡不羁
 探索公开交易信息之扒一扒龙虎榜

