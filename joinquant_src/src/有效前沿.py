# 导入函数库
import pandas as pd
import numpy as np
import jqdata
#最优化投资组合的推导是一个约束最优化问题
import scipy.optimize as sco

def handle_data(context, data):
    pass

# 初始化函数，设定基准等等
def initialize(context):
    set_option('use_real_price', True)
    
    ## 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）
      # 开盘前运行
    run_daily(before_market_open, time='before_open', reference_security='000300.XSHG') 
      # 开盘时运行
    run_daily(market_open, time='open', reference_security='000300.XSHG')
      # 收盘后运行
    run_daily(after_market_close, time='after_close', reference_security='000300.XSHG')

class EffectiveFrontier(object):
    def __init__(self):
        self.stock_set = ['000063.XSHE','000001.XSHE','000002.XSHE']
        self.noa = len(self.stock_set)
        self.returns = pd.DataFrame()
        self.get_returns()
    def get_returns(self):
        start_date = '2017-01-01'
        end_date = '2017-08-01'
        df = get_price(self.stock_set, start_date, end_date, 'daily', ['close'])
        data = df['close']
        self.returns = np.log(data / data.shift(1))
    def monte_carlo(self):
        port_returns = []
        port_variance = []
        for p in range(4000):
            weights = np.random.random(self.noa)
            weights /=np.sum(weights)
            port_returns.append(np.sum(self.returns.mean()*weights))
            port_variance.append(np.sqrt(np.dot(weights.T, np.dot(self.returns.cov(), weights))))
        port_returns = np.array(port_returns)
        port_variance = np.array(port_variance)
    def statistics(self, weights):
        weights = np.array(weights)
        port_returns = np.sum(self.returns.mean()*weights)
        port_variance = np.sqrt(np.dot(weights.T, np.dot(self.returns.cov(),weights)))
        return np.array([port_returns, port_variance, port_returns/port_variance])
    #最小化夏普指数的负值
    def min_sharpe(self, weights):
        return -self.statistics(weights)[2]
    def optimize_func(self):
        #约束是所有参数(权重)的总和为1。这可以用minimize函数的约定表达如下
        cons = ({'type':'eq', 'fun':lambda x: np.sum(x)-1})
        #我们还将参数值(权重)限制在0和1之间。这些值以多个元组组成的一个元组形式提供给最小化函数
        bnds = tuple((0,1) for x in range(self.noa))
        #优化函数调用中忽略的唯一输入是起始参数列表(对权重的初始猜测)。我们简单的使用平均分布。
        opts = sco.minimize(self.min_sharpe, self.noa*[1./self.noa,], method = 'SLSQP', bounds = bnds, constraints = cons)
        log.info(opts)
    def calc_frontier(self):
        self.monte_carlo()
        self.optimize_func()

class Relative():
    def __init__(self):
        pass
    def _get_all_stock_list(self, stock_list):
        stock_df = get_fundamentals(query(
            valuation.code, valuation.market_cap, valuation.pe_ratio, valuation.pb_ratio,income.total_operating_revenue
        ).filter(
            valuation.pe_ratio < 65,
            valuation.pe_ratio > 0,
            valuation.pb_ratio < 5,
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
    def relative_analyze(self):
        base_stock = '600795.XSHG'
        stock_set = []
        all_stocks = []
        base_start_date = '2017-01-01'
        base_end_date = '2017-08-01'
        base_close_df = get_price([base_stock], base_start_date, base_end_date, 'daily', ['close'])['close']
        self._get_all_stock_list(all_stocks)
        #all_stocks = ["600886.XSHG"]
        corr_dict = {}
        for m_stock in all_stocks:
            if m_stock == base_stock:
                continue
            start_date = '2017-01-10'
            end_date = '2017-08-10'
            close_df = get_price([m_stock], start_date, end_date, 'daily', ['close'])['close']
            corr_dict[m_stock] = base_close_df[base_stock].corr(close_df[m_stock])
        
        corr_dict = sorted(corr_dict.iteritems(),key = lambda d:d[1],reverse = True)
        log.info(corr_dict[0])
        log.info(corr_dict[-1])
        #first_k = corr_dict.keys()[0]
        #last_k = corr_dict.keys()[-1]
        #log.info(corr_dict[first_k])
        #log.info(corr_dict[last_k])
        
class VarMethod():
    def __get_rate_chance(self, return_rate_df, ret_rate_chance):
        section_var_tmp = range(-1005, 1010, 5)
        section_var = [float(x)/1000 for x in section_var_tmp]
        all_count = float(return_rate_df.dropna().count().values[0])
        dict_section = {}
        for rate in section_var:
            obj_df = (return_rate_df[return_rate_df > rate])
            item_df = obj_df.dropna()
            item_count = float(item_df.count().values[0])
            dict_section[rate] = item_count/all_count
        rate_dict = sorted(dict_section.iteritems(),key = lambda d:d[0],reverse = False)
        for (rate, chance) in rate_dict:
            if (chance < 0.95):
                break
        ret_rate_chance.append((rate, chance))
        
    def __get_position(self, rate):
        position = (-1.0/100.0)/rate
        log.info("position = %f"%position)
        
    def single_var(self):
        base_stock = '600415.XSHG'
        base_start_date = '2017-06-01'
        base_end_date = '2017-08-29'
        base_close_df = get_price([base_stock], base_start_date, base_end_date, 'daily', ['close'])['close']
        return_rate_df = base_close_df/base_close_df.shift(1) - 1
        ret_list = []
        self.__get_rate_chance(return_rate_df,ret_list)
        (rate, chance) = ret_list[0]
        log.info("(rate = %f, chance = %f)"%(rate, chance))
        self.__get_position(rate)
            
## 开盘前运行函数     
def before_market_open(context):
    log.info('before market open')
    #有效前沿
    #effectFrontier = EffectiveFrontier()
    #effectFrontier.calc_frontier()
    #相关性
    #relativeAnalyze = Relative()
    #relativeAnalyze.relative_analyze()
    #风险价值
    varMethod = VarMethod()
    varMethod.single_var()
    
## 开盘时运行函数
def market_open(context):
    log.info('market open')
 
## 收盘后运行函数  
def after_market_close(context):
    log.info('market close')
    