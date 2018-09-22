# 导入函数库
from jqdata import *

# 初始化函数，设定基准等等
def initialize(context):
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    
    # 过滤掉order系列API产生的比error级别低的log
    # log.set_level('order', 'error')
    
    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')
    
    ## 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）
      # 开盘前运行
    run_daily(before_market_open, time='before_open', reference_security='000300.XSHG') 
      # 开盘时或每分钟开始时运行
    #run_daily(market_open, time='every_bar', reference_security='000300.XSHG')
      # 收盘后运行
    run_daily(after_market_close, time='after_close', reference_security='000300.XSHG')
    
def _get_all_stock_list(stock_list):
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
    
## 开盘前运行函数     
def before_market_open(context):
    g.cnt = 0
    g.stock_list = []
    _get_all_stock_list(g.stock_list)
    
## 开盘时运行函数
def market_open(context):
    pass

def handle_data(context, data):
    g.cnt += 1
    #g.stock_list = ['000063.XSHE']
    if 2 == g.cnt:
        for stock in g.stock_list:
            close_data = attribute_history(stock, 1, '1d', ['close'])['close'][0]
            current_price = data[stock].close
            if current_price > close_data * 1.03:
                log.info(stock)
 
## 收盘后运行函数  
def after_market_close(context):
    pass

