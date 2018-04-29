from __future__ import division
import numpy as np
import sympy
import time
import math

def deco_timer(func):
	def calc_timer(*args,**argv):
		t0 = time.time()
		result = func(*args,**argv)
		diff_time = time.time() - t0
		print("[%.8fs] %s"%(diff_time, func.__name__))
		return result
	return calc_timer

def float_div(a,b):
	return a/float(b)

def get_YTM(face_val, rate, market_val, y_n):
	x = sympy.Symbol('x', real = True, positive = True)
	fx = -market_val
	fx += face_val/(1+x)**y_n
	c = face_val*rate
	for i in range(1,y_n+1):
		fx += c/(1+x)**i
	ret = sympy.solve(fx,x)
	if 1 == len(ret):
		return ret[0]
	else:
		return 0

def get_f_a(rate, y_n):
	val_list = []
	for i in range(y_n):
		val_list.append((1+rate)**i)
	val_array = np.array(val_list)
	f_a = val_array.sum()
	return f_a

def get_p_a(rate, y_n):
	init_list = []
	for i in range(1, y_n + 1):
		init_list.append(1/(1+rate)**i)
	p_a_array = np.array(init_list)
	p_a = p_a_array.sum()
	return p_a

def get_final_from_annuity(annuity, rate, y_n):
	f_a = get_f_a(rate, y_n)
	final_val = annuity * f_a
	return final_val
@deco_timer
def get_annuity_from_final(final, rate, y_n):
	f_a = get_f_a(rate, y_n)
	print('f_a = %f'%(f_a))
	annuity = float_div(final, f_a)
	return annuity

def get_init_from_annuity(annuity, rate, y_n):
	p_a = get_p_a(rate, y_n)
	init_val = annuity * p_a
	return init_val

def get_annuity_from_init(init_val, rate, y_n):
	p_a = get_p_a(rate, y_n)
	print('p_a = %f'%(p_a))
	annuity = float_div(init_val,p_a)
	return annuity

def get_market_val_from_ytm(face_val, rate, ytm, y_n):
	c = face_val * rate
	market_val = float_div(face_val, (1+ytm)**y_n)
	for i in range(1,y_n+1):
		market_val += float_div(c, (1+ytm)**i)
	return market_val

def get_Dmac(face_val, rate, ytm, y_n):
	market_val = get_market_val_from_ytm(face_val, rate, ytm, y_n)
	calc_dmac = 0
	c = face_val * rate
	calc_dmac += float_div(face_val, (1+ytm)**y_n)*y_n
	for i in range(1,y_n+1):
		calc_dmac += float_div(c,(1+ytm)**i)*i
	return float_div(calc_dmac, market_val)

def get_delta_p_from_delta_y(Dmod, market_val, delta_y):
	delta_p = -market_val*Dmod*delta_y
	return delta_p

def get_found_subscription_share(subscribed_amount, fee, interest,found_face_val):
	net_amount = float_div(subscribed_amount, 1+fee)
	share = float_div(net_amount+interest,found_face_val)
	return share

def get_accounts_receivable_turnover_days(sales_volume, accounts_receivable):
	turnover_days_fee = float_div(sales_volume, accounts_receivable)
	turnover_days = float_div(365, turnover_days_fee)
	return turnover_days

def get_geometric_avg_rate_return(return_list):
	periods_n = len(return_list)
	if 0 == periods_n:
		return 0
	mul_result = 1
	for data in return_list:
		mul_result *= (1 + data)
	rate_g = mul_result**float_div(1,periods_n) - 1
	return rate_g

def get_weight_time_rate_return(net_init_share_list, bonus_list):
	net_share_len = len(net_init_share_list)
	bonus_len = len(bonus_list)
	if net_share_len != bonus_len:
		return 0
	if net_share_len < 3:
		return float_div(net_init_share_list[1], net_init_share_list[0])
	rate_return = float_div(net_init_share_list[1], net_init_share_list[0])
	for i in range(1, net_share_len - 1):
		rate_return*= float_div(net_init_share_list[i+1], net_init_share_list[i] - bonus_list[i])
	return rate_return - 1

def get_sharpe_ratio(found_yield, risk_free_rate_return, std_found_yield):
	return float_div(found_yield - risk_free_rate_return, std_found_yield)

def cppi(principal,rf,m,rt_list):
	#check args valid
	ret_A_list = []
	ret_E_list = []
	ret_D_list = []
	y_n = len(rt_list)
	At = principal
	for i in range(y_n):
		Ft = float_div(principal, (1+rf)**(y_n-i))
		St = At - Ft
		Et = m * St
		Dt = At - Et
		ret_A_list.append(At)
		ret_E_list.append(Et)
		ret_D_list.append(Dt)
		At = Dt*(1+rf) + Et *(1 + rt_list[i])
	return zip(ret_A_list, ret_E_list, ret_D_list)

def kelly(win_probability, rw, rl):
	return float_div(win_probability, rl) - float_div(1-win_probability, rw)

def math_test():
	ytm = kelly(0.5, 5, 1)
	print(ytm)

if __name__ == '__main__':
	ret = get_init_from_annuity(39600, 0.048, 30)
	print(ret)