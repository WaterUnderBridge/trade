import tushare as ts

def test1():
	profit = 262.42 - 252.4 + 0.61 + 7.35 + 0.475 + 7.17
	print("profit = %f"%(profit))
	profit1 = 262.42 - 252.4 + 0.61 + 7.35 + 0.475 + 7.17+0.1565-0.447
	print("profit1 = %f"%(profit1))
	profit2 = 262.42 - 252.4 + 0.61 + 7.35 + 0.475 + 7.17+0.1565-0.447-4.5
	print("profit2 = %f"%(profit2))
	profit3 = 262.42 - 252.4 -4.5 + 1.42-(-0.9766)+(-0.1155)
	print("profit3 = %f"%(profit3))
	ret = 169.43 + 1.8 + 21.66 + 12.49 + 41.92 + 3.79 - 0.116 + 1.42
	print("re1 = %f"%(ret))

def test2():
	#9a8ef78af271d1c77368eca7e9b4c800481b7b125936725705f9c21d
	print("a")
	print(ts.__version__)
	ts.set_token('9a8ef78af271d1c77368eca7e9b4c800481b7b125936725705f9c21d')
	pro = ts.pro_api('9a8ef78af271d1c77368eca7e9b4c800481b7b125936725705f9c21d')

if __name__ == "__main__":
	print("hello wolrd2!")
	test2()




