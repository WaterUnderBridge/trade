#encoding=utf8

#你好
def add(a, b):
    return a+b

def minus(a, b):
    return a-b

def multi(a, b):
    return a*b

def divide(a, b):
    return a/b

def test_func():
	ret = add(1,2)
	print(ret)

if __name__ == '__main__':
	print("hello world!")
	test_func()