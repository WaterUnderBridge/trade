df = attribute_history('000001.XSHE', 5, '1d') #获取DataFrame表
    write_file('df.csv', df.to_csv(), append=False) #写到文件中