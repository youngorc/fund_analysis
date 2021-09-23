import os
import pandas as pd
import json
import requests
import re
import datetime
from concurrent.futures import ThreadPoolExecutor,as_completed,wait
pd.set_option('display.max_columns', None)

today=datetime.datetime.today().strftime("%Y%m%d")
code_list=[399997,399989]

def get_trading_volume(code):
    url="http://push2his.eastmoney.com/api/qt/stock/kline/get"
    headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

    }
    params={#'cb': 'jQuery112405429083791998797_1632366916319',
    'secid': '0.{}'.format(code)
    # ,'ut': 'fa5fd1943c7b386f172d6893dbfba10b'
    ,'fields1': 'f1,f2,f3,f4,f5'
    ,'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58'
    ,'klt': '101'
    ,'fqt': '0'
    ,'beg': '20210101'
    ,'end': '{}'.format(today)
    #,'_': '1632366916438'
    }
    r=requests.get(url,params=params,headers=headers)
    dict1=json.loads(r.text)
    dict2=dict1["data"]
    dict3=dict2["klines"]
    df=pd.DataFrame(dict3)
    df_parse=df.iloc[:,0].str.split(",",expand=True)
    df_parse.columns=["日期","开盘","收盘","最高","最低","成交量","成交额","振幅"]
    df_parse["基金"]=code
    print("{}处理完毕！".format(code))
    return df_parse

with ThreadPoolExecutor() as pool:
    futures={}
    for code in code_list:
        result=pool.submit(get_trading_volume,code)
        futures[result] = str(code)
    wait(futures)
    file=pd.ExcelWriter("成交额数据.xlsx")
    for future in as_completed(futures):
        code=futures[future]
        (future.result()).to_excel(file,sheet_name=code,index=None)
        print("{}输出完毕！".format(code))
    file.save()
    file.close()


