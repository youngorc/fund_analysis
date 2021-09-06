import pandas as pd
import time
import os
import re
import datetime
import xlrd
import matplotlib.pyplot as plt
import numpy as np
import requests
import json
plt.rcParams['font.family'] = ['SimHei']
plt.rcParams['axes.unicode_minus']=False
pd.set_option('display.max_columns', None)

def get_fund_code(fund):
    '''获取基金code'''
    result = re.search("\d{6}", fund)
    if result == None:
        print("{}中未包含基金代码，请修正".format(fund))
        return None
    else:
        return result.group()

def get_quote_pe_pb(quote_code):
    '''根据股票代码从亿牛网爬取滚动pe/pb'''
    if quote_code.lower().startswith("hk"):
        r_pe = requests.get("https://eniu.com/chart/peh/{}".format(quote_code))
        r_pb = requests.get("https://eniu.com/chart/pbh/{}".format(quote_code))
    else:
        r_pe = requests.get("https://eniu.com/chart/pea/{}/t/60".format(quote_code))
        r_pb = requests.get("https://eniu.com/chart/pba/{}/t/60".format(quote_code))
    dict1=json.loads(r_pe.text)
    dict2=json.loads(r_pb.text)
    df_pe_pb_1 = pd.DataFrame(dict1)
    df_pe_pb_1.columns=["date","pe_ttm","price"]
    df_pe_pb_2 = pd.DataFrame(dict2)
    df_pe_pb_2.columns = ["date", "pb", "price"]
    df_pe_pb=pd.merge(left=df_pe_pb_1,right=df_pe_pb_2,on="date",how="inner")
    df_pe_pb["code"]=quote_code
    return df_pe_pb



def get_fund_code_excel(fund):
    try:
        df=pd.read_excel(writer_fund_pe,sheet_name=fund)
    except:
        df=pd.DataFrame(columns=["date","fund","累计净值","pe_ttm","pb"])
    return df

def get_fund_code_data(df):
    return (today-datetime.timedelta(120)).strftime("%Y-%m-%d") if pd.isna(df["date"].max()) else df["date"].max()

today=datetime.datetime.today()
file = r"./数据/基金持仓数据.xlsx"
jingzhi_file = r"./数据/基金日线数据.xlsx"
xlsx=pd.ExcelFile(file)
writer_pe = pd.ExcelWriter(r"./数据/持仓股票市盈率.xlsx")
writer_pb = pd.ExcelWriter(r"./数据/持仓股票市净率.xlsx")
writer_fund_pe = pd.ExcelWriter(r"./数据/基金历史估值.xlsx")
for fund in xlsx.sheet_names:
    df=pd.read_excel(file,sheet_name=fund,dtype=str)
    data=pd.DataFrame()
    for code in df["代码"]:
        if code=="nan":
            pass
        else:
            df_pe_pb=get_quote_pe_pb(code)
            data=data.append(df_pe_pb,ignore_index=True)
    if data.empty:
        print("{}基金估值爬取跳过!".format(fund))
        pass
    else:
        result_pe=pd.pivot_table(data,"pe_ttm",columns="code",index="date")
        result_pb=pd.pivot_table(data,"pb",columns="code",index="date")
        df["净值占比"]=df["占净值比例"].str.extract("(\d+\.\d+)").astype("float")
        df_cc=pd.pivot_table(df,"净值占比",columns="代码")*0.01
        fund_history_pe=get_fund_code_excel(fund)
        last_pe_day=get_fund_code_data(fund_history_pe)
        if len(result_pe[result_pe.index > last_pe_day]) > 0:
            fund_current_pe = result_pe[result_pe.index > last_pe_day].copy()
            fund_current_pb = result_pb[result_pb.index > last_pe_day].copy()
            fund_current_pe["pe_ttm"] = ((np.array(fund_current_pe) * np.array(df_cc)).sum(axis=1)) / (df_cc.sum(axis=1)).values
            fund_current_pe["pb"] = ((np.array(fund_current_pb) * np.array(df_cc)).sum(axis=1))  / (df_cc.sum(axis=1)).values
            fund_current_pe["fund"] = fund
            fund_current_pe.reset_index(inplace=True)
            fund_price=pd.read_excel(jingzhi_file,sheet_name=fund)
            fund_current_guzhi=pd.merge(fund_current_pe,fund_price,left_on="date",right_on="净值日期",how="left")
            fund_history_pe = fund_history_pe.append(fund_current_guzhi[["date", "fund", "累计净值","pe_ttm","pb"]],ignore_index=True)
        else:
            pass
        result_pe["基金"] = fund
        result_pb["基金"] = fund
        result_pe.reset_index(inplace=True)
        result_pb.reset_index(inplace=True)
        print("{}基金估值爬取完毕!".format(fund))
        result_pe.to_excel(writer_pe, sheet_name=fund,index=None)
        result_pb.to_excel(writer_pb, sheet_name=fund,index=None)
        fund_history_pe.to_excel(writer_fund_pe,sheet_name=fund,index=None)
writer_pe.save()
writer_pe.close()
writer_pb.save()
writer_pb.close()
writer_fund_pe.save()
writer_fund_pe.close()