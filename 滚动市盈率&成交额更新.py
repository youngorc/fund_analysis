#!/usr/bin/python3
# -*- coding: utf-8 -*-
'''
@Time    : 2021/12/7 0007 17:22
@Author  : youngorc
@FileName: series.py
@Software: PyCharm
@GitHub:https://github.com/youngorc
仅用于分析股票、非板块指数，在<持仓明细分析.py>后跑，只可分析含a股、港股基金
'''

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
import argparse
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor,as_completed,wait
pd.set_option('display.max_columns', None)
plt.rcParams['font.family'] = ['SimHei']
plt.rcParams['axes.unicode_minus']=False


# def get_fund_code(fund):
#     '''获取基金code'''
#     result = re.search("\d{6}", fund)
#     if result == None:
#         print("{}中未包含基金代码，请修正".format(fund))
#         return None
#     else:
#         return result.group()

def get_quote_pe_pb_volumn(quote_code,start):
    '''根据股票代码从亿牛网爬取滚动pe/pb'''
    months=(today.year-start.year)*(today.month-start.month)+1
    if months>=120:
        t=120
    elif months>=60:
        t=60
    elif months>=12:
        t=12
    elif months>6:
        t=6
    else:
        t=3

    try:
        if quote_code.lower().startswith("hk"):
            r_pe = requests.get("https://eniu.com/chart/peh/{}".format(quote_code))
            r_pb = requests.get("https://eniu.com/chart/pbh/{}".format(quote_code))
        else:
            r_pe = requests.get("https://eniu.com/chart/pea/{}/t/{}".format(quote_code,t))
            r_pb = requests.get("https://eniu.com/chart/pba/{}/t/{}".format(quote_code,t))
        dict1=json.loads(r_pe.text)
        dict2=json.loads(r_pb.text)
        df_pe_pb_1 = pd.DataFrame(dict1)
        df_pe_pb_1.columns=["date","pe_ttm","price"]
        df_pe_pb_2 = pd.DataFrame(dict2)
        df_pe_pb_2.columns = ["date", "pb", "price"]
        df_pe_pb=pd.merge(left=df_pe_pb_1,right=df_pe_pb_2,on="date",how="inner")
        df_pe_pb["code"]=quote_code
    except:
        df_pe_pb=pd.DataFrame(columns=['date', 'pe_ttm', 'price_x', 'pb', 'price_y', 'code'])
    '''
    :param code: 股票代码
    :return:
    '''
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

    }
    params = {  # 'cb': 'jQuery112405429083791998797_1632366916319',
        'secid': '{}.{}'.format("1" if quote_code.startswith("sh") else '116' if quote_code.startswith("hk") else "0", quote_code[2:])
        # ,'ut': 'fa5fd1943c7b386f172d6893dbfba10b'
        , 'fields1': 'f1,f2,f3,f4,f5'
        , 'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58'
        , 'klt': '101'
        , 'fqt': '0' if ~quote_code.startswith("hk") else '1'
        , 'beg': '{}'.format(start.strftime("%Y%m%d"))
        , 'end': '{}'.format(today.strftime("%Y%m%d"))
        # ,"lmt": '10'
    }
    r = requests.get(url, params=params, headers=headers)
    dict1 = json.loads(r.text)
    dict2 = dict1["data"]
    dict3 = dict2["klines"]
    df = pd.DataFrame(dict3)
    df_parse = df.iloc[:, 0].str.split(",", expand=True)
    df_parse.columns = ["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "振幅"]
    df_parse["股票"] = quote_code
    result=pd.merge(left=df_parse,right=df_pe_pb,left_on='日期',right_on='date',how="left")
    output=result[['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '股票', \
       'pe_ttm',  'pb', 'code']]
    return output

def get_fund_code_excel(file,fund):
    try:
        df=pd.read_excel(file,sheet_name=fund)
    except:
        df=pd.DataFrame(columns=["日期","基金","持仓","累计净值","pe_ttm","pb","成交额","成交量","平均每次成交额"])
    return df

# def get_fund_code_date(df):
#     return (today-datetime.timedelta(120)).strftime("%Y-%m-%d") if pd.isna(df["date"].max()) else df["date"].max()

def plot_graph(df_fund,fund,last_days=120):
    period=5
    if len(df_fund) > 20:
        df=df_fund[df_fund.notnull().all(axis=1)]
        pe_line_25 = df["pe_ttm"].sort_values().tolist()[len(df)//4]
        pe_line_50 = df["pe_ttm"].sort_values().tolist()[len(df)//2]
        pe_line_75 = df["pe_ttm"].sort_values().tolist()[len(df)*3 //4]
        pb_line_25 = df["pb"].sort_values().tolist()[len(df)//4]
        pb_line_50 = df["pb"].sort_values().tolist()[len(df)//2]
        pb_line_75 = df["pb"].sort_values().tolist()[len(df)*3 //4]
        df_plot = df[-last_days:]
        # df_plot = df_plot[df_plot.notnull().all(axis=1)]
        # plt.figure(figsize=(10,10))
        # plt.subplot(3, 1, 1)
        fig, axes = plt.subplots(3,1)
        axe1 = axes[0].twinx()
        # xticks=[i for i in df_plot["日期"][::3]]
        axes[0].plot(df_plot["日期"][::period],df_plot["累计净值"][::period],color="k",lw=1.,label="累计净值")
        axe1.plot(df_plot["日期"][::period],df_plot["pe_ttm"][::period],color='orange', lw=1., label="pe_ttm")
        axes[0].set_xticklabels(df_plot["日期"][::period],rotation=45,fontsize=7)
        axe1.text(df_plot.iloc[-5].日期, df_plot.iloc[-1].pe_ttm,
                 "{:4.2f}".format(df_plot.iloc[-1]["pe_ttm"]), color="blue", fontsize=6,weight="bold")
        # axes[0].legend()
        axe1.legend()
        axe1.axhline(y=pe_line_25, color='green',lw=0.5, linestyle='--')
        axe1.axhline(y=pe_line_50, color='grey', lw=0.5, linestyle='--')
        axe1.axhline(y=pe_line_75, color='red', lw=0.5, linestyle='--')
        plt.title("{}".format(fund))
        #第二幅图
        axe2 = axes[1].twinx()
        axes[1].plot(df_plot["日期"][::period],df_plot["累计净值"][::period],color="k",lw=1.,label="累计净值")
        axe2.plot(df_plot["日期"][::period],df_plot["pb"][::period],color='orange', lw=1., label="pb")
        axes[1].set_xticklabels(df_plot["日期"][::period],rotation=45,fontsize=7)
        axe2.text(df_plot.iloc[-5].日期, df_plot.iloc[-1].pb,
                 "{:4.2f}".format(df_plot.iloc[-1]["pb"]), color="blue", fontsize=6,weight="bold")
        # axes[1].legend()
        axe2.axhline(y=pb_line_25, color='green',lw=0.5, linestyle='--')
        axe2.axhline(y=pb_line_50, color='grey', lw=0.5, linestyle='--')
        axe2.axhline(y=pb_line_75, color='red', lw=0.5, linestyle='--')
        axe2.legend()
        axe3 = axes[2].twinx()
        trade_range=90
        axes[2].bar(df_plot.iloc[-trade_range:]["日期"],df_plot.iloc[-trade_range:]["成交额"])
        axe3.plot(df_plot.iloc[-trade_range:]["日期"],df_plot.iloc[-trade_range:]["平均每次成交额"]\
                  ,color='orange', lw=1., label="平均每次成交额")
        axes[2].set_xticklabels(df_plot.iloc[-trade_range:]["日期"][::period], rotation=45, fontsize=7)
        axes[2].set_xticks(df_plot.iloc[-trade_range:]["日期"][::period])
        axes[2].text(df_plot.iloc[-1].日期, df_plot.iloc[-1].成交额,
                 "{:4.2f}万".format(df_plot.iloc[-1]["成交额"]), color="blue", fontsize=6,weight="bold")
        plt.tight_layout()
        plt.savefig("./数据/{}市盈率分析指标".format(fund),bbox_inches='tight',dpi=200)
        plt.close()
    else:
        print("{}线数据太少，无法绘图".format(fund))

def multithread_crawl_data(config):
    '''多线程爬取所有基金下的所有股票pe、pb以及成交额
        file_src存放基金pe、pb、成交额等历史数据的文件
        file_cc存放基金持仓数据的文件
    '''
    file_src=config.file_src
    file_cc=config.file_cc
    df_output=pd.DataFrame()
    futures = []
    with ThreadPoolExecutor() as pool:
        for sheet in pd.ExcelFile(file_cc).sheet_names:
            df_src=get_fund_code_excel(file_src,sheet)
            max_time = datetime.datetime(2000, 1, 1, 0, 0,0) if df_src.empty else \
                datetime.datetime.strptime(df_src["日期"].max(),"%Y-%m-%d")
            df_cc=pd.read_excel(file_cc,sheet_name=sheet)
            quote_list=df_cc["代码"].tolist()
            for code in quote_list:
                result=pool.submit(get_quote_pe_pb_volumn,(code),(max_time))  #获取pe,pb,成交量
                futures.append(result)  #获取
        for future in as_completed(futures):
            df_output=df_output.append(future.result(),ignore_index=True).drop_duplicates()
            print("已获取数据{}条".format(len(df_output)))
    return df_output

def pivot_concat_table(df_src, df_jingzhi, df_output, df_cc, fund):
    df_output = df_output[(df_output["成交额"].notnull()) & (df_output["成交量"].notnull())]
    df_output["成交额"] = df_output["成交额"].astype(float)
    df_output["成交量"] = df_output["成交量"].astype(float)
    df = pd.merge(left=df_cc[["股票名称", "占净值比例", "代码", "基金"]], right=df_output, left_on="代码", right_on="股票",\
                  how="inner")
    df_pivot = pd.pivot_table(df, values=["成交额", "成交量", "pe_ttm", "pb"], index="日期", columns="股票")
    df_cc["净值占比"] = df_cc["占净值比例"].str.extract("(\d+\.\d+)").astype("float")
    df_cc_pt = pd.pivot_table(df_cc, "净值占比", columns="代码") * 0.01
    result = pd.DataFrame()
    result["日期"] = df_pivot.index
    result["基金"] = fund
    result["持仓"] = " ;".join((df_cc["股票名称"] + "," + df_cc["占净值比例"]).tolist())
    for col in df_pivot.columns.levels[0]:
        result[col] = ((np.array(df_pivot[col]) * np.array(\
            df_cc_pt.loc[:, df_cc_pt.columns.isin(df_pivot[col])])).sum(axis=1)) / (\
                          df_cc_pt.loc[:, df_cc_pt.columns.isin(df_pivot[col])].sum(axis=1)).values
    result["成交额"] = result["成交额"] / 10000
    result["成交量"] = result["成交量"] / 10000
    output = pd.merge(left=result, right=df_jingzhi[["净值日期", "累计净值"]], left_on="日期", right_on="净值日期", how="left")
    for col in ["pe_ttm","pb","成交额","成交量"]:
        if col not in output.columns:
            output[col]=None
    output = output[["日期","基金","持仓","累计净值","pe_ttm","pb","成交额","成交量"]]
    output["平均每次成交额"] = output["成交额"]/output["成交量"]
    df_fund=df_src.append(output,ignore_index=True)
    plot_graph(df_fund,fund)
    return df_fund

def multiprocess_analysis_data(config,df_outout):
    with ProcessPoolExecutor() as pool:#必须放在__name__=="__main__"里面
        xlsx=pd.ExcelFile(config.file_cc)
        writer=pd.ExcelWriter(config.file_src)
        futures={}
        for fund in xlsx.sheet_names:
            df_src=get_fund_code_excel(config.file_src,fund)
            df_jingzhi=pd.read_excel(config.file_jingzhi,sheet_name=fund)
            df_cc=pd.read_excel(config.file_cc,sheet_name=fund)
            future=pool.submit(pivot_concat_table,(df_src),(df_jingzhi),(df_output),(df_cc),(fund))
            futures[future]=fund
        for future in as_completed(futures):
            result=future.result()
            code=futures[future]
            result.to_excel(writer,sheet_name=code,index=None)
            print("{}数据处理完毕！".format(code))
    writer.save()
    writer.close()

if __name__ == "__main__":
    global today
    today = datetime.datetime.today()
    start_time = time.perf_counter()
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_jingzhi",default=r'G:\股市分析\数据\基金日线数据.xlsx')
    parser.add_argument("--file_src",default=r"G:\股市分析\数据\基金历史指标.xlsx")
    parser.add_argument("--file_cc", default=r"G:\股市分析\数据\基金持仓数据.xlsx")
    config=parser.parse_args(args=[])
    if os.path.exists(config.file_src) != True:
        print("source file not exists!")
        df = pd.DataFrame()
        df.to_excel(config.file_src,index=None)
    print("开始爬取！")
    df_output=multithread_crawl_data(config)
    multiprocess_analysis_data(config,df_output)
    end_time = time.perf_counter()
    print('Multithread ,plot in {} seconds'.format(end_time - start_time))

