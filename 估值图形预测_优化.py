import requests
import os
import re
import pandas as pd
import json
import xlrd
import datetime
import matplotlib.pyplot as plt
import numpy as np
import argparse
import time
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor,as_completed,wait
plt.rcParams['font.family'] = ['SimHei']
plt.rcParams['axes.unicode_minus']=False

pd.set_option('display.max_columns', None)

# def get_fund_code(fund):
#     '''获取基金code'''
#     result = re.search("\d{6}", fund)
#     if result == None:
#         print("{}中未包含基金代码，请修正".format(fund))
#         return None
#     else:
#         return result.group()
#
# def get_fund_code_excel(file,fund):
#     try:
#         df=pd.read_excel(file,sheet_name=fund,dtype={"代码":str})
#     except xlrd.biffh.XLRDError:
#         df=pd.DataFrame()
#     return df

def get_fund_code_value(file,fund):
    try:
        df = pd.read_excel(file, sheet_name=fund, dtype={"代码": str})
    except xlrd.biffh.XLRDError:
        df = pd.DataFrame()
    code = df[df["代码"].notnull()]["代码"][0]
    url='http://fundgz.1234567.com.cn/js/{}.js'.format(code)
    df_result=pd.DataFrame()
    r=requests.get(url)
    value_gusuan=re.search('"gsz":"(\d+\.\d+)?"',r.text)
    date=re.search('"gztime":"(\d+-\d+-\d+)+\s',r.text)
    value_danwei=re.search('"dwjz":"(\d+\.\d+)?"',r.text)
    if value_gusuan != None:
        date=date.group(1)
        value_danwei=float(value_danwei.group(1))
        value_gusuan=float(value_gusuan.group(1))
        df_result=pd.DataFrame(columns=["净值日期","单位净值",'累计净值',"代码","最高价", "最低价", "RSV", "K值", "D值", "J值", "ema12", "ema26", "diff", "dea", "macd"],dtype=float)
        df_result.loc[0,"净值日期"] = date
        df_result.loc[0,"单位净值"] = value_danwei
        df_result.loc[0,'累计净值'] = round(df.iloc[-1]["累计净值"]*value_gusuan/value_danwei,4)
        df_result.loc[0,"代码"] = str(code)
        df_result.loc[0, ["最高价", "最低价", "RSV", "K值", "D值", "J值", "ema12", "ema26", "diff", "dea", "macd"]] = (
        None, None, None, None, None, None, None, None, None, None, None)
        print("{}数据已获取".format(fund))
        df_fund = df.append(df_result,ignore_index=True,sort=True)
        # df_fund.sort_values("净值日期",inplace=True)
        df_fund = df_fund.drop_duplicates('净值日期')
        df_fund.index = range(len(df_fund))
        return df_fund
    else:
        print("{}无估值数据".format(fund))
        return df

def data_analysis(df,fund):
    for i in df[df["ema12"].isna()].index.tolist():
        if i == 0:
            df.loc[i, "最高价"] = None
            df.loc[i, "最低价"] = None
            df.loc[i, "RSV"] = None
            df.loc[i, "K值"] = None
            df.loc[i, "D值"] = None
            df.loc[i, "J值"] = None
            df.loc[i, "ema12"] = df.loc[i, "累计净值"]
            df.loc[i, "ema26"] = df.loc[i, "累计净值"]
            df.loc[i, "diff"] = df.loc[i, "ema12"] - df.loc[i, "ema26"]
            df.loc[i, "dea"] = df.loc[i, "diff"]
            df.loc[i, "macd"] = (df.loc[i, "diff"] - df.loc[i, "dea"]) * 2
        elif (i >0) & (i < 8):
            df.loc[i, "最高价"] = None
            df.loc[i, "最低价"] = None
            df.loc[i, "RSV"] = None
            df.loc[i, "K值"] = None
            df.loc[i, "D值"] = None
            df.loc[i, "J值"] = None
            df.loc[i, "ema12"] = df.loc[i, "累计净值"] * 2 / 13 + 11 / 13 * df.loc[i - 1, "ema12"]
            df.loc[i, "ema26"] = df.loc[i, "累计净值"] * 2 / 27 + 25 / 27 * df.loc[i - 1, "ema26"]
            df.loc[i, "diff"] = df.loc[i, "ema12"] - df.loc[i, "ema26"]
            df.loc[i, "dea"] = df.loc[i, "diff"] * 2 / 10 + 8 / 10 * df.loc[i - 1, "dea"]
            df.loc[i, "macd"] =(df.loc[i, "diff"] - df.loc[i, "dea"]) * 2
        else:
            df.loc[i, "最高价"] = df.loc[i - 9: i, "累计净值"].max()
            df.loc[i, "最低价"] = df.loc[i - 9: i, "累计净值"].min()
            df.loc[i, "RSV"] = 100 if (df.loc[i, "最高价"] - df.loc[i, "最低价"]) == 0 else 100 * (df.loc[i, "累计净值"] - df.loc[i, "最低价"]) / (df.loc[i, "最高价"] - df.loc[i, "最低价"])
            df.loc[i, "K值"] = 2 / 3 * df.loc[i - 1, "K值"] + 1 / 3 * df.loc[i, "RSV"] if df.loc[i - 1, "K值"] != None else 2 / 3 * 50 + 1 / 3 * df.loc[ i, "RSV"]
            df.loc[i, "D值"] = 2 / 3 * df.loc[i - 1, "D值"] + 1 / 3 * df.loc[i, "K值"] if df.loc[i - 1, "D值"] != None else 2 / 3 * 50 + 1 / 3 * df.loc[i, "K值"]
            df.loc[i, "J值"] = 3 * df.loc[i, "K值"] - 2 * df.loc[i, "D值"]
            df.loc[i, "ema12"] = df.loc[i, "累计净值"] * 2 / 13 + 11 / 13 * df.loc[i - 1, "ema12"]
            df.loc[i, "ema26"] = df.loc[i, "累计净值"] * 2 / 27 + 25 / 27 * df.loc[i - 1, "ema26"]
            df.loc[i, "diff"] = df.loc[i, "ema12"] - df.loc[i, "ema26"]
            df.loc[i, "dea"] = df.loc[i, "diff"] * 2 / 10 + 8 / 10 * df.loc[i - 1, "dea"]
            df.loc[i, "macd"] = (df.loc[i, "diff"] - df.loc[i, "dea"]) * 2
        df["boll_mid"] = df["累计净值"].rolling(20).mean()
        df["boll_std"] = df["累计净值"].rolling(20).std()
        df["boll_top"] = df["boll_mid"] + 2 * df["boll_std"]
        df["boll_bottom"] = df["boll_mid"] - 2 * df['boll_std']
    print("{}数据处理完毕！".format(fund))
    return df

def plot_graph(df,fund,last_days=100):
    df_plot = df[-last_days:]
    df_plot = df_plot[df_plot.notnull().all(axis=1)]
    plt.figure(figsize=(10,10))
    plt.subplot(3, 1, 1)
    df_plot["累计净值"].plot(color="k", lw=2.5, label="净值")
    df_plot['boll_mid'].plot(color='b', lw=1., legend=True)
    df_plot['boll_top'].plot(color='r', lw=1., legend=True)
    df_plot['boll_bottom'].plot(color='g', lw=1., legend=True)
    plt.legend()
    plt.text(df_plot.iloc[-1].name,df_plot.iloc[-1]["累计净值"], "{}:\n{}".format(df_plot.iloc[-1]["净值日期"],df_plot.iloc[-1]["累计净值"]),color="red",fontsize=6)
    plt.xticks(df_plot.index[::3], df_plot["净值日期"][::3].tolist(), rotation=45,fontsize=7)
    plt.title("{}估值预测".format(fund))
    plt.subplot(3,1,2)
    df_plot["K值"].plot(color="orange",label="K值")
    df_plot["D值"].plot(color="purple",label="D值")
    df_plot["J值"].plot(color="black",label="J值")
    plt.legend(fontsize=7,loc="upper left")   #添加图例
    plt.xticks([])
    floor_line = df_plot[['K值', 'D值', 'J值']].min().min()
    plt.text(df_plot.iloc[0].name, floor_line, "K值：{:4.2f}".format(df_plot.iloc[-1]["K值"]), ha='center',color="orange",fontsize=8)  #最新一天数据标注
    plt.text(df_plot.iloc[5].name, floor_line, "D值：{:4.2f}".format(df_plot.iloc[-1]["D值"]), ha='center', color="purple", fontsize=8)  #最新一天数据标注
    plt.text(df_plot.iloc[10].name, floor_line, "J值：{:4.2f}".format(df_plot.iloc[-1]["J值"]), ha='center', color="black", fontsize=8)  #最新一天数据标注
    plt.axhline(y=20, color='green', linestyle='-')  #画20的横线
    plt.axhline(y=80, color='red', linestyle='-')   #画80的横线
    plt.axhline(y=50, color='gray', linestyle='-')
    plt.subplot(3, 1, 3)
    df_plot["diff"].plot(color="brown", label="diff")
    df_plot["dea"].plot(color="blue", label="dea")
    # for index in df[-last_days:].index:
    #     if df.loc[index,"macd"]>0:
    #         plt.bar(df.index,df["macd"],color="green")
    #     else:
    #         plt.bar(df.index, df["macd"],color="red")
    for index, row in df_plot.iterrows():
        if (row['macd'] > 0):  # 大于0则用红色
            plt.bar(index, row['macd'], width=0.5, color='red')
        else:  # 小于等于0则用绿色
            plt.bar(index, row['macd'], width=0.5, color='green')
    plt.xticks([])
    # plt.xticks(df_plot.index[::3],df_plot["净值日期"][::3].tolist(),rotation=45,fontsize=7)
    plt.legend(fontsize=7)
    plt.tight_layout()
    plt.savefig("./数据/估值预测/{}估值分析指标".format(fund),bbox_inches='tight',dpi=200)
    print("{}数据绘制完毕！".format(fund))
    plt.close()

def multithread_crawl_data(config):
    with open(config.funds,mode="r") as wp:
        funds=wp.readlines()
    with ThreadPoolExecutor() as executor_multhread:
        crawl_task={}
        data_tmp={}
        for fund in funds:
            fund=fund.replace("\n","")
            future=executor_multhread.submit(get_fund_code_value,(config.file),(fund))
            crawl_task[future]=fund
        for future in as_completed(crawl_task):
            result=future.result()
            fund_key=crawl_task[future]
            data_tmp[fund_key]=result
    return data_tmp

def multiprocess_data(data,func):
    with ProcessPoolExecutor() as pool:
        tasks=[]
        for fund,df in data.items():
            future=pool.submit(func,(df),(fund))
            tasks.append(future)
        wait(tasks)

if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--file",default=r'G:\股市分析\数据\基金日线数据.xlsx')
    parser.add_argument("--funds",default=r"G:\股市分析\funds.txt")
    config=parser.parse_args(args=[])
    start_time = time.perf_counter()
    data_tmp = multithread_crawl_data(config)
    multiprocess_data(data_tmp,data_analysis)
    multiprocess_data(data_tmp, plot_graph)
    end_time = time.perf_counter()
    print('Program takes {} seconds'.format(end_time - start_time))
