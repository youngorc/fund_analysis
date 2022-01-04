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

# def get_df_for_level(df,level):
#     if level=="周" or level.lower() =="week":
#         df_level=df.groupby(['周索引']).agg({"净值日期":max,"累计净值":[max,min,lambda x:x.tolist()[-1]]}).reset_index()
#         df_level.columns=["维度索引","净值日期","最大值","最小值","收盘价"]
#     else:
#         df_level=df.groupby(['月索引']).agg({"净值日期":max,"累计净值":[max,min,lambda x:x.tolist()[-1]]}).reset_index()
#         df_level.columns=["维度索引","净值日期","最大值","最小值","收盘价"]
#     return df_level
# def data_create_index(df_src,fund):
#     df_src.sort_values('净值日期', inplace=True)
#     df_src["周信息"]=df_src["净值日期"].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d").isocalendar()[:2])
#     df_src["周几"]=df_src["净值日期"].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d").isocalendar()[-1])
#     df_src["周索引"]=df_src["周信息"].apply(lambda x:df_src["周信息"].unique().tolist().index(x))
#     df_src["月信息"]=df_src["净值日期"].str.replace("-","").str[:6]
#     df_src["月索引"]=df_src["月信息"].apply(lambda x:df_src["月信息"].unique().tolist().index(x))
#     return df_src


def get_data_result(file,fund,level):
    print("开始处理{}{}线数据!".format(fund,level))
    df_src = pd.read_excel(file, sheet_name=fund, dtype={"代码": str})
    df_src.sort_values('净值日期', inplace=True)
    df_src["周信息"]=df_src["净值日期"].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d").isocalendar()[:2])
    df_src["周几"]=df_src["净值日期"].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d").isocalendar()[-1])
    df_src["周索引"]=df_src["周信息"].apply(lambda x:df_src["周信息"].unique().tolist().index(x))
    df_src["月信息"]=df_src["净值日期"].str.replace("-","").str[:6]
    df_src["月索引"]=df_src["月信息"].apply(lambda x:df_src["月信息"].unique().tolist().index(x))
    if level=="周" or level.lower() =="week":
        df=df_src.groupby(['周索引']).agg({"净值日期":max,"累计净值":[max,min,lambda x:x.tolist()[-1]]}).reset_index()
        df.columns=["维度索引","净值日期","最大值","最小值","收盘价"]
    else:
        df=df_src.groupby(['月索引']).agg({"净值日期":max,"累计净值":[max,min,lambda x:x.tolist()[-1]]}).reset_index()
        df.columns=["维度索引","净值日期","最大值","最小值","收盘价"]
    df[["最高价","最低价","RSV", "K值", "D值", "J值", "ema12", "ema26", "diff", "dea", "macd"]] = df.apply(lambda x: (None, None,None, None, None, None, None, None, None, None, None), axis=1, result_type='expand')
    for i in df[df["ema12"].isna()].index.tolist():
        if i == 0:
            df.loc[i, "最高价"] = None
            df.loc[i, "最低价"] = None
            df.loc[i, "RSV"] = None
            df.loc[i, "K值"] = None
            df.loc[i, "D值"] = None
            df.loc[i, "J值"] = None
            df.loc[i, "ema12"] = df.loc[i, "收盘价"]
            df.loc[i, "ema26"] = df.loc[i, "收盘价"]
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
            df.loc[i, "ema12"] = df.loc[i, "收盘价"] * 2 / 13 + 11 / 13 * df.loc[i - 1, "ema12"]
            df.loc[i, "ema26"] = df.loc[i, "收盘价"] * 2 / 27 + 25 / 27 * df.loc[i - 1, "ema26"]
            df.loc[i, "diff"] = df.loc[i, "ema12"] - df.loc[i, "ema26"]
            df.loc[i, "dea"] = df.loc[i, "diff"] * 2 / 10 + 8 / 10 * df.loc[i - 1, "dea"]
            df.loc[i, "macd"] =(df.loc[i, "diff"] - df.loc[i, "dea"]) * 2
        else:
            df.loc[i, "最高价"] = df.loc[i - 9: i, "最大值"].max()
            df.loc[i, "最低价"] = df.loc[i - 9: i, "最小值"].min()
            df.loc[i, "RSV"] = 100 * (df.loc[i, "收盘价"] - df.loc[i, "最低价"]) / (df.loc[i, "最高价"] - df.loc[i, "最低价"])
            df.loc[i, "K值"] = 2 / 3 * df.loc[i - 1, "K值"] + 1 / 3 * df.loc[i, "RSV"] if df.loc[i - 1, "K值"] !=None else 2 / 3 * 50 + 1 / 3 * df.loc[ i, "RSV"]
            df.loc[i, "D值"] = 2 / 3 * df.loc[i - 1, "D值"] + 1 / 3 * df.loc[i, "K值"] if df.loc[i - 1, "D值"] !=None else 2 / 3 * 50 + 1 / 3 * df.loc[i, "K值"]
            df.loc[i, "J值"] = 3 * df.loc[i, "K值"] - 2 * df.loc[i, "D值"]
            df.loc[i, "ema12"] = df.loc[i, "收盘价"] * 2 / 13 + 11 / 13 * df.loc[i - 1, "ema12"]
            df.loc[i, "ema26"] = df.loc[i, "收盘价"] * 2 / 27 + 25 / 27 * df.loc[i - 1, "ema26"]
            df.loc[i, "diff"] = df.loc[i, "ema12"] - df.loc[i, "ema26"]
            df.loc[i, "dea"] = df.loc[i, "diff"] * 2 / 10 + 8 / 10 * df.loc[i - 1, "dea"]
            df.loc[i, "macd"] = (df.loc[i, "diff"] - df.loc[i, "dea"]) * 2
        df["boll_mid"] = df["收盘价"].rolling(20).mean()
        df["boll_std"] = df["收盘价"].rolling(20).std()
        df["boll_top"] = df["boll_mid"] + 2 * df["boll_std"]
        df["boll_bottom"] = df["boll_mid"] - 2 * df['boll_std']
    print("{}基金{}线数据处理完毕!".format(fund,level))
    return df[['维度索引','净值日期','最大值','最小值','收盘价','最高价','最低价','RSV','K值','D值','J值','ema12'\
        ,'ema26','diff','dea','macd','boll_mid','boll_std','boll_top','boll_bottom']]

def plot_graph(df,fund,level,last_units=80):
    if len(df) > 20:
        df_plot = df[-last_units:]
        df_plot = df_plot[df_plot.notnull().all(axis=1)]
        plt.figure(figsize=(10,10))
        plt.subplot(3, 1, 1)
        df_plot["收盘价"].plot(color="k",lw=2.5,label="净值")
        df_plot['boll_mid'].plot(color='b', lw=1., legend=True)
        df_plot['boll_top'].plot(color='r', lw=1., legend=True)
        df_plot['boll_bottom'].plot(color='g', lw=1., legend=True)
        plt.legend()
        plt.text(df_plot.iloc[-1].name,df_plot.iloc[-1]["收盘价"], "{}:\n{}".format(df_plot.iloc[-1]["净值日期"],df_plot.iloc[-1]["收盘价"]),color="black",fontsize=8)
        plt.xticks(df_plot.index[::2], df_plot["净值日期"][::2].tolist(), rotation=45,fontsize=7)
        plt.title("{}<{}>".format(fund,level))
        plt.subplot(3,1,2)
        df_plot["K值"].plot(color="orange",label="K值")
        df_plot["D值"].plot(color="purple",label="D值")
        df_plot["J值"].plot(color="black",label="J值")
        plt.legend(fontsize=7,loc="upper left")  #添加图例
        plt.xticks([])
        # plt.xticks(df_plot.index[::3],df_plot["净值日期"][::3].tolist(),rotation=45,fontsize=7)
        floor_line = df_plot[['K值', 'D值', 'J值']].min().min()
        plt.text(df_plot.iloc[0].name, floor_line+15, "K值：{:4.2f}".format(df_plot.iloc[-1]["K值"]), ha='center',color="orange",fontsize=8)  #最新一天数据标注
        plt.text(df_plot.iloc[0].name, floor_line+7, "D值：{:4.2f}".format(df_plot.iloc[-1]["D值"]), ha='center', color="purple", fontsize=8)  #最新一天数据标注
        plt.text(df_plot.iloc[0].name, floor_line, "J值：{:4.2f}".format(df_plot.iloc[-1]["J值"]), ha='center', color="black", fontsize=8)  #最新一天数据标注
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
        plt.savefig("./数据/{}{}线分析指标".format(fund,level),bbox_inches='tight',dpi=200)
        plt.close()
        print("{}{}线分析指标绘制完成".format(fund, level))
    else:
        print("{}{}线数据太少，无法绘图".format(fund,level))

def multiprocess_analysis_data(config,writer,level):
    with open(config.funds,mode="r") as wp:
        funds=wp.readlines()
    with ProcessPoolExecutor() as pool:
        tasks1={}
        data={}
        for fund in funds:
            fund = fund.replace("\n", "")
            future1=pool.submit(get_data_result,(config.file),(fund),(level))
            tasks1[future1]=fund
        for future1 in as_completed(tasks1):
            fund1 = tasks1[future1]
            df_fund1=future1.result()
            data[fund1] = df_fund1
            df_fund1.to_excel(writer,sheet_name=fund1,index=None)
    return data

def multiprocess_plot_data(data,level):
    with ProcessPoolExecutor() as pool:
        tasks = []
        for fund, df in data.items():
            fund = fund.replace("\n", "")
            future = pool.submit(plot_graph, (df), (fund),(level))
            tasks.append(future)
        wait(tasks)

    # df_week = get_df_for_level(df, level="周")
    # df_month = get_df_for_level(df, level="月")
    # df_week_result=get_data_result(df_week)
    # df_month_result=get_data_result(df_month)
    # print("{}数据处理完毕".format(fund))
    # plot_graph(df_week_result, fund , level="周")
    # plot_graph(df_month_result, fund, level="月")
    # return df_week_result,df_month_result

if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--file",default=r'G:\股市分析\数据\基金日线数据.xlsx')
    parser.add_argument("--funds",default=r"G:\股市分析\funds.txt")
    parser.add_argument("--figpath", default=r"G:\股市分析\数据")
    config=parser.parse_args(args=[])
    start_time = time.perf_counter()
    writer_week = pd.ExcelWriter(r"./数据/基金周线数据.xlsx")
    writer_month = pd.ExcelWriter(r"./数据/基金月线数据.xlsx")
    data_week = multiprocess_analysis_data(config,writer_week,"周")
    data_month = multiprocess_analysis_data(config, writer_week, "月")
    writer_week.save()
    writer_week.close()
    writer_month.save()
    writer_month.close()

    multiprocess_plot_data(data_week,"周")
    multiprocess_plot_data(data_month, "月")
    end_time = time.perf_counter()
    print('Multithread,plot  in {} seconds'.format(end_time - start_time))
