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

def get_fund_code(fund):
    '''获取基金code'''
    result = re.search("\d{6}", fund)
    if result == None:
        print("{}中未包含基金代码，请修正".format(fund))
        return None
    else:
        return result.group()

def get_fund_code_excel(file,fund):
    try:
        df=pd.read_excel(file,sheet_name=fund)
    except xlrd.biffh.XLRDError:
        df=pd.DataFrame()
    return df

def get_fund_code_data(file,fund_key,fund_code):
    try:
        df_file=pd.read_excel(file,sheet_name=fund_key)
    except xlrd.biffh.XLRDError:
        df_file = pd.DataFrame()
    sdate = "2015-01-01" if df_file.empty else df_file["净值日期"].max()
    edate = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    i=1
    url='http://fund.eastmoney.com/f10/F10DataApi.aspx'
    df_result=pd.DataFrame()
    while True:
        params = {
            'type': 'lsjz'
            , 'per': '20'
            , 'sdate': sdate
            , 'edate': edate
            , 'code': fund_code
            , 'page': i
        }
        r=requests.get(url,params=params)
        df=pd.read_html(r.text)
        if i ==1 :
            pages=re.search("pages:(\d+)",r.text)
            if pages==None:
                break;
            else:
                pages=int(pages.group(1))
        df_result=df_result.append(df,ignore_index=True,sort=True)
        # print("{}爬取到{}/{}页".format(fund,i,pages))
        if i == pages:
            break
        else:
            i+=1
    result = df_result.loc[df_result["累计净值"].notnull(),['净值日期','单位净值','累计净值']]
    print("{}数据爬取完毕,爬取数据{}条".format(fund_key,len(result)))
    result["代码"] = str(fund_code)
    result[["最高价", "最低价", "RSV", "K值", "D值", "J值", "ema12", "ema26", "diff", "dea", "macd"]] = result.apply( \
        lambda x: (None, None, None, None, None, None, None, None, None, None, None), axis=1, result_type='expand')
    df_fund = result.append(df_file,ignore_index=True,sort=True)
    df_fund = df_fund.sort_values("净值日期")
    df_fund = df_fund.drop_duplicates('净值日期')
    df_fund.index = range(len(df_fund))
    # df_fund.to_excel(writer, sheet_name=fund_key, index=None)
    return df_fund

def data_analysis(fund,df):
    fund_key=fund
    df=df
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
        # df.loc[i,"label"]=df
    # df.to_excel(writer,sheet_name=fund_key,index=None)
    print("{}数据处理完毕!".format(fund_key))
    return df

# def divergences(df_plot):
#     max_jz = df_plot["累计净值"].max()
#     min_jz = df_plot["累计净值"].min()
#     max_macd = df_plot["macd"].max()
#     min_macd = df_plot["macd"].min()
#     if (df_plot.tail(1)["累计净值"] == max_jz) & (df_plot.tail(1)["macd"] != max_macd)


def plot_graph(df,fund,last_days=100):
    if len(df) > 20:
        df_plot = df[-last_days:]
        df_plot = df_plot[df_plot.notnull().all(axis=1)]
        # label=df_plot.apply()
        plt.figure(figsize=(10,10))
        plt.subplot(3, 1, 1)
        df_plot["累计净值"].plot(color="k",lw=2.5,label="净值")
        df_plot['boll_mid'].plot(color='b', lw=1., legend=True)
        df_plot['boll_top'].plot(color='r', lw=1., legend=True)
        df_plot['boll_bottom'].plot(color='g', lw=1., legend=True)
        plt.legend()
        plt.text(df_plot.iloc[-1].name,df_plot.iloc[-1]["累计净值"], "{}:\n{}".format(df_plot.iloc[-1]["净值日期"],df_plot.iloc[-1]["累计净值"]),color="black",fontsize=8)
        plt.xticks(df_plot.index[::3], df_plot["净值日期"][::3].tolist(), rotation=45,fontsize=7)
        plt.title("{}".format(fund))
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
        plt.savefig("./数据/{}日线分析指标".format(fund),bbox_inches='tight',dpi=200)
        plt.close()
        print("{}日线图绘制完毕".format(fund))
    else:
        print("{}线数据太少，无法绘图".format(fund))

def remove_png_file(dirname):
    files=os.listdir(dirname)
    for file in files:
        if file.endswith("png"):
            os.remove(os.path.join(dirname,file))
            print("已删除{}".format(file))

def multithread_crawl_data(config):
    file_dirname = os.path.dirname(config.file)
    remove_png_file(file_dirname)
    with open(config.funds,mode="r") as wp:
        funds=wp.readlines()
    funds_dict={}
    for fund in funds:
        fund=fund.replace("\n","")
        funds_dict[fund]=get_fund_code(fund)  #装载成字典
    if os.path.exists(config.file) != True:
        print("source file not exists!")
        df = pd.DataFrame()
        df.to_excel(config.file)
    with ThreadPoolExecutor() as executor_multhread:
        crawl_task={}
        data_tmp={}
        for key, value in funds_dict.items():
            future=executor_multhread.submit(get_fund_code_data,(config.file),(key),(value))
            crawl_task[future]=key
        for future in as_completed(crawl_task):
            df_fund=future.result()
            fund_key=crawl_task[future]
            data_tmp[fund_key]=df_fund
        # writer.save()
        # writer.close()
    return data_tmp

def multiprocess_analysis_data(data):
    with ProcessPoolExecutor() as pool:#必须放在__name__=="__main__"里面
        tasks={}
        data_tmp={}
        for fund,df in list(data.items()):
            future = pool.submit(data_analysis,(fund),(df))
            tasks[future]=fund
        for future in as_completed(tasks):
            df_fund=future.result()
            fund=tasks[future]
            df_fund.to_excel(writer,sheet_name=fund,index=None) #此处用到多进程，不能在多进程下的函数中同时使用writer(多线程可以)，不然会出错。
            data_tmp[fund]=(df_fund)
    return data_tmp

def multiprocess_plot_data(data):
    with ProcessPoolExecutor() as pool:
        tasks=[]
        for fund,df in data.items():
            future=pool.submit(plot_graph,(df),(fund))
            tasks.append(future)
        wait(tasks)


if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--file",default=r'G:\股市分析\数据\基金日线数据.xlsx')
    parser.add_argument("--funds",default=r"G:\股市分析\funds.txt")
    parser.add_argument("--figpath", default=r"G:\股市分析\数据")
    config=parser.parse_args(args=[])
    start_time = time.perf_counter()
    data_tmp=multithread_crawl_data(config)
    writer=pd.ExcelWriter(config.file)
    data_plot=multiprocess_analysis_data(data_tmp)
    writer.save()
    writer.close()
    multiprocess_plot_data(data_plot)
    end_time = time.perf_counter()
    print('Multithread,plot  in {} seconds'.format(end_time - start_time))