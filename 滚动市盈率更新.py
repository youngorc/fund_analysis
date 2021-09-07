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
        df=pd.DataFrame(columns=["date","fund","持仓","累计净值","pe_ttm","pb"])
    return df

def get_fund_code_data(df):
    return (today-datetime.timedelta(120)).strftime("%Y-%m-%d") if pd.isna(df["date"].max()) else df["date"].max()

def plot_graph(df_fund,fund,last_days=250):
    period=3
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
        fig, axes = plt.subplots(2,1)
        axe1 = axes[0].twinx()
        # xticks=[i for i in df_plot["date"][::3]]
        axes[0].plot(df_plot["date"][::period],df_plot["累计净值"][::period],color="k",lw=1.,label="累计净值")
        axe1.plot(df_plot["date"][::period],df_plot["pe_ttm"][::period],color='orange', lw=1., label="pe_ttm")
        axes[0].set_xticklabels(df_plot["date"][::period],rotation=45,fontsize=7)
        axe1.text(df_plot.iloc[-1].date, df_plot.iloc[-1].pe_ttm,
                 "{:4.2f}".format(df_plot.iloc[-1]["pe_ttm"]), color="black", fontsize=8)
        # axes[0].legend()
        axe1.legend()
        axe1.axhline(y=pe_line_25, color='green',lw=0.5, linestyle='--')
        axe1.axhline(y=pe_line_50, color='grey', lw=0.5, linestyle='--')
        axe1.axhline(y=pe_line_75, color='red', lw=0.5, linestyle='--')
        #第二幅图
        axe2 = axes[1].twinx()
        axes[1].plot(df_plot["date"][::period],df_plot["累计净值"][::period],color="k",lw=1.,label="累计净值")
        axe2.plot(df_plot["date"][::period],df_plot["pb"][::period],color='orange', lw=1., label="pb")
        axes[1].set_xticklabels(df_plot["date"][::period],rotation=45,fontsize=7)
        axe2.text(df_plot.iloc[-1].date, df_plot.iloc[-1].pb,
                 "{:4.2f}".format(df_plot.iloc[-1]["pb"]), color="black", fontsize=8)
        # axes[1].legend()
        axe2.axhline(y=pb_line_25, color='green',lw=0.5, linestyle='--')
        axe2.axhline(y=pb_line_50, color='grey', lw=0.5, linestyle='--')
        axe2.axhline(y=pb_line_75, color='red', lw=0.5, linestyle='--')
        axe2.legend()
        plt.tight_layout()
        plt.savefig("./数据/{}估值分析指标".format(fund),bbox_inches='tight',dpi=200)
        plt.close()
    else:
        print("{}线数据太少，无法绘图".format(fund))



def main():
    global today
    today = datetime.datetime.today()
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
                fund_current_guzhi["持仓"]=" ;".join((df["股票名称"]+","+df["占净值比例"]).tolist())
                fund_history_pe = fund_history_pe.append(fund_current_guzhi[["date", "fund", "持仓","累计净值","pe_ttm","pb"]],ignore_index=True)
                plot_graph(fund_history_pe,fund)
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

if __name__ == "__main__":
    main()