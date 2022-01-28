import pandas as pd
import requests
import datetime
import re
import json
import argparse
pd.set_option('display.max_columns', None)

year=(datetime.datetime.today()-datetime.timedelta(120)).year

def get_fund_code(fund):
    '''获取基金code'''
    result = re.search("\d{6}", fund)
    if result == None:
        print("{}中未包含基金代码，请修正".format(fund))
        return None
    else:
        return result.group()

def get_fund_cc(fund_code):
    url="https://fundf10.eastmoney.com/FundArchivesDatas.aspx"
    params = {
        'type': 'jjcc'
        ,'code': fund_code
        ,'topline': '20'
        ,'year': year}
    r=requests.get(url,params=params)
    # a=re.findall("</a><a href='//quote.eastmoney.com/(.*?)\.html' >行情",r.text)
    tree=etree.HTML(r.text)
    table=tree.xpath("//table")[0]
    code_list=table.xpath("./tbody/tr/td[2]//text()")
    df_code=pd.read_html(r.text)[0]
    df_code["股票代码"]=code_list
    df_code["代码"]=df_code["股票代码"].apply(lambda x:'hk'+x if len(x)==5 else 'sh'+x if x.startswith('6') else 'sz'+x)
    df_code["日期"]=datetime.datetime.today().strftime("%Y-%m-%d")
    df_code["基金"]=fund_code
    return df_code

# def get_quote_pe_pb(quote_code):
#     '''根据股票代码从亿牛网爬取滚动pe/pb'''
#     if quote_code.lower().startswith("hk"):
#         r_pe = requests.get("https://eniu.com/chart/peh/{}".format(quote_code))
#         r_pb = requests.get("https://eniu.com/chart/pbh/{}".format(quote_code))
#     else:
#         r_pe = requests.get("https://eniu.com/chart/pea/{}/t/60".format(quote_code))
#         r_pb = requests.get("https://eniu.com/chart/pba/{}/t/60".format(quote_code))
#     dict1=json.loads(r_pe.text)
#     dict2=json.loads(r_pb.text)
#     df_pe_pb_1 = pd.DataFrame(dict1)
#     df_pe_pb_2 = pd.DataFrame(dict2)
#     df_pe_pb=pd.merge(left=df_pe_pb_1,right=df_pe_pb_2,on="date",how="inner")
#     df_pe_pb["code"]=quote_code
#     return df_pe_pb


def main():
    with open(config.funds,mode="r") as wp:
        funds=wp.readlines()
    funds_dict={}
    for fund in funds:
        fund=fund.replace("\n","")
        funds_dict[fund]=get_fund_code(fund)  #装载成字典
    writer_cc = pd.ExcelWriter("./数据/基金持仓数据.xlsx")
    for fund in funds_dict:
        fund_code = funds_dict[fund]
        df=get_fund_cc(fund_code)
        df.to_excel(writer_cc,sheet_name=fund,index=None)
        print("{}持仓爬取完毕".format(fund))
    writer_cc.save()
    writer_cc.close()

if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument("--funds",default=r"G:\股市分析\funds.txt")
    config = parser.parse_args(args=[])
    main()
