import json
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import yfinance as yf
import os
from pathlib import Path
import io
from datetime import datetime

from .UsefulFuc import generalFuc
from .UsefulFuc.generalFuc import *
from .UsefulFuc.plotting import *


emFinance_filepath = Path(__file__).parent /"emFinance_stock"
if not os.path.isdir(emFinance_filepath):
    os.mkdir(emFinance_filepath)

emFinanceUS_reference_filepath = Path(__file__).parent /"emFinance_screener"


def get_report_link(stockcode, typ):
    
    if typ == 4: #4: main_indicator quarterly
        link = ("https://datacenter.eastmoney.com/securities/api/data/v1/get?reportName=RPT_USF10_FN_GMAININDICATOR&"
                f"columns=USF10_FN_GMAININDICATOR&quoteColumns=&filter=(SECUCODE%3D%22{stockcode}%22)"
                "(DATE_TYPE_CODE%20in%20(%22003%22%2C%22006%22%2C%22007%22%2C%22008%22))"
                "&pageNumber=1&pageSize=100&sortTypes=-1&sortColumns=REPORT_DATE&source=SECURITIES&client=PC&v=05595833636792304")
    elif typ == 5: #5: main_indicator yearly (no use, finding fiscal year date only)
        link = ("https://datacenter.eastmoney.com/securities/api/data/v1/get?reportName=RPT_USF10_FN_GMAININDICATOR&"
                f"columns=USF10_FN_GMAININDICATOR&quoteColumns=&filter=(SECUCODE%3D%22{stockcode}%22)"
                "(DATE_TYPE_CODE%3D%22001%22)&pageNumber=1&pageSize=6&"
                "sortTypes=-1&sortColumns=REPORT_DATE&source=SECURITIES&client=PC&v=044140820547875337")
    else:
        link = None
    return link


def get_code(_stockcode):
    if type(_stockcode) != str:
        print(_stockcode, " not a string")
        return "0"
    if _stockcode in list(pd.read_csv(emFinanceUS_reference_filepath/"nasdaq_screener_NASDAQ.csv")["Symbol"]):
        return _stockcode + ".O"
    elif _stockcode in list(pd.read_csv(emFinanceUS_reference_filepath/"nasdaq_screener_NYSE.csv")["Symbol"]):
        return _stockcode + ".N"
    elif _stockcode in list(pd.read_csv(emFinanceUS_reference_filepath/"nasdaq_screener_AMEX.csv")["Symbol"]):
        return _stockcode + ".A"



#currently only support main_indicator
class emTickerUS():
    def __init__(self, stockcode, period = [2002, 2024], read = True, update = False):
        #code: for hist data, yfinance
        #uscode: for other, emFinance
        self.code = stockcode
        self.uscode = get_code(stockcode)
        self.period = period
        if update:
            self.em_to_excel()
        elif read:
            self.em_read_excel()


    def em_get_stmt(self, typ): 
        
        """
        self.report_interval = get_report_interval(self.code)
        if typ == "balance_sheet" or typ == 1:
            link = get_report_link(self.code, 1, self.period, self.report_interval) #get balance_sheet
            main_indicator = False
        elif typ == "income_stmt" or typ == 2:
            link = get_report_link(self.code, 2, self.period, self.report_interval) #get income_stmt
            main_indicator = False
        elif typ == "cashflow_stmt" or typ == 3:
            link = get_report_link(self.code, 3, self.period, self.report_interval) #get cashflow_stmt
            main_indicator = False
        """
        if typ == "main_indicator_quarter" or typ == 4:
            link = get_report_link(self.uscode, 4) #get main_indicator
            main_indicator = True
        elif typ == "main_indicator_yearly" or typ == 5:
            link = get_report_link(self.uscode, 5) #get main_indicator
            main_indicator = True
        else:
            print("no stmt return from em_get_stmt")
            return 
        headers =  {r'User-Agent': 'Mozilla/5.0 (Linux; Android 5.1.1; SM-G928X Build/LMY47X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.83 Mobile Safari/537.36'}
        r = requests.get(link, headers = headers)
        soup = BeautifulSoup(r.content, 'lxml')
        data_ = json.loads(soup.html.body.p.text)["result"]["data"]
        if not main_indicator:
            columns_r = []
            index_r = []
            for dic in data_:
                columns_r.append(dic["STD_ITEM_NAME"])
                index_r.append(dic["REPORT_DATE"])

            columns_r = list(set(columns_r))
            index_r = list(set(index_r))
            df = pd.DataFrame(columns = columns_r, index = index_r)
            for dic in data_:
                df.at[dic["REPORT_DATE"], dic["STD_ITEM_NAME"]] = dic["AMOUNT"]
        else:
            df_lst = []
            for dic in data_:
                df_f = pd.DataFrame.from_dict(dic, orient = "index")
                df_lst.append(df_f)

            df = pd.concat(df_lst, axis = 1)
            df = df.T
            df.index = df["REPORT_DATE"]
            df = df.drop(columns = ["REPORT_DATE"])

        df.index = pd.to_datetime(df.index).tz_localize('Asia/Hong_Kong').tz_localize(None)

        return df.sort_index(ascending = False)
    

    def em_get_hist_price(self, period = "max", interval = "1mo"):
        #follow what is on yfinance
        csfyf = yf.Ticker(self.code)
        csf_hist = csfyf.history(period, interval)
        csf_share = csfyf.fast_info["shares"]
        csf_hist.index = csf_hist.index.tz_localize(None)
        csf_hist["share"] = csf_share 
        csf_hist["market_cap"] = csf_hist["share"] *csf_hist["Close"]
        self.hist_price = csf_hist.sort_index(ascending = False)

        try:
            csf_hist.index = pd.to_datetime(csf_hist.index).tz_convert('Asia/Hong_Kong').tz_localize(None)
        except TypeError:
            csf_hist.index = pd.to_datetime(csf_hist.index).tz_localize('Asia/Hong_Kong').tz_localize(None)

        return csf_hist.sort_index(ascending = False)
    

    def em_to_excel(self, path = emFinance_filepath):
        #create a 4 sheet excel, 1.historic price, 2.main indicator (eps, roe,etc), 3.balance_sheet, 4.income_stmt, 5.cashflow_stmt
        self.hist_price = self.em_get_hist_price()
        self.main_indicator = self.em_get_stmt(typ = "main_indicator_quarter")
        self.a_main_indicator = self.em_get_stmt(typ = "main_indicator_yearly")
        self.fiscal_year = self.a_main_indicator.index[0].strftime('%Y-%m-%d %X')[5:10]

        with pd.ExcelWriter(path / f'{self.code} {self.period[0]}-{self.period[1]}.xlsx') as writer:
            self.hist_price.to_excel(writer, sheet_name = "price_history")
            self.main_indicator.to_excel(writer, sheet_name = "main_indicator")
            self.a_main_indicator.to_excel(writer, sheet_name = "a_main_indicator")
        return
    
    def em_read_excel(self, path = emFinance_filepath):
        #retract whatever saved by the em_to_excel()
        excel_file = path / f'{self.code} {self.period[0]}-{self.period[1]}.xlsx'
        try:
            self.hist_price = pd.read_excel(excel_file, sheet_name = "price_history", index_col = 0)
            self.main_indicator = pd.read_excel(excel_file, sheet_name = "main_indicator", index_col = 0)
            self.a_main_indicator = pd.read_excel(excel_file, sheet_name = "a_main_indicator", index_col = 0)
            self.fiscal_year = self.a_main_indicator.index[0].strftime('%Y-%m-%d %X')[5:10]
            self.name = self.a_main_indicator["SECURITY_NAME_ABBR"].iloc[0]

            #print(self.a_combine_stmt.index[-1], self.hist_price.index[-1])
        except FileNotFoundError:
            print(f"{self.code} no data saved yet, to_excel automatically")
            self.em_to_excel()
            #print(self.combine_stmt.index[-1], self.hist_price.index[-1])
        return 