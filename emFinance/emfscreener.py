import requests
from bs4 import BeautifulSoup
import pandas as pd
import pickle
import json
import yfinance as yf
import os
from pathlib import Path
import io
from datetime import datetime
import numpy as np

from .UsefulFuc import generalFuc
from .UsefulFuc.generalFuc import *
from .emFinanceHK import emTicker, emTickerFCF
from .emFinanceUS import emTickerUS



emfScreener_filepath = Path(__file__).parent /"emFinance_screener"
if not os.path.isdir(emfScreener_filepath):
    os.mkdir(emfScreener_filepath)


def read_pickle(filepath):
    with open(filepath, "rb") as f:
        data = pickle.load(f)
    return data

def save_pickle(data, filepath):
    with open(filepath, "wb") as f:
        pickle.dump(data, f)


class emfScreener():
    #init: check all file if file not exist in emFinance stock, parse it
    def __init__(self, stock_data_path = emfScreener_filepath):
        self.data_path = stock_data_path /"emfScreenerdata.csv"
        self.not_exist_path = stock_data_path / "emfScreenerNotExistCode.pickle"
        self.data = self.init_data()
        self.data["date_year"] = [date[:4] for date in self.data["date"]]
        self.screen_company_lst = list(set(self.data["SECUCODE"]))

        #include_none: determine whether include code without the year_data during screening
        self.include_none = True

    #simple screen, just main_indicator, roe & roic
    def init_data(self):
        try:
            datas = [pd.read_csv(self.data_path, index_col=0)]
            datacode = set(datas[0]["SECUCODE"])
        except FileNotFoundError:
            datas = []
            datacode = set()
        try:
            Not_existcode = read_pickle(self.not_exist_path)
        except FileNotFoundError:
            Not_existcode = []
            
        count = 0
        complete_code = set("0" + "0"*(4-len(str(i+1)))+ str(i+1) +".HK" for i in range(9999))
        wanted_code = complete_code ^ datacode ^ set(Not_existcode)
        
        #no need parse new data, return orignal data
        if len(wanted_code) == 0:
            data = datas[0]
            return data
        
        for code in wanted_code:
            stockcode = code[1:]         
            count += 1
            try:
                tick = emTicker(stockcode, period=[2002,2024], read = False, update = False)
                tick.main_indicator  = tick.em_get_stmt("main_indicator_yearly")
                tick.main_indicator = tick.main_indicator.reset_index(names = ["date"])
                datas.append(tick.main_indicator)
                print(stockcode)
            except TypeError:
                Not_existcode.append("0" + stockcode)
                print(stockcode + " Not exist")
            
            #save every 200 parse
            if count % 200 == 0:
                save_pickle(Not_existcode, self.not_exist_path)
                data = pd.concat(datas)
                data = data.reset_index(drop = True)
                data.to_csv(self.data_path)
        
        save_pickle(Not_existcode, self.not_exist_path)
        data = pd.concat(datas)
        data = data.reset_index(drop = True)
        data.to_csv(self.data_path)
        
        return data

    def reset_screen(self):
        self.screen_company_lst = list(set(self.data["SECUCODE"]))

    #screener in total, check specific inuput requirement 
    def screen(self, screening_parameter):
        self.screen_parameter = screening_parameter
        self.screen_table = pd.DataFrame(index = list(set(self.data["SECUCODE"])), columns = [f"{k[0]}-{k[1]}-{k[2]}" for k in screening_parameter])
        for k in screening_parameter:
            if k[0] == "value":
                self.screen_company_lst = self.value_screen(k[1],k[2],k[3],k[4])
            elif k[0] == "CAGR":
                self.screen_company_lst =  self.CAGR_screen(k[1],k[2],k[3],k[4])
            elif k[0] == "average_multiply":
                self.screen_company_lst =  self.average_multiply(k[1],k[2],k[3],k[4])
            #print(len(self.tmp_series.index), len(set(self.tmp_series.index)))
            self.screen_table[f"{k[0]}-{k[1]}-{k[2]}"] = self.tmp_series
            #print(len(self.screen_company_lst))
        self.screen_table = self.screen_table[self.screen_table.index.isin(self.screen_company_lst)]
        return self.screen_company_lst
    
    #screening_parameter: list with mutiple lists with [year, parameter, min ,max]
    def value_screen(self, parameter, year, min, max):
        tmp_data = self.data[self.data["SECUCODE"].isin(self.screen_company_lst)]
        tmp_data_company_lst = tmp_data["SECUCODE"]
        tmp_data = tmp_data[tmp_data["date_year"]==str(year)]
        if self.include_none:
            tmp_data_company_lst = set(tmp_data["SECUCODE"]) ^ set(tmp_data_company_lst)
            tmp_data = tmp_data[(tmp_data[parameter]>= min) & (tmp_data[parameter]<= max) | (tmp_data[parameter].isnull())]
            self.tmp_series = pd.Series(list(tmp_data[parameter]), index = tmp_data["SECUCODE"])
            self.tmp_series = self.tmp_series.groupby(self.tmp_series.index).first()
            self.screen_company_lst = list(set(self.screen_company_lst) & set(list(tmp_data["SECUCODE"]) + list(tmp_data_company_lst)))
        else:
            tmp_data = tmp_data[(tmp_data[parameter]>= min) & (tmp_data[parameter]<= max) | (tmp_data[parameter].isnull())]
            self.tmp_series = pd.Series(list(tmp_data[parameter]), index = tmp_data["SECUCODE"])
            self.tmp_series = self.tmp_series.groupby(self.tmp_series.index).first()
            self.screen_company_lst = list(set(self.screen_company_lst) & set(list(tmp_data["SECUCODE"])))
        return self.screen_company_lst

    #return CAGR screening
    def CAGR_screen(self, parameter, period:list, min, max):
        period = [str(n) for n in range(period[0], period[1]+1)]
        tmp_data = self.data[self.data["SECUCODE"].isin(self.screen_company_lst)]
        tmp_data = tmp_data[tmp_data["date_year"].isin(period)]
        CAGR_lst = []
        company_lst = []
        for group in tmp_data.groupby("SECUCODE"):
            company = group[0]
            main_indicator = group[1].sort_values(by="date_year", ascending=False)
            CAGR = CAGR_calculate(main_indicator[parameter], average_count=2)
            CAGR_lst.append([CAGR[k] for k in CAGR][0]*100)
            if [CAGR[k] for k in CAGR][0]*100 >= min and [CAGR[k] for k in CAGR][0]*100 <= max:
                company_lst.append(company)
                
        self.tmp_series = pd.Series(CAGR_lst, index = tmp_data["SECUCODE"])
        self.tmp_series = self.tmp_series.groupby(self.tmp_series.index).first()
        self.screen_company_lst = list(set(self.screen_company_lst) & set(company_lst))
        return self.screen_company_lst
    

    #return avervage multiply: a*b*c*d, works for YOY
    def average_multiply(self, YOYparameter, period:list, min, max):
        period = [n for n in range(period[0], period[1]+1)]
        tmp_data = self.data[self.data["SECUCODE"].isin(self.screen_company_lst)]
        average_multiply_lst = []
        company_lst = []
        for group in tmp_data.groupby("SECUCODE"):
            company = group[0]
            main_indicator = group[1]
            main_indicator.index = main_indicator["date"]
            main_indicator = slice_df(main_indicator, period[0],period[-1])
            if len(main_indicator) != len(period):
                continue
            if np.prod(main_indicator[YOYparameter]/100+1) >= min and np.prod(main_indicator[YOYparameter]/100+1) <= max:
                company_lst.append(company)
                average_multiply_lst.append(np.prod(main_indicator[YOYparameter]/100+1))

        self.tmp_series = pd.Series(average_multiply_lst, index = company_lst)
        self.tmp_series = self.tmp_series.groupby(self.tmp_series.index).first()
        self.screen_company_lst = list(set(self.screen_company_lst) & set(company_lst))
        return self.screen_company_lst



    #parse and plot all screen company using emFinanceHK, works after calling screen()
    def plot_company_data(self, screen_company_lst):
        save_path_ = Path(__file__).parent / "emFinance_plot"/ "screener"
        if not os.path.isdir(save_path_):
            os.mkdir(save_path_)
        with open(save_path_/"screener_para.json", "w") as f:
            json.dump(self.screen_parameter, f, indent=2)
        for com in screen_company_lst:
            try:
                code = com[1:]
                tick = emTickerFCF(code)
                tick.plotting_valuation(save_path = save_path_)
            except TypeError:
                print(code)


#Screener for US stock, slight change
class emfScreenerUS(emfScreener):
    def __init__(self, stock_data_path = emfScreener_filepath):
        self.data_dir = stock_data_path
        self.data_path = stock_data_path /"emfScreenerUSdata.csv"
        self.not_exist_path = stock_data_path / "emfScreenerUSNotExistCode.pickle"
        self.data = self.init_data()
        self.data["date_year"] = [date[:4] for date in self.data["date"]]
        self.screen_company_lst = list(set(self.data["SECUCODE"]))

        #include_none: determine whether include code without the year_data during screening
        self.include_none = True


    def init_data(self):
        try:
            datas = [pd.read_csv(self.data_path, index_col=0)]
            datacode = set(datas[0]["SECURITY_CODE"])
        except FileNotFoundError:
            datas = []
            datacode = set()
        try:
            Not_existcode = read_pickle(self.not_exist_path)
        except FileNotFoundError:
            Not_existcode = []
            
        count = 0
        nasdaq_code = set(pd.read_csv(self.data_dir/"nasdaq_screener_NASDAQ.csv")["Symbol"])
        nyse_code = set(pd.read_csv(self.data_dir/"nasdaq_screener_NYSE.csv")["Symbol"])
        amex_code = set(pd.read_csv(self.data_dir/"nasdaq_screener_AMEX.csv")["Symbol"])
        complete_code = nasdaq_code | nyse_code | amex_code
        wanted_code = complete_code ^ datacode ^ set(Not_existcode)
        #no need parse new data, return orignal data
        if len(wanted_code) == 0:
            data = datas[0]
            return data
        
        for stockcode in wanted_code: 
            if type(stockcode)!= str:
                continue       
            count += 1
            try:
                tick = emTickerUS(stockcode, period=[2002,2024], read = False, update = False)
                tick.main_indicator  = tick.em_get_stmt("main_indicator_yearly")
                tick.main_indicator = tick.main_indicator.reset_index(names = ["date"])
                datas.append(tick.main_indicator)
                print(stockcode)
            except TypeError:
                Not_existcode.append(stockcode)
                print(stockcode, " Not exist")
            
            #save every 200 parse
            if count % 200 == 0:
                save_pickle(Not_existcode, self.not_exist_path)
                data = pd.concat(datas)
                data = data.reset_index(drop = True)
                data.to_csv(self.data_path)
        
        save_pickle(Not_existcode, self.not_exist_path)
        data = pd.concat(datas)
        data = data.reset_index(drop = True)
        data.to_csv(self.data_path)
        
        return data