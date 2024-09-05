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

#make directory for saving data if not exist:
emFinance_filepath = Path(__file__).parent /"emFinance_stock"
if not os.path.isdir(emFinance_filepath):
    os.mkdir(emFinance_filepath)

def get_report_date(start, end, report_interval):
    #start = 2022, end = ?
    dates = ""
    for n in range(int(start)- int(end)+1):
        for q in range(len(report_interval)):
            if n == 0 and q == 0:
                dates+= f"%27{start-n}-{report_interval[q]}%27"
            else:
                dates+= f"%2C%27{start-n}-{report_interval[q]}%27"
    return dates


def get_report_link(stockcode, typ, period:list  = [], report_interval:list = []):
    #1: balance_sheet, 2:income_stmt, 3:cashflow_stmt
    #only support hk stock code currently, e.g stockcode = 0883.HK
    
    stockcode = "0" + stockcode

    if typ == 1:
        reportdate = get_report_date(period[1],period[0], report_interval)
        link = ("https://datacenter.eastmoney.com/securities/api/data/v1/get?reportName=RPT_HKF10_FN_BALANCE_PC&"
            "columns=SECUCODE%2CSECURITY_CODE%2CSECURITY_NAME_ABBR%2CORG_CODE%2CREPORT_DATE%2CDATE_TYPE_CODE%2CFISCAL_YEAR"
            f"%2CSTD_ITEM_CODE%2CSTD_ITEM_NAME%2CAMOUNT%2CSTD_REPORT_DATE&quoteColumns=&filter=(SECUCODE%3D%22{stockcode}%22)"
            f"(REPORT_DATE%20in%20({reportdate}))&"
            "pageNumber=1&pageSize=&sortTypes=-1%2C1&sortColumns=REPORT_DATE%2CSTD_ITEM_CODE&source=F10&client=PC&v=09198984768151934")
    elif typ == 2:
        reportdate = get_report_date(period[1],period[0], report_interval)
        link = ("https://datacenter.eastmoney.com/securities/api/data/v1/get?reportName=RPT_HKF10_FN_INCOME_PC&"
                "columns=SECUCODE%2CSECURITY_CODE%2CSECURITY_NAME_ABBR%2CORG_CODE%2CREPORT_DATE%2CDATE_TYPE_CODE%2CFISCAL_YEAR"
                f"%2CSTART_DATE%2CSTD_ITEM_CODE%2CSTD_ITEM_NAME%2CAMOUNT&quoteColumns=&filter=(SECUCODE%3D%22{stockcode}%22)"
                f"(REPORT_DATE%20in%20({reportdate}))&"
                "pageNumber=1&pageSize=&sortTypes=-1%2C1&sortColumns=REPORT_DATE%2CSTD_ITEM_CODE&source=F10&client=PC&v=08958254802158732")
    elif typ == 3:
        reportdate = get_report_date(period[1],period[0], report_interval)
        link = ("https://datacenter.eastmoney.com/securities/api/data/v1/get?reportName=RPT_HKF10_FN_CASHFLOW_PC&"
                "columns=SECUCODE%2CSECURITY_CODE%2CSECURITY_NAME_ABBR%2CORG_CODE%2CREPORT_DATE%2CDATE_TYPE_CODE%2CFISCAL_YEAR"
                f"%2CSTART_DATE%2CSTD_ITEM_CODE%2CSTD_ITEM_NAME%2CAMOUNT&quoteColumns=&filter=(SECUCODE%3D%22{stockcode}%22)"
                f"(REPORT_DATE%20in%20({reportdate}))&"
                "pageNumber=1&pageSize=&sortTypes=-1%2C1&sortColumns=REPORT_DATE%2CSTD_ITEM_CODE&source=F10&client=PC&v=04744763967365846")
    elif typ == 4: #4: main_indicator quarterly
        link = ("https://datacenter.eastmoney.com/securities/api/data/v1/get?reportName=RPT_HKF10_FN_MAININDICATOR&"
                f"columns=HKF10_FN_MAININDICATOR&quoteColumns=&filter=(SECUCODE%3D%22{stockcode}%22)"
                "&pageNumber=1&pageSize=100&sortTypes=-1&sortColumns=STD_REPORT_DATE&"
                "source=F10&client=PC&v=09761950614251711")
    elif typ == 5: #5: main_indicator yearly (no use, finding fiscal year date only)
        link = ("https://datacenter.eastmoney.com/securities/api/data/v1/get?reportName=RPT_HKF10_FN_MAININDICATOR&"
                f"columns=HKF10_FN_MAININDICATOR&quoteColumns=&filter=(SECUCODE%3D%22{stockcode}%22)"
                "(DATE_TYPE_CODE%3D%22001%22)&pageNumber=1&pageSize=9&sortTypes=-1&sortColumns=STD_REPORT_DATE&"
                "source=F10&client=PC&v=09761950614251711")
    else:
        link = None
    return link


def get_report_interval(stockcode):
    stockcode = "0" + stockcode
    link = ("https://datacenter.eastmoney.com/securities/api/data/v1/get?reportName=RPT_HKF10_FN_MAININDICATOR&"
                f"columns=HKF10_FN_MAININDICATOR&quoteColumns=&filter=(SECUCODE%3D%22{stockcode}%22)"
                "&pageNumber=1&pageSize=9&sortTypes=-1&sortColumns=STD_REPORT_DATE&"
                "source=F10&client=PC&v=09761950614251711")
    headers =  {r'User-Agent': 'Mozilla/5.0 (Linux; Android 5.1.1; SM-G928X Build/LMY47X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.83 Mobile Safari/537.36'}
    r = requests.get(link, headers = headers)
    soup = BeautifulSoup(r.content, 'lxml')
    data_ = json.loads(soup.html.body.p.text)["result"]["data"]

    df = pd.DataFrame()
    for dic in data_:
        df_f = pd.DataFrame.from_dict(dic, orient = "index")
        df = pd.concat([df, df_f], axis = 1)

    df = df.T
    df.index = df["REPORT_DATE"]
    df = df.drop(columns = ["REPORT_DATE"])
    return list(set([str(n)[5:10] for n in df.index]))


class emTicker(): #eastmoney Ticker
    
    def __init__(self, stockcode, period = [2002, 2024], read = True, update = False):
        self.code = stockcode
        self.period = period
        if update:
            self.em_to_excel()
        elif read:
            self.em_read_excel()


    def em_get_stmt(self, typ): 
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
        elif typ == "main_indicator_quarter" or typ == 4:
            link = get_report_link(self.code, 4, self.period, self.report_interval) #get main_indicator
            main_indicator = True
        elif typ == "main_indicator_yearly" or typ == 5:
            link = get_report_link(self.code, 5, self.period, self.report_interval) #get main_indicator
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


    def em_get_callput_ratio(self):
        #can only return 1 year data
        if self.code == r"^HSI":
            #HSI warrant = ALL
            code = "ALL"
        else:
            code = "0" + self.code[:-3]

        today_date = datetime.today()
        end_date = f'{today_date.year}/{today_date.month}/{today_date.day}'
        start_date = f'{today_date.year-1}/{today_date.month}/{today_date.day}'

        link = (f"https://www.hkex.com.hk/chi/sorc/market_data/statistics_putcall_ratio_c.aspx?action=ajax&type=getCSV&"
                f"ucode={code}&date_form={start_date}&date_to={end_date}&page=1")
        headers =  {r'User-Agent': 'Mozilla/5.0 (Linux; Android 5.1.1; SM-G928X Build/LMY47X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.83 Mobile Safari/537.36'}
        r = requests.get(link, headers = headers)
        soup = BeautifulSoup(r.content, 'lxml')
        try:
            self.callput_ratio = pd.read_csv(io.StringIO(soup.html.body.p.text), header = 1, index_col = 0)
            self.callput_ratio.index = [pd.Timestamp(datetime.strptime(c, "%d/%m/%Y").strftime("%Y-%m-%d")) for c in self.callput_ratio.index]
        except AttributeError:
            print(f"This company {self.code} has no warrant!")
            self.callput_ratio = None
        return self.callput_ratio

    

    def em_to_excel(self, path = emFinance_filepath):
        #create a 4 sheet excel, 1.historic price, 2.main indicator (eps, roe,etc), 3.balance_sheet, 4.income_stmt, 5.cashflow_stmt
        self.hist_price = self.em_get_hist_price()
        self.main_indicator = self.em_get_stmt(typ = "main_indicator_quarter")
        self.a_main_indicator = self.em_get_stmt(typ = "main_indicator_yearly")
        self.fiscal_year = self.a_main_indicator.index[0].strftime('%Y-%m-%d %X')[5:10]
        self.name = self.a_main_indicator["SECURITY_NAME_ABBR"].iloc[0]
        self.balance_sheet = self.em_get_stmt(typ = "balance_sheet")
        self.cashflow_stmt = self.em_get_stmt(typ = "cashflow_stmt")
        self.income_stmt = self.em_get_stmt(typ = "income_stmt")
        self.combine_stmt = pd.concat([self.balance_sheet, self.income_stmt, self.cashflow_stmt] ,axis =1)

        #a = annual
        self.a_balance_sheet = pd.DataFrame([self.balance_sheet.loc[ind] for ind in self.balance_sheet.index if ind.strftime('%Y-%m-%d %X')[5:10] == self.fiscal_year])
        self.a_income_stmt = pd.DataFrame([self.income_stmt.loc[ind] for ind in self.income_stmt.index if ind.strftime('%Y-%m-%d %X')[5:10] == self.fiscal_year])
        self.a_cashflow_stmt = pd.DataFrame([self.cashflow_stmt.loc[ind] for ind in self.cashflow_stmt.index if ind.strftime('%Y-%m-%d %X')[5:10] == self.fiscal_year])
        self.a_combine_stmt = pd.concat([self.a_balance_sheet, self.a_income_stmt, self.a_cashflow_stmt] ,axis =1)

        with pd.ExcelWriter(path / f'{self.code} {self.period[0]}-{self.period[1]}.xlsx') as writer:
            self.hist_price.to_excel(writer, sheet_name = "price_history")
            self.main_indicator.to_excel(writer, sheet_name = "main_indicator")
            self.a_main_indicator.to_excel(writer, sheet_name = "a_main_indicator")
            self.balance_sheet.to_excel(writer, sheet_name = "balance_sheet")
            self.income_stmt.to_excel(writer, sheet_name = "income_stmt")
            self.cashflow_stmt.to_excel(writer, sheet_name = "cashflow_stmt")
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
            self.balance_sheet = pd.read_excel(excel_file, sheet_name = "balance_sheet", index_col = 0)
            self.income_stmt = pd.read_excel(excel_file, sheet_name = "income_stmt", index_col = 0)
            self.cashflow_stmt = pd.read_excel(excel_file, sheet_name = "cashflow_stmt", index_col = 0)
            self.combine_stmt = pd.concat([self.balance_sheet, self.income_stmt, self.cashflow_stmt] ,axis =1)
            
            #a = annual
            self.a_balance_sheet = pd.DataFrame([self.balance_sheet.loc[ind] for ind in self.balance_sheet.index if ind.strftime('%Y-%m-%d %X')[5:10] == self.fiscal_year])
            self.a_income_stmt = pd.DataFrame([self.income_stmt.loc[ind] for ind in self.income_stmt.index if ind.strftime('%Y-%m-%d %X')[5:10] == self.fiscal_year])
            self.a_cashflow_stmt = pd.DataFrame([self.cashflow_stmt.loc[ind] for ind in self.cashflow_stmt.index if ind.strftime('%Y-%m-%d %X')[5:10] == self.fiscal_year])
            self.a_combine_stmt = pd.concat([self.a_balance_sheet, self.a_income_stmt, self.a_cashflow_stmt] ,axis =1)
            #print(self.a_combine_stmt.index[-1], self.hist_price.index[-1])
        except FileNotFoundError:
            print(f"{self.code} no data saved yet, to_excel automatically")
            self.em_to_excel()
            #print(self.combine_stmt.index[-1], self.hist_price.index[-1])
        return 
    


#FCF valuation for emTicker
class emTickerFCF(emTicker):
    def __init__(self, *args, **kwargs):
        super(emTickerFCF, self).__init__(*args, **kwargs)


    #init stock for analysis, usually for old stock
    def value_init(self):
        #method
        #1: is dividend tempting?
        self.pe_dividend = get_pe_dividend(self.hist_price, self.a_main_indicator)

        #2: FCF measurement
        self.cashflow_stmt["cum_operating cashflow"] = cumulated_df(self.cashflow_stmt["经营业务现金净额"], self.fiscal_year, inc_display= False)
        self.cashflow_stmt["cum_investing cashflow"] = cumulated_df(self.cashflow_stmt["投资业务现金净额"], self.fiscal_year, inc_display= False)
        self.cashflow_stmt["cum_financing cashflow"] = cumulated_df(self.cashflow_stmt["融资业务现金净额"], self.fiscal_year, inc_display= False)
        self.cashflow_stmt["free_cashflow"] = self.cashflow_stmt["cum_operating cashflow"] + self.cashflow_stmt["cum_investing cashflow"]
        self.cashflow_stmt["net_free_cashflow"]  = self.cashflow_stmt["cum_operating cashflow"] + self.cashflow_stmt["cum_investing cashflow"] + self.cashflow_stmt["cum_financing cashflow"]

        #3: safety measurement: earning plot, debt plot, roe plot
        self.main_indicator["EPS"] = self.main_indicator['BASIC_EPS']
        self.main_indicator["CUM_EPS"] = cumulated_df(self.main_indicator['BASIC_EPS'], self.fiscal_year).iloc[:,0]
        self.main_indicator["EPS_INC"] = cumulated_df(self.main_indicator['BASIC_EPS'], self.fiscal_year).iloc[:,1]

        self.balance_sheet["bank_debt"] = self.balance_sheet[[n for n in self.balance_sheet.columns if "贷款" in n]].sum(axis=1)


    #fcf valuation
    def fcf_valuation(self):
        #find debt ratio
        self.balance_sheet["debt_ratio"] = self.balance_sheet['总负债']/ (self.balance_sheet['总权益'] + self.balance_sheet['总负债'])
        self.balance_sheet["average_debt"] = average_df(self.combine_stmt['总负债'],2)
        paras = ['加:利息支出']  + [col for col in self.cashflow_stmt if "付利息" in col]

        #cost of debt, wacc_debt
        self.balance_sheet['cost_of_debt'] = cumulated_df(self.cashflow_stmt[paras], self.fiscal_year, False).sum(axis=1) / self.balance_sheet["average_debt"]
        self.balance_sheet['wacc_debt'] = self.balance_sheet["debt_ratio"] * self.balance_sheet['cost_of_debt'] * (1-0.25)

        #cost of equity, wacc_equity
        #cost of equity formula = current_debt_ratio / average_debt_ratio past 5-10 years * alpha + risk free return
        self.balance_sheet['cost_of_equity'] = self.balance_sheet["debt_ratio"].to_frame() /  average_df(self.balance_sheet["debt_ratio"], 20) * 0.03 + 0.06
        self.balance_sheet['wacc_equity'] = (1- self.balance_sheet["debt_ratio"]) * self.balance_sheet['cost_of_equity']
        
        dividend_align = unify_df_index([self.pe_dividend, self.balance_sheet], mode = "max")
                                            
        self.balance_sheet['wacc'] = self.balance_sheet['wacc_equity'] + self.balance_sheet['wacc_debt']
        self.roic_table = pd.DataFrame(index = self.balance_sheet['wacc'].index)
        self.roic_table["cost_of_equity"] = self.balance_sheet['cost_of_equity']
        self.roic_table["ROE"] = self.main_indicator["ROE_YEARLY"]/100
        self.roic_table["wacc"] = self.balance_sheet['wacc']
        self.roic_table["ROIC"] = self.main_indicator["ROIC_YEARLY"]/100
        #ROIC - dividend - wacc_debt = return of equity after debt and dividend
        self.roic_table["ROIC - dividend - wacc_debt"] = (self.roic_table["ROIC"] - cumulated_df(self.cashflow_stmt['已付股息(融资)'], self.fiscal_year, False)/ \
                                                          (self.balance_sheet['总权益'] + self.balance_sheet['总负债']) \
                                                          - self.balance_sheet['wacc_debt']) / (1- self.balance_sheet["debt_ratio"])
        self.roic_table["dividend%"] = dividend_align["dividend%"]


    #plot dcf
    def get_dcf(self, fcf_inc = None, average_count = 8, dcf_duration = 10):

        def fcf_valuation(wacc, fcf, fcf_inc, duration = dcf_duration):
            #calculate discount free cash flow of furture (duration) years
            def sigma_fcf(old_value = 0, start = 0, end = duration):
                value = old_value + fcf*((1+fcf_inc) / (1+wacc))**(start)
                if start-1 == end:
                    return value
                else:
                    return sigma_fcf(value, start+1)
                
            dfcf = sigma_fcf()
            return dfcf
        
        self.CAGR_cashflow = CAGR_calculate(self.cashflow_stmt["free_cashflow"], average_count)
        
        self.cashflow_stmt["free_cashflow_av8"] = average_df(self.cashflow_stmt["free_cashflow"], average_count)
        if fcf_inc == None:
            fcf_inc = [self.CAGR_cashflow[k] for k in self.CAGR_cashflow][0]
        self.dfcf = fcf_valuation(self.roic_table["wacc"], self.cashflow_stmt["free_cashflow_av8"], fcf_inc)


    def plotting_valuation(self, save_path = Path(__file__).parent /"emFinance_plot"):
        self.value_init()
        self.balance_sheet["inventory"] = self.balance_sheet[[n for n in self.balance_sheet.columns if '存货' in n]].sum(axis = 1)
        self.balance_sheet["total_equity"] = self.balance_sheet[[n for n in self.balance_sheet.columns if '总权益' in n]].sum(axis = 1)
        preplot_df = pd.concat([ self.pe_dividend[["PE","dividend","EPS/dividend rate"]],
                        self.main_indicator[["EPS","CUM_EPS","EPS_INC"]],
                        self.balance_sheet[["inventory","bank_debt","total_equity"]],
                        self.cashflow_stmt[["cum_operating cashflow", "cum_investing cashflow", "cum_financing cashflow"]],
                        self.cashflow_stmt[["free_cashflow","net_free_cashflow"]],  self.main_indicator['DEBT_ASSET_RATIO']], axis = 1)
        plot_df_histprice_bar(preplot_df, self.hist_price, main_title=f"{self.code}-{self.name}", save_path=save_path/f"{self.code}-{self.name}.png")

