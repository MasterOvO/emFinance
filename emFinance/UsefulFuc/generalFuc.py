import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from math import ceil
from scipy.optimize import fsolve


#slicing df with according to year
#usable for all statement type
def slice_df(df:pd.DataFrame, start_year, end_year = 2023):
    df = check_df(df)
    df.index = pd.to_datetime(df.index)
    n_list = []
    for n, ind in enumerate(df.index):
        if int(str(ind)[:4]) < start_year or int(str(ind)[:4]) > end_year:
            pass
        else:
            n_list.append(n)
    new_df = df.iloc[n_list]
    new_df = new_df.sort_index(ascending= False)
    return new_df


#CAGR, but take average within (year_average) years
#df_paras: all columns in the df will be calculated
def CAGR_calculate(df:pd.DataFrame, average_count = 5):
    df = check_df(df)
    CAGR_dict = {}
    df = average_df(df, average_count)
    for col in df.columns:
        if df[col].iloc[0]>=0  and df[col].iloc[-1]>=0:
            CAGR_rate = (df[col].iloc[0]/df[col].iloc[-1])**(1/len(df))-1
        else:
            CAGR_rate = np.nan
        CAGR_dict[f"CAGR-from {str(df.index[-1])[:4]}-{str(df.index[0])[:4]}   {col}"] = CAGR_rate
    return CAGR_dict


#calculate average value base on average_count
#return avg_df with length-average_count+1
def average_df(df: pd.DataFrame, average_count):
    df = check_df(df)
    avg_df = pd.DataFrame(index = df.index[:-average_count+1])
    for col in df.columns:
        shift_df = df[col]
        combine_lst = [shift_df]
        for n in range(average_count-1):
            shift_df = shift_df.shift(-1)
            combine_lst.append(shift_df)
        combine_df = pd.concat(combine_lst, axis = 1)
        
        avg_df[col] = combine_df.mean(axis = 1)

    return avg_df



#get cumulated_df with quarterly_data
def cumulated_df(df_quarter: pd.DataFrame, fiscal_year, inc_display = True):
    df = check_df(df_quarter)
    all_df = pd.DataFrame(index = df.index)
    #cumulative para with quarterly
    for col in df.columns:
        all_df[f"CUM_{col}"] = np.nan
        all_df[f"INC_{col}"] = np.nan
        fiscal_year_lst = {k:df.iloc[-1][col] for k in set(str(l)[5:10] for l in df.index)}
        df = df[::-1]
        for ind in df.index:
            try:
                if str(ind)[5:10] == fiscal_year:
                    all_df.at[ind, f"INC_{col}"] = (df.at[ind, col] - fiscal_year_lst[fiscal_year]) / fiscal_year_lst[fiscal_year]
                    fiscal_year_lst[fiscal_year] = df.at[ind, col]
                    all_df.at[ind, f"CUM_{col}"] = fiscal_year_lst[fiscal_year]
                else:
                    current_para = fiscal_year_lst[fiscal_year] - fiscal_year_lst[str(ind)[5:10]] + df.at[ind, col]
                    para_inc = (df.at[ind, col]- fiscal_year_lst[str(ind)[5:10]]) / fiscal_year_lst[str(ind)[5:10]]
                    all_df.at[ind, f"INC_{col}"] = para_inc
                    all_df.at[ind, f"CUM_{col}"] = current_para
                    fiscal_year_lst[str(ind)[5:10]] = df.at[ind, col]
            except TypeError:
                #print("TypeError")
                all_df.at[ind, f"CUM_{col}"] = 0
                all_df.at[ind, f"INC_{col}"] = 0
        if not inc_display:
            if len(df.columns)== 1:
                return all_df[f"CUM_{col}"]
            all_df  = all_df.drop(columns= [f"INC_{col}"])
            
    
    return all_df


#getting pe_dividend table yearly
def get_pe_dividend(use_hist:pd.DataFrame, main_indicator:pd.DataFrame):
    use_hist.index = pd.to_datetime(use_hist.index)
    main_indicator.index = pd.to_datetime(main_indicator.index)
    main_indicator = main_indicator.fillna(value= np.nan)
    use_hist = use_hist.sort_index(ascending=True)
    main_indicator = main_indicator.sort_index(ascending = True)
    pe_d = pd.DataFrame(index = ["Price", "market_cap", "PE", "EPS", "dividend", "dividend%", "EPS/dividend rate"])
    n = 0
    for i, ind in enumerate(main_indicator.index):
        for j, ind_p in enumerate(use_hist.index):
            if ind_p >= ind:
                price = use_hist["Close"].iloc[j]
                market_cap = use_hist["market_cap"].iloc[j]
                pe = use_hist["Close"].iloc[j]/main_indicator["BASIC_EPS"].iloc[i]
                eps =  main_indicator["BASIC_EPS"].iloc[i]
                dividend = np.sum(use_hist["Dividends"].iloc[n:j])
                dividend_percent = np.sum(use_hist["Dividends"].iloc[n:j])/use_hist["Close"].iloc[j]
                ed_rate = dividend/ eps
                n = j
                pe_d[main_indicator.index[i]] = [price, market_cap, pe, eps, dividend, dividend_percent, ed_rate]
                break
    pe_d  = pe_d.T
    return pe_d.sort_index(ascending= False)


def unify_df_index(dfs: tuple, mode: "max" or "min" or 0):
    #max: return combine df in most index 
    #min: return combine df in least index
    #0 : return combine df according to the first df index
    for df in dfs:
        try:
            df.index = pd.to_datetime(df.index).tz_convert('Asia/Hong_Kong')
        except TypeError:
            #print(f"Can't convert {df.index[0]}")
            df.index = pd.to_datetime(df.index).tz_localize('Asia/Hong_Kong')

    if mode == "max":
        target_df_len = (np.max([len(df.index) for df in dfs]))
    elif mode == "min":
        target_df_len = (np.min([len(df.index) for df in dfs]))
    elif mode == 0:
        target_df_len = len(dfs[0].index)
    
    target_df = [df for df in dfs if len(df.index) == target_df_len][0]
    unify_df = pd.concat(dfs).sort_index(ascending=True).ffill().loc[target_df.index]
    unify_df["date"] = unify_df.index
    unify_df = unify_df.drop_duplicates("date", keep= "last")
    unify_df = unify_df.drop(columns = ["date"])
    unify_df.index.name = None
            
    return unify_df.sort_index(ascending = False)


#calculate MACD & signal line
def get_MACD(tick_hist, short_long_period = [12, 26]):
    tick_hist = tick_hist.sort_index(ascending = True)
    EMA_short = tick_hist["Close"].ewm(span=short_long_period[0], adjust=False).mean()
    EMA_long = tick_hist["Close"].ewm(span=short_long_period[1], adjust=False).mean()
    tick_hist["MACD"] = EMA_short - EMA_long
    tick_hist["signal_line"] = tick_hist["MACD"].ewm(span=9, adjust=False).mean()
    return tick_hist.sort_index(ascending = False)

#calculate RSI
def get_RSI(tick_hist, period=14):
    tick_hist = tick_hist.sort_index(ascending = True)
    delta = tick_hist["Close"].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    tick_hist["RSI"] = rsi
    return tick_hist.sort_index(ascending = False)


#internal function, in case df is a series rather than df
def check_df(df):
    if len(df.shape) == 1: #if df is a series
        return df.to_frame()
    else:
        return df
    


