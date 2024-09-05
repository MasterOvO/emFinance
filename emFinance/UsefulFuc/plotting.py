import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from math import ceil


#plot paras agisnt price individually
#need to pre-treat with slice_df
def plot_df_histprice_bar(df_paras:pd.DataFrame, hist_price:pd.DataFrame,count_col = 3, main_title = None, save_path = None, show = False):
    #prepare processed df_paras & hist_price first
    count = len(df_paras.axes[1])
    count_row = ceil(count/count_col)
    fig, axs = plt.subplots(ncols = count_col, nrows = count_row, layout='constrained', figsize=(3.5 * 4, 3.5 * ceil(count_row)))
    for i in range(count_row):
        for j in range(count_col):
            v = j+i+((count_col-1) *i)
            try:
                if count_row == 1:
                    df_paras.index = pd.to_datetime(df_paras.index)
                    axs[j].bar(df_paras.index, df_paras.iloc[:,v], width = 100, color=(0,0,0,0.4))
                    axs[j].set_title(df_paras.columns[v])
                    ax2 = axs[j].twinx()
                    ax2.plot(hist_price.index, hist_price["Close"])
                else:
                    df_paras.index = pd.to_datetime(df_paras.index)
                    axs[i,j].bar(df_paras.index, df_paras.iloc[:,v], width = 100, color=(0,0,0,0.4))
                    axs[i,j].set_title(df_paras.columns[v])
                    ax2 = axs[i,j].twinx()
                    ax2.plot(hist_price.index, hist_price["Close"])
            except IndexError:
                pass
    plt.xticks(rotation = 45)
    if main_title != None:
        fig.suptitle(main_title)
    if save_path !=None:
        plt.savefig(save_path)
    if show:
        plt.show()
    else:
        plt.close()
    return 


def format_e(n):
    return n.split('0')[0]