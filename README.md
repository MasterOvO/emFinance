# emFinance
Python scraping hk &amp; us stock from eastmoney finance



example

from emFinance.emFinanceHK import emTicker

#HK stock code 0001.HK

tick = emTicker("0001.HK")

print(tick.balance_sheet)

print(tick.income_stmt)

print(tick.cashflow_stmt)
