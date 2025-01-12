#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 29 12:02:13 2024

@author: shaiyaan
"""

import os
import cx_Oracle as ora
import pandas as pd
import numpy as np

# SQL Queries
rates_data = """
SELECT <your_condition_here>;
FROM <your_condition_here>;
WHERE <your_condition_here>;
"""

fwd_data_1m = """
SELECT <your_condition_here>;
FROM <your_condition_here>;
WHERE <your_condition_here>;
"""

int_rate = """
SELECT <your_condition_here>;
FROM <your_condition_here>;
"""

currencies = ['EUR', 'GBP', 'AUD', 'NZD', 'USD', 'CHF', 'CAD', 'NOK', 'SEK', 'JPY']

top_currency_pairs_num = "" # used for your sharpe ratio strategy
bottom_currency_pairs_num = "" # used for your sharpe ratio strategy - 1


# Functions
def ora_conn(user_nm, passwd):  # Connect to Oracle
    return ora.connect(
        f"your password here"
    )

def get_sql_data(string):
    user_nm = os.environ.get('your_username')
    passwd = os.environ.get('your password')
    conn = ora_conn(user_nm, passwd)
    curr = conn.cursor()
    curr.execute(string)
    data = curr.fetchall()
    df = pd.DataFrame(data)
    return df

def find_missing_pairs(df):
    df.columns = ['record_id', 'date', 'base_ccy', 'quote_ccy', 'contract', 'exchange_rate']
    calculated_missing_pairs = []
    grouped_data = df.groupby('date')

    for date, group in grouped_data:
        exchange_rate = {(row['base_ccy'], row['quote_ccy']): row['exchange_rate'] for idx, row in group.iterrows()}
        reverse_rate = {(row['quote_ccy'], row['base_ccy']): 1 / row['exchange_rate'] for idx, row in group.iterrows()}
        exchange_rate.update(reverse_rate)
        
        for base in currencies:
            for quote in currencies:
                if base != quote and (base, quote) not in exchange_rate:
                    if (base, 'USD') in exchange_rate and (quote, 'USD') in exchange_rate:
                        cross_rate = exchange_rate[(base, 'USD')] / exchange_rate[(quote, 'USD')]
                        calculated_missing_pairs.append([date, base, quote, cross_rate])

    return calculated_missing_pairs

def merge_n_clean(df, rates):
    missing_rates_df = pd.DataFrame(rates, columns=['date', 'base_ccy', 'quote_ccy', 'exchange_rate'])
    full_rates_df = pd.concat([df, missing_rates_df])
    full_rates_df = full_rates_df.sort_values(by=['date', 'base_ccy', 'quote_ccy', 'exchange_rate'])
    return full_rates_df

def reorder_rates(row):
    base, quote = row['base_ccy'], row['quote_ccy']
    rate = row['exchange_rate']
    if currencies.index(base) > currencies.index(quote):
        return quote, base, 1 / rate
    else:
        return base, quote, rate

def fix_order(df):
    df[['ordered_base', 'ordered_quote', 'rate']] = df.apply(
        reorder_rates, axis=1, result_type='expand'
    )
    data_standard = df[['date', 'ordered_base', 'ordered_quote', 'rate']]
    data_standard = data_standard.drop_duplicates(subset=['date', 'ordered_base', 'ordered_quote', 'rate'])
    data_standard.columns = ['date', 'base_ccy', 'quote_ccy', 'exchange_rate']
    return data_standard

def calc_cum_returns(daily_returns):
    cumulat_returns = 0
    for daily_return in daily_returns:
        cumulat_returns = (1 + cumulat_returns) * (1 + daily_return) - 1
    return cumulat_returns

def calc_monthly_returns(df, first_day):
    monthly_factor_returns = []
    for month in first_day['Month'].unique():
        tops = first_day[(first_day['Month'] == month) & (first_day['Rank'] <= top_currency_pairs_num)][['base_ccy', 'quote_ccy']]
        bottoms = first_day[(first_day['Month'] == month) & (first_day['Rank'] >= (first_day['Rank'].max() - bottom_currency_pairs_num))][['base_ccy', 'quote_ccy']]
        
        top_returns = df[(df['Month'] == month) & (df[['base_ccy', 'quote_ccy']].apply(tuple, axis=1).isin(tops.apply(tuple, axis=1)))]
        bottom_returns = df[(df['Month'] == month) & (df[['base_ccy', 'quote_ccy']].apply(tuple, axis=1).isin(bottoms.apply(tuple, axis=1)))]
        
        top_return = top_returns.groupby(['base_ccy', 'quote_ccy', 'Month'], group_keys=False)['ccy_return'].apply(calc_cum_returns).mean()
        bottom_return = bottom_returns.groupby(['base_ccy', 'quote_ccy', 'Month'], group_keys=False)['ccy_return'].apply(calc_cum_returns).mean()
        
        factor_return = top_return - bottom_return
        monthly_factor_returns.append(factor_return)

    return monthly_factor_returns

# Main Execution
df_spot = get_sql_data(rates_data) # pulling spot rates
calculated_rates = find_missing_pairs(df_spot)
complete_data_df = merge_n_clean(df_spot, calculated_rates)
spot_df = fix_order(complete_data_df)
spot_df.tail()

fwd_rate = get_sql_data(fwd_data_1m) # pulling one month forward rates
missed_rates = find_missing_pairs(fwd_rate)
filled_data_df = merge_n_clean(fwd_rate, missed_rates)
fwd_table = fix_order(filled_data_df).rename(columns={'exchange_rate': 'fwd_rate'})
fwd_table.tail()

df_1month = get_sql_data(int_rate) # USD interest rate data
df_1month.columns=['date', 'ccy', 'value']
df_1month.tail()

merge_df = spot_df.merge(fwd_table, on=['date', 'base_ccy', 'quote_ccy'], how='outer')
merge_df = merge_df.merge(df_1month, on=['date'], how='outer')

merge_df["IMPLIED_INTEREST_RATE_1M"] = merge_df["fwd_rate"] / merge_df["exchange_rate"] * (merge_df["exchange_rate"] + 1) - 1
merge_df = merge_df[merge_df['date'] > '2003-01-01']

new_merge_df = merge_df.drop_duplicates(['date', 'base_ccy', 'quote_ccy', 'exchange_rate', 'fwd_rate'], keep='first')
new_merge_df = new_merge_df.sort_values(by=['base_ccy', 'quote_ccy', 'date'])
new_merge_df['previous_spot_rate'] = new_merge_df.groupby(['base_ccy', 'quote_ccy'])['exchange_rate'].shift(1)
new_merge_df['ccy_return'] = (new_merge_df['exchange_rate'] - new_merge_df['previous_spot_rate']) / new_merge_df['previous_spot_rate']

new_merge_df = new_merge_df.dropna(subset=['fwd_rate', 'IMPLIED_INTEREST_RATE_1M'])
new_merge_df = new_merge_df[new_merge_df['ccy_return'] != 0]
new_merge_df['Month'] = new_merge_df['date'].dt.to_period('M')

final_data = new_merge_df[['date', 'Month', 'base_ccy', 'quote_ccy', 'ccy_return', 'IMPLIED_INTEREST_RATE_1M']].copy()
final_data = final_data.rename(columns={'IMPLIED_INTEREST_RATE_1M': 'interest_rate_diff'})

first_day = final_data.drop_duplicates(subset=['base_ccy', 'quote_ccy', 'Month'], keep='first').copy()
first_day['Rank'] = first_day.groupby('Month')['interest_rate_diff'].rank(ascending=False)

monthly_factor_returns = calc_monthly_returns(final_data, first_day)

factor_return_df = pd.DataFrame({'Month': first_day['Month'].unique(), 'Factor Return': monthly_factor_returns})

annualized_return = factor_return_df['Factor Return'].mean() * 12
annualized_volatility = factor_return_df['Factor Return'].std() * np.sqrt(12)
sharpe_ratio = annualized_return / annualized_volatility

print(f"Annualized Return is: {annualized_return}")
print(f"Annualized Volatility is: {annualized_volatility}")
print(f"The Sharpe Ratio is: {sharpe_ratio}")
