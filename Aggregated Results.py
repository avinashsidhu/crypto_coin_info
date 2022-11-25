#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from datetime import datetime
import pandas
import json
import os

pandas.get_option("display.max_columns")
api_key = "565PQIJ7QVSJA2G75UHGBQDQIU8KXZE9N8"


# In[2]:


def url(**kwargs):
    url = f"https://api.etherscan.io/api?apikey={api_key}"
    
    for key, value in kwargs.items():
        url += f"&{key}={value}"
        
    return url


# In[3]:


def eth_transactions(address):
    # Code to fetch the normal transactions
    normal_tx_url = url(module="account", action="txlist", address=address, sort="asc")
    normal_tx_response = requests.get(normal_tx_url)
    normal_tx_data = normal_tx_response.json()["result"]
    
    # Code to fetch the internal transactions
    internal_tx_url = url(module="account", action="txlistinternal", address=address, sort="asc")
    internal_tx_response = requests.get(internal_tx_url)
    internal_tx_data = internal_tx_response.json()["result"]
    
    # The data is then appended together and sorted by date
    normal_tx_data.extend(internal_tx_data)
    data = normal_tx_data
    if len(data) != 0:
        data.sort(key=lambda x: int(x['timeStamp']))

        df = pandas.DataFrame(data)
        df['timeStamp'] = df['timeStamp'].apply(lambda x: datetime.fromtimestamp(int(x)))
        df.to_csv(f'{os.getcwd()}/ETH/eth_transactions_{address}.csv')

        with open(f'{os.getcwd()}/ETH/eth_transactions_{address}.json', 'w') as f:
            json.dump(data, f, indent = 2)

    return data


# In[4]:


def erc20_transactions(address):
    # Code to fetch the ERC20 transactions
    tx_url = url(module="account", action="tokentx", address=address, sort="asc")
    tx_response = requests.get(tx_url)
    data = tx_response.json()["result"]

    data.sort(key=lambda x: int(x['timeStamp']))
    if len(data) != 0:
        df = pandas.DataFrame(data)
        df['timeStamp'] = df['timeStamp'].apply(lambda x: datetime.fromtimestamp(int(x)))
        df.to_csv(f'{os.getcwd()}/ERC/erc20_transactions_{address}.csv')

        with open(f'{os.getcwd()}/ERC/erc20_transactions_{address}.json', 'w') as f:
            json.dump(data, f, indent = 2)
    return data


# In[5]:


def get_historical_price(token, date):
    try:
        df = pandas.read_csv(f'coins/{token}-usd.csv')
    except:
        return 0
    df['snapped_at'] = pandas.to_datetime(df['snapped_at']).dt.date
    if len(df[df['snapped_at'] == date.date()]['price']) == 0: 
        return 0
    return float(df[df['snapped_at'] == date.date()]['price'])


# In[6]:


def historical_eth_balance(address, given_time):
    data = eth_transactions(address)
    times = []
    balances = []
    wealths = []
    latest_balance = 0
    for transaction in data:
        time = datetime.fromtimestamp(int(transaction['timeStamp']))
        price_at_time = get_historical_price("eth", time)
        
        if datetime.strptime(given_time, '%Y-%m-%d %H:%M:%S') < time:
            break
        
        receiver = transaction["to"]
        sender = transaction["from"]
        value = int(transaction["value"]) / (10**18)
        
        if "gasPrice" in transaction:
            gas = int(transaction["gasUsed"]) * int(transaction["gasPrice"]) / (10**18)
        else:
            gas = int(transaction["gasUsed"]) / (10**18)
            
        if receiver.lower() == address.lower():
            latest_balance += value

        else:
            latest_balance -= value + gas
        
        balances.append(latest_balance)
        wealths.append(latest_balance*price_at_time)
        times.append(str(time))
        
    return (times, balances, wealths)


# In[7]:


def historical_erc20_balance(address, symbol_list, given_time):
    data = erc20_transactions(address)
    result_dict = {}
    latest_quantity = {}
    recent_quantity = 0
    symbol_list = symbol_list
    for transaction in data:
        time = datetime.fromtimestamp(int(transaction['timeStamp']))
        if datetime.strptime(given_time, '%Y-%m-%d %H:%M:%S') < time:
            break
        
        receiver = transaction["to"]
        sender = transaction["from"]
        symbol = transaction["tokenSymbol"].lower()
        token_decimal = transaction['tokenDecimal']
        try:
            token_decimal = 10**int(token_decimal)
        except:
            token_decimal = 1
        value = int(transaction["value"]) / token_decimal
        
        if symbol in symbol_list:
            price_at_time = get_historical_price(symbol, time)
            if "gasPrice" in transaction:
                gas = int(transaction["gasUsed"]) * int(transaction["gasPrice"]) / (10**18)
            else:
                gas = int(transaction["gasUsed"]) / (10**18)

            if receiver.lower() == address.lower():
                if symbol in latest_quantity.keys():
                    latest_quantity[symbol] = latest_quantity[symbol] + value
                    recent_quantity = latest_quantity[symbol] + value
                else:
                    latest_quantity[symbol] = value
                    recent_quantity = value
            else:
                if symbol in latest_quantity.keys():
                    latest_quantity[symbol] = latest_quantity[symbol] - (value)
                    recent_quantity = latest_quantity[symbol] - (value)
                else:
                    latest_quantity[symbol]= - value
                    recent_quantity = - value
            if symbol in result_dict.keys():
                result_dict[symbol]['balance'].append(recent_quantity)
                result_dict[symbol]['wealth'].append(recent_quantity*price_at_time)
                result_dict[symbol]['time'].append(str(time))
            else:
                result_dict[symbol] = {}
                result_dict[symbol]["balance"] = []
                result_dict[symbol]["time"] = []
                result_dict[symbol]["wealth"] = []
                result_dict[symbol]["wealth"].append(recent_quantity*price_at_time)
                result_dict[symbol]["balance"].append(recent_quantity)
                result_dict[symbol]["time"].append(str(time))
    return result_dict


# In[18]:


def results(addresses):
    result_transactions = {}
    result_required = {}
    i = 1
    for address in addresses:
        data = historical_erc20_balance(address, symbol_list, "2022-11-20 00:00:00")
        eth_data = historical_eth_balance(address, "2022-11-20 00:00:00")
        data['eth'] = {}
        data['eth']['time'] = eth_data[0]
        data['eth']['balance'] = eth_data[1]
        data['eth']['wealth'] = eth_data[2]
        result_transactions[address] = data
        result_per_address = {}
        for key, val in data.items():
            if len(val['time']) != 0:
                df = pandas.DataFrame(val)
                df['time'] = pandas.to_datetime(df['time'])
                result = df.groupby([df['time'].dt.year, df['time'].dt.month]).agg({'wealth': 'max','balance': 'count'}).rename(columns = {'balance': 'Number'})
                result.index = result.index.set_names(['Year', 'Month'])
                result.reset_index(inplace = True)
                result['Time Period'] = result[['Year', 'Month']].astype(str).agg('-'.join, axis=1)
                temp = {}
                temp['Time Period'] = result['Time Period'].tolist()
                temp['Max Wealth held'] = result['wealth'].tolist()
                temp['Count of transactions'] = result['Number'].tolist()
                result_per_address[key] = temp
        result_required[address] = result_per_address
    return (result_required, result_transactions)


# In[19]:


df1 = pandas.read_csv('symbols_to_extract.csv')
symbol_list =  df1['symbol_name'].tolist()
df2 = pandas.read_csv('updated_worker_address.csv')
addresses = df2[(df2['Etherscan'] == 1)]['address'].tolist()


# In[17]:


result, data = results([addresses])


# In[ ]:




