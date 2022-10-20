import requests
import json
import pandas as pd
from pycoingecko import CoinGeckoAPI
import numpy as np
from itertools import chain
import datetime as dt
from datetime import datetime
import time
import yfinance as yf
from sqlalchemy import create_engine
import sqlalchemy
cg = CoinGeckoAPI()


query="""query MyQuery {
  projectDb{
    name
    coingeckoApi
    yFinanceApi
  }
}"""

url = 'https://api.baseql.com/airtable/graphql/' #airtable
r = requests.post(url, json={'query': query})
print(r.status_code)
print(r.text)

json_data = json.loads(r.text)
df_data = json_data["data"]["projectDb"]
df = pd.DataFrame(df_data)

df['coingeckoApi']=df['coingeckoApi'].str.strip()
df['yFinanceApi']=df['yFinanceApi'].str.strip()

dataset_cg=df[['name', 'coingeckoApi']]
dataset_yf=df[['name', 'yFinanceApi']]

listcoingecko=dataset_cg['coingeckoApi'].tolist()
listyfinance=dataset_yf['yFinanceApi'].tolist()

listcoingecko = [x for x in listcoingecko if x != None]
listyfinance = [x for x in listyfinance if x != None]

#need to find a better way to bypass this api lim
listcoingecko_2=[listcoingecko[0:25], listcoingecko[25:50], listcoingecko[50:75], listcoingecko[75:100], listcoingecko[100:125], listcoingecko[125:150], listcoingecko[150:175], listcoingecko[175:200], listcoingecko[200:225], listcoingecko[225:250]]

positions=[]
later=[]
today='positions'+ str(datetime.date(datetime.now()))

for p in range(len(listcoingecko_2)):
    listtocheck=listcoingecko_2[p]


    if len(listtocheck)>1:

        for i in range(len(listtocheck)):
            print(listtocheck[i])

            try:
                market_data=cg.get_coin_market_chart_by_id(id=listtocheck[i], vs_currency='usd', days='90')
                market_data=market_data['prices']

            except:
                print(str(listtocheck[i])+" did not work")
                later.append(listtocheck[i])
                pass


            for n in range(len(market_data)):

                market_data[n].append(listtocheck[i])

            positions.append(market_data)
        print('List '+ str(p) +' is ready. Waiting 120 seconds until next pull')
        time.sleep(120)

    else:
        pass

positions_yf=pd.DataFrame(columns=['Date', 'Close','coingecko_api'])

for i in listyfinance:
    ticker=i
    temporal=yf.Ticker(i)
    hist=temporal.history(period='1y')
    hist=hist.reset_index()
    hist=hist[['Date','Close']]
    hist['coingecko_api']=ticker
    positions_yf=pd.concat([positions_yf,hist])

positions_yf['Date']=pd.to_datetime(positions_yf['Date']).dt.date

positions_yf=positions_yf.rename(columns={'Date':'date_price', 'Close': 'price_coingecko'})
positions_yf.price_coingecko=positions_yf.price_coingecko.astype(float)

positions_yf=positions_yf.merge(df,left_on='coingecko_api',right_on='yFinanceApi')
positions_yf=positions_yf.rename(columns={'name':'name_position'})
positions_yf_tabular=positions_yf[['date_price','coingecko_api', 'name_position', 'price_coingecko']]



positions=list(chain(*positions))


positions_df=pd.DataFrame(positions)
positions_df[0]=pd.to_datetime(positions_df[0], unit='ms')
positions_df=positions_df.reset_index()
positions_df[0]=pd.to_datetime(positions_df[0]).dt.date
positions_df=positions_df.groupby([0,2]).mean()

positions_df=positions_df.reset_index()
positions_df=positions_df.merge(df,left_on=2,right_on='coingeckoApi')
positions_df=positions_df.rename(columns={0:'date_price', 1:'price_coingecko', 2: 'coingecko_api', 'name':'name_position'})


positions_df_tabular=positions_df[['date_price','coingecko_api', 'name_position', 'price_coingecko']]
positions_df_tabular=pd.concat([positions_df_tabular, positions_yf_tabular])



#export to posgresql db
host = ''
port = int(25060)
database = ''
user=''
password=''
mydb = create_engine('postgresql://' + user + ':' + password + '@' + host + ':' + str(port) + '/' + database, echo=False)

mydb.connect()


positions_df_tabular.to_sql(name='liquid_prices', index=False, con=mydb, if_exists='append',
                dtype={'date_price': sqlalchemy.types.DateTime(),
                       'name_position': sqlalchemy.types.VARCHAR(length=255),
                       'coingecko_api': sqlalchemy.types.VARCHAR(length=255),
                       'price_coingecko': sqlalchemy.types.FLOAT()})


mydb.dispose()


print(str(datetime.now()))
