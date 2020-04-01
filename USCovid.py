#!/usr/bin/env python
# coding: utf-8

#Import libraries 
from bs4 import BeautifulSoup
import sqlalchemy
from sqlalchemy import create_engine
import requests
import pandas as pd
#Create SQLite connection
conn = create_engine('sqlite://')

#Scrape data from New York State website
res = requests.get("https://www.worldometers.info/coronavirus/country/us/")
#Parse Data and get it ready for Pandas
soup = BeautifulSoup(res.content,'lxml')
table = soup.find_all('table')[0] 
#Put it in a Pandas Dataframe
US = pd.read_html(str(table))[0]

#Same thing here
res = requests.get("https://worldpopulationreview.com/states/")
soup = BeautifulSoup(res.content,'lxml')
table = soup.find_all('table')[0] 
USPop = pd.read_html(str(table))[0]

#Pass dataframes to SQL for cleaning
USPop.to_sql('USPop',conn, if_exists='replace')
US.to_sql('USCov', conn, if_exists='replace')

USF = pd.read_sql(
  '''
With Pop as (
Select 
case when State='District of Columbia' then 'District Of Columbia' else State end as State, 
[2020 Pop.] as Pop, 
[% of US] as USPct 
from USPop)

Select p.*, 
TotalCases, 
NewCases,
TotalDeaths,
NewDeaths, 
ActiveCases 
from Pop p 
LEFT JOIN USCov c on c.USAState=p.State
''',conn
)

USF.to_csv('USACov.csv')
