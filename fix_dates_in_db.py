'''
Created on 25 mar. 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-

from log_functions import *
from import_functions import *
from pandas.io import sql

db_name = "test_base"
db_user = "postgres"
db_host = "192.168.0.52"
db_pass = "postgres"
db_package = [db_name, db_user, db_host, db_pass]

if DB_direct_connection(db_package)[0]:
    conn = DB_direct_connection(db_package)[1]

dates_query = "SELECT id, birth FROM ID where id > 4000000 limit 1000;"
dataframe = sql.read_sql(dates_query, conn, index_col='id')
print "done importing table"
dataframe.columns = ['birth']
dataframe['date'] = pd.Series([pd.to_datetime(date) for date in dataframe['birth']])

for cpt in range(len(dataframe.index)):
    date = dataframe.at[dataframe.index[cpt], 'date']
    year = date.year
    if year < 1900:
        date = date + relativedelta(years = 100)
    if year > 2000:
        date = date - relativedelta(years = 100)
        