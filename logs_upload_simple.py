# Upload Metrics data from files to ClickHouse
# -*- coding: utf-8 -*-
import requests
import clickhouse
import pandas as pd


############################################################
### UPLOAD DATA

print('UpLoad data to ch ***')

REPLACE_DATA = 0 # replace 1, add 0

CH_HOST_NAME = 'localhost'
CH_USER      = 'default'
CH_DB_NAME   = 'default'

CH_PASS      = open('chpass.txt').read().strip() # Put password to chpass.txt
CH_HOST      = f'http://{CH_HOST_NAME}:8123'
#CH_HOST      = f'https://{CH_HOST_NAME}:8443'

SSL_VERIFY = 0
#SSL_VERIFY = 'YandexInternalRootCA.crt'


#The files are created by the script logs_download_simple.py
hits_df = pd.read_csv('metrika_hits.csv', sep = '\t')
visits_df = pd.read_csv('metrika_visits.csv', sep = '\t')

#####################################
# Drop table hits_test
if (REPLACE_DATA == 1):

    q = f'drop table if exists {CH_DB_NAME}.hits_test '

    r = requests.post(CH_HOST, data=q, auth=(CH_USER, CH_PASS), verify=SSL_VERIFY)

    if r.status_code == 200:
        print(f'Hits table dropped')
    else:
        raise ValueError(r.text)

#####################################
# Create table hits_test

# Please see the list of fields in logs_download_simple.py
q = f'''
create table IF NOT EXISTS {CH_DB_NAME}.hits_test (
    Browser String,
    ClientID UInt64,
    EventDate Date,
    EventTime DateTime,
    DeviceCategory String,
    TraficSource String,
    OSRoot String,
    URL String
) ENGINE = MergeTree(EventDate, intHash32(ClientID), (EventDate, intHash32(ClientID)), 8192)
'''

r = requests.post(CH_HOST, data=q, auth=(CH_USER, CH_PASS), verify=SSL_VERIFY)

if r.status_code == 200:
    print(f'Hits table created')
else:
    raise ValueError(r.text)

hits_df.head()

hits_df.rename(columns={'ym:pv:browser':'Browser',
                'ym:pv:clientID':'ClientID',
                'ym:pv:date':'EventDate',
                'ym:pv:dateTime':'EventTime',
                'ym:pv:deviceCategory':'DeviceCategory',
                'ym:pv:lastTrafficSource':'TraficSource',
                'ym:pv:operatingSystemRoot':'OSRoot',
                'ym:pv:URL':'URL'}, inplace = True)

q = {'query': 'INSERT INTO hits_test FORMAT TabSeparatedWithNames'}


r = requests.post(CH_HOST, data=hits_df.to_csv(index = False, sep = '\t').encode('utf-8'), params=q, 
                          auth=(CH_USER, CH_PASS), verify=SSL_VERIFY)
result = r.text
if r.status_code == 200:
    print(f'Hits uploaded')
else:
    raise ValueError(r.text)

#####################################
# Drop table visits_test
if (REPLACE_DATA == 1):

    q = f'drop table if exists {CH_DB_NAME}.visits_test '

    r = requests.post(CH_HOST, data=q, auth=(CH_USER, CH_PASS), verify=SSL_VERIFY)

    if r.status_code == 200:
        print(f'Visits table dropped')
    else:
        raise ValueError(r.text)


#####################################
# Create table visits_test

# Please see the list of fields in logs_download_simple.py
q = f'''
create table IF NOT EXISTS {CH_DB_NAME}.visits_test (
    Browser String,
    ClientID UInt64,
    StartDate Date,
    StartTime DateTime,
    DeviceCategory UInt8,
    TraficSource String,
    OSRoot String,
    Purchases Int32,
    Revenue Double,
    StartURL String
) ENGINE = MergeTree(StartDate, intHash32(ClientID), (StartDate, intHash32(ClientID)), 8192)
'''

r = requests.post(CH_HOST, data=q, auth=(CH_USER, CH_PASS), verify=SSL_VERIFY)

if r.status_code == 200:
   print(f'Visits table created')
else:
   raise ValueError(r.text)

visits_df.head()

visits_df.rename(columns={'ym:s:browser':'Browser',
                'ym:s:clientID':'ClientID',
                'ym:s:date':'StartDate',
                'ym:s:dateTime':'StartTime',
                'ym:s:deviceCategory':'DeviceCategory',
                'ym:s:lastTrafficSource':'TraficSource',
                'ym:s:operatingSystemRoot':'OSRoot',
                'ym:s:purchaseRevenue': 'Purchase.Revenue', 
                'ym:s:purchaseID': 'Purchase.ID',
                'ym:s:startURL':'StartURL'}, inplace = True)

# Array conversion example
visits_df['Purchases'] = visits_df['Purchase.Revenue'].map(lambda x: x.count(',') + 1 if x != '[]' else 0 )
visits_df['Revenue'] = visits_df['Purchase.Revenue'].map(lambda x: sum(map(float,x[1:-1].split(','))) if x != '[]' else 0)

# Dropp fields example
visits_df.drop(columns=['Purchase.ID','Purchase.Revenue'], inplace=True)



q = {'query': 'INSERT INTO visits_test FORMAT TabSeparatedWithNames'}


r = requests.post(CH_HOST, data=visits_df.to_csv(index = False, sep = '\t').encode('utf-8'), params=q, 
                          auth=(CH_USER, CH_PASS), verify=SSL_VERIFY)
result = r.text
if r.status_code == 200:
    print(f'Visits uploaded')
else:
    raise ValueError(r.text)




 