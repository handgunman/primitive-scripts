#Upload Metrics data from files to PostgreSQL
# -*- coding: utf-8 -*-
import requests
import pandas as pd
import subprocess


############################################################
### UPLOAD DATA
print('UpLoad data to PosrgreSQL ***')

REPLACE_DATA = 1 # replace 1, add 0. Use 1 if you have changed the list of fields and are loading the data again

PG_HOST      = 'localhost' #'rc1b-...'
PG_PORT      = '5432' # '6432'
PG_USER      = 'postgres'
PG_DB_NAME   = 'postgres'
PG_UTIL      = './psql' #'psql'

PG_PASS      = open('pgpass.txt').read().strip() # Put password to pgpass.txt
#PG_CERT     = '...postgresql/root.crt'
PG_SSL       = 'disable' # 'verify-full'


#The files are created by the script logs_download_simple.py
#hits_df = pd.read_csv('metrika_hits.csv', sep = '\t')
visits_df = pd.read_csv('metrika_visits.csv', sep = '\t')


conn = f'host={PG_HOST} port={PG_PORT} dbname={PG_DB_NAME} user={PG_USER} password={PG_PASS} sslmode={PG_SSL}'


#####################################
# Drop table hits_test
if (REPLACE_DATA == 1):

    q = f'--command=drop table if exists hits_test'

    r = subprocess.run([PG_UTIL, conn, q], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')

    if r.returncode == 0:
        print(f'Hits table dropped')
    else:
        raise ValueError(r)

#####################################
# Create table hits_test

# Please see the list of fields in logs_download_simple.py
q = f'''--command=create table IF NOT EXISTS hits_test (
    Browser varchar,
    ClientID numeric(22),
    EventDate date,
    EventTime timestamp,
    DeviceCategory varchar,
    TraficSource varchar,
    OSRoot varchar,
    URL varchar
);
'''

r = subprocess.run([PG_UTIL, conn, q], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')

if r.returncode == 0:
    print(f'Hits table created')
else:
    raise ValueError(r.stderr)


# Upload data
q = f'--command=\\copy hits_test FROM \'metrika_hits.csv\' DELIMITER E\'\\t\' CSV HEADER ENCODING \'UTF8\''

r = subprocess.run([PG_UTIL, conn, q], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')

if r.returncode == 0:
    print(f'Hits uploaded')
else:
    raise ValueError(r.stderr)


#####################################
# Drop table visits_test
if (REPLACE_DATA == 1):

    q = f'--command=drop table if exists visits_test'

    r = subprocess.run([PG_UTIL, conn, q], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')

    if r.returncode == 0:
        print(f'Visits table dropped')
    else:
        raise ValueError(r.stderr)


#####################################
# Create table visits_test

# Please see the list of fields in logs_download_simple.py
q = f'''--command=create table IF NOT EXISTS visits_test (
    Browser varchar,
    ClientID numeric(22),
    StartDate date,
    StartTime timestamp,
    DeviceCategory int,
    TraficSource varchar,
    OSRoot varchar,
    Purchases bigint,
    Revenue double precision,
    StartURL varchar
)
'''

r = subprocess.run([PG_UTIL, conn, q], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')

if r.returncode == 0:
    print(f'Visits table created')
else:
    raise ValueError(r.stderr)

# Data change
# Changing headers may be used to selectively load fields
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

# Array conversion example may be used to replace arrays by aggregations of number of elements or sums over elements
visits_df['Purchases'] = visits_df['Purchase.Revenue'].map(lambda x: x.count(',') + 1 if x != '[]' else 0 )
visits_df['Revenue'] = visits_df['Purchase.Revenue'].map(lambda x: sum(map(float,x[1:-1].split(','))) if x != '[]' else 0)

# Dropp fields example
visits_df.drop(columns=['Purchase.ID','Purchase.Revenue'], inplace=True)

# Save changed data to the file
# the order of the fields may change
visits_df.to_csv('metrika_visits_ch.csv', sep = '\t', index = False)


# list of fields in the desired composition and order can be specified
q = f'''--command=\\copy visits_test 
    (Browser,ClientID,StartDate,StartTime,DeviceCategory,TraficSource,OSRoot,StartURL,Purchases,Revenue) 
    FROM \'metrika_visits_ch.csv\' DELIMITER E\'\\t\' CSV HEADER ENCODING \'UTF8\''''


r = subprocess.run([PG_UTIL, conn, q], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')

if r.returncode == 0:
    print(f'Visits uploaded')
else:
    raise ValueError(r.stderr)
