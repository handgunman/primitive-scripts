#Loading Yandex Metrica logs
# -*- coding: utf-8 -*-
import requests
import pandas as pd
from io import StringIO
import datetime
import json
from urllib.parse import urlencode
import time
import os
import utils

# https://yandex.ru/dev/oauth/doc/dg/tasks/get-oauth-token.html
# https://oauth.yandex.ru/client/new
# https://oauth.yandex.ru/authorize?response_type=token&client_id=<идентификатор приложения>
# put token to yatoken.txt
TOKEN = open('yatoken.txt').read().strip()

API_HOST = 'https://api-metrika.yandex.ru'
COUNTER_ID = 25412549


START_DATE = '' # '2022-08-23'
END_DATE = '' # '2022-08-24'

if START_DATE == '':
    sd = (datetime.datetime.today() - datetime.timedelta(1)) \
                .strftime(utils.DATE_FORMAT)
    ed = (datetime.datetime.today() - datetime.timedelta(1)) \
                .strftime(utils.DATE_FORMAT)
else:
    sd = START_DATE
    ed = END_DATE

print(f'Date period: {sd} - {ed}')

############################################################
###### HITS

print('Load hits ***')

SOURCE = 'hits' 

#https://yandex.ru/dev/metrika/doc/api2/logs/fields/hits.html
API_FIELDS = ('ym:pv:date', 'ym:pv:dateTime', 'ym:pv:URL', 'ym:pv:deviceCategory', 
         'ym:pv:operatingSystemRoot', 'ym:pv:clientID', 'ym:pv:browser', 'ym:pv:lastTrafficSource')

header_dict = {'Authorization': f'OAuth {TOKEN}',
'Content-Type': 'application/x-yametrika+json'
}

url_params = urlencode(
    [
        ('date1', sd),
        ('date2', ed),
        ('source', SOURCE),
        ('fields', ','.join(sorted(API_FIELDS, key=lambda s: s.lower())))
    ]
)
url = '{host}/management/v1/counter/{counter_id}/logrequests?'\
    .format(host=API_HOST,
            counter_id=COUNTER_ID) \
      + url_params

r = requests.post(url, headers=header_dict)

print(f'Request hits data: {r.status_code}')

json.loads(r.text)['log_request']

request_id = json.loads(r.text)['log_request']['request_id']

status = 'created'
while status == 'created':
    time.sleep(60)
    print('trying')
    url = '{host}/management/v1/counter/{counter_id}/logrequest/{request_id}' \
            .format(request_id=request_id,
                    counter_id=COUNTER_ID,
                    host=API_HOST)

    r = requests.get(url, headers=header_dict)
    if r.status_code == 200:
        status = json.loads(r.text)['log_request']['status']
        print(json.dumps(json.loads(r.text)['log_request'], indent = 4))
    else:
        raise(BaseException(r.text))

json.loads(r.text)['log_request']

parts = json.loads(r.text)['log_request']['parts']

tmp_dfs = []
for part_num in map(lambda x: x['part_number'], parts):
    url = '{host}/management/v1/counter/{counter_id}/logrequest/{request_id}/part/{part}/download' \
            .format(
                host=API_HOST,
                counter_id=COUNTER_ID,
                request_id=request_id,
                part=part_num
            )

    r = requests.get(url, headers=header_dict)
    if r.status_code == 200:
        tmp_df = pd.read_csv(StringIO(r.text), sep = '\t')
        tmp_dfs.append(tmp_df)
    else:
        raise(BaseError(r.text))
        
hits_df = pd.concat(tmp_dfs)

hits_df.to_csv('metrika_hits.csv', sep = '\t', index = False)

############################################################
### VISITS

print('Load visits ***')

SOURCE = 'visits'

#https://yandex.ru/dev/metrika/doc/api2/logs/fields/visits.html
API_FIELDS = ('ym:s:date', 'ym:s:dateTime', 'ym:s:startURL', 'ym:s:deviceCategory', 
         'ym:s:operatingSystemRoot', 'ym:s:clientID', 'ym:s:browser', 'ym:s:lastTrafficSource', 'ym:s:purchaseRevenue', 'ym:s:purchaseID')

url_params = urlencode(
    [
        ('date1', sd),
        ('date2', ed),
        ('source', SOURCE),
        ('fields', ','.join(sorted(API_FIELDS, key=lambda s: s.lower())))
    ]
)
url = '{host}/management/v1/counter/{counter_id}/logrequests?'\
    .format(host=API_HOST,
            counter_id=COUNTER_ID) \
      + url_params

r = requests.post(url, headers=header_dict)

print(f'Request visits data: {r.status_code}')

json.loads(r.text)['log_request']

request_id = json.loads(r.text)['log_request']['request_id']

status = 'created'
while status == 'created':
    time.sleep(60)
    print('trying')
    url = '{host}/management/v1/counter/{counter_id}/logrequest/{request_id}' \
            .format(request_id=request_id,
                    counter_id=COUNTER_ID,
                    host=API_HOST)

    r = requests.get(url, headers=header_dict)
    if r.status_code == 200:
        status = json.loads(r.text)['log_request']['status']
        print(json.dumps(json.loads(r.text)['log_request'], indent = 4))
    else:
        raise(BaseException(r.text))

json.loads(r.text)['log_request']

parts = json.loads(r.text)['log_request']['parts']

tmp_dfs = []
for part_num in map(lambda x: x['part_number'], parts):
    url = '{host}/management/v1/counter/{counter_id}/logrequest/{request_id}/part/{part}/download' \
            .format(
                host=API_HOST,
                counter_id=COUNTER_ID,
                request_id=request_id,
                part=part_num
            )

    r = requests.get(url, headers=header_dict)
    if r.status_code == 200:
        tmp_df = pd.read_csv(StringIO(r.text), sep = '\t')
        tmp_dfs.append(tmp_df)
    else:
        raise(BaseError(r.text))
        
visits_df = pd.concat(tmp_dfs)

visits_df.to_csv('metrika_visits.csv', sep = '\t', index = False)



