#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import re
import datetime
import numpy as np
import requests
from pandas.io.json import json_normalize
import json
import os
import os.path
import snowflake.connector
import boto3
from __future__ import print_function
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import random
import concurrent.futures
import time
from tqdm import tqdm


# ## Setup

# ### Credentials

# In[2]:


## https://developers.google.com/sheets/api/quickstart/python

# Google Sheets
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SPREADSHEET_ID = '18WNpqJ1v4NRAkQG6M2VqPMKqO5JdxS2gRyUVNUmbqGU'
RANGE_NAME = "'Batch 8'!A:B"
PATH_TO_SECRETS_FILE = 'credentials.json'
creds = None

# Snowflake
con = snowflake.connector.connect(user='charlotte.cole@scale.com',
                                 account='pxa65918',
                                 authenticator='externalbrowser',
                                 warehouse='COMPUTE_WH',
                                 database='SCALE_CRAWLER',
                                 role='GENERAL_RO')
cs = con.cursor()

# Corp
CORP_KEY = 'scaleint_26e8b448701e41b9bc1128152129ced9'
CUSTOMER_KEY = '608748001b4605002c151701'
LIVE_API_KEY = f'{CORP_KEY}|{CUSTOMER_KEY}'
ENDPOINT = "textcollection"
PROJECT = 'Post-Processing Claim Test'
## Task payload
FIELDS = [
    {
      "type": "category",
      "field_id": "auditLevel",
      "title": "Audit Level",
      "required": 'true',
      "description": "Select the level at which you are auditing this site",
      "choices": [
        {
          "label": "QA",
          "value": "qa"
        },
        {
          "label": "Spotter",
          "value": "spotter"
        }
      ]
    },
    {
      "type": "category",
      "field_id": "spotterChanges",
      "title": "Did you need to makes changes to this site?",
      "required": 'true',
      "choices": [
        {
          "label": "Yes",
          "value": "yes"
        },
        {
          "label": "No",
          "value": "no"
        }
      ],
      "conditions": [
        {
          "auditLevel": [
            "spotter"
          ]
        }
      ]
    },
    {
      "type": "text",
      "field_id": "changesMade",
      "title": "Changes made",
      "description": "Any notes of what you fixed",
      "conditions": [
        {
          "spotterChanges": [
            "yes"
          ]
        }
      ]
    },
    {
      "type": "text",
      "field_id": "QAQuestions",
      "title": "Questions?",
      "description": "Use this field to note any questions / confusions you have!",
      "conditions": [
        {
          "auditLevel": [
            "qa"
          ]
        }
      ]
    },
    {
      "type": "text",
      "field_id": "spotterQuestions",
      "title": "Questions?",
      "description": "Use this field to note any questions / confusions you have!",
      "conditions": [
        {
          "auditLevel": [
            "spotter"
          ]
        }
      ]
    }
  ]
ATTACHMENT = '''
<ol>
    <li>Post-process <a href="{url}" target="_blank">this site segment</a></li>
    <li>Submit the task when you are finished</li>
</ol>

'''


# ### Inputs

# In[57]:


max_desc_per_set = 300
# sample_sites = ['surfnwearbeachhouse.com', # big site 7975 desc
#                 'www.fancybands.net', # medium site 606 desc
#                 'www.tinyorganics.com'] # small site 23 desc

sample_sites = ['www.citymanusa.com']


# ## Get data

# In[58]:


# def pullFromGS(SCOPES,PATH_TO_SECRETS_FILE,creds,SPREADSHEET_ID,RANGE_NAME):
#     if os.path.exists('token.json'):
#         creds = Credentials.from_authorized_user_file('token.json', SCOPES)

#     if not creds or not creds.valid:
#         if creds and creds.expired and creds.refresh_token:
#             creds.refresh(Request())
#         else:
#             flow = InstalledAppFlow.from_client_secrets_file(PATH_TO_SECRETS_FILE, SCOPES)
#             creds = flow.run_local_server(port=0)
#         with open('token.json', 'w') as token:
#             token.write(creds.to_json())

#     try:
#         service = build('sheets', 'v4', credentials=creds)

#         sheet = service.spreadsheets()
#         result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,range=RANGE_NAME).execute()
#         values = result.get('values', [])

#         if not values:
#             print('No data found.')
        
#     except HttpError as err:
#         print(err)
        
#     df = pd.DataFrame(values[1:],columns = values[0])    
#     return df

def getAllSiteDescriptions():
    sql = f'''
    select
      brand,
      count(distinct SCRAPED_ATTRIBUTES :description) ct_description
    from
      PUBLIC.PRODUCTVARIANTS
    where status != 'cancelled'
    group by
      1
    '''
    cs.execute(sql)
    all_sites = cs.fetch_pandas_all()
    return all_sites

def getSiteDescriptionSegments(all_sites):
    all_sites['num_segment'] = (all_sites['CT_DESCRIPTION'] / max_desc_per_set).apply(np.ceil)
    all_site_segments = all_sites.loc[all_sites.index.repeat(all_sites['num_segment'])].reset_index(drop = True)
    all_site_segments['segment_index'] = all_site_segments.groupby(['BRAND']).cumcount()
    all_site_segments['limit'] = max_desc_per_set    
    all_site_segments['skip'] = all_site_segments['segment_index'].apply(lambda x: 0 if x == 0 else max_desc_per_set*x)
    all_site_segments['url'] = all_site_segments.apply(lambda x: 'https://dashboard.crawler.scale.com/pdp-qa-dashboard?limitConsolidated=' + str(max_desc_per_set) + '&skipConsolidated=' + str(x['skip']) + '&url=' + x['BRAND'], axis = 1)
    return all_site_segments

def addPriorities(all_site_segments):
    batches = pd.DataFrame()
    for num in range(4,9): 
        tmp = pullFromGS(SCOPES,PATH_TO_SECRETS_FILE,creds,SPREADSHEET_ID,f"'Batch {num}'!A:B").rename(columns = {'Domain ':'BRAND'})
        tmp['batch'] = num
        batches = pd.concat([batches,tmp])
    dff = all_site_segments.merge(batches, how = 'inner', on = 'BRAND')
    dff['priority'] = 1000 - dff['segment_index'] - dff['batch']
    return dff


# In[64]:


all_site_segments = getSiteDescriptionSegments(getAllSiteDescriptions())


# In[ ]:


# all_site_segments_w_priorities = addPriorities(all_site_segments)


# ## Create tasks

# In[ ]:


def generate_payload(url, metadata, attachments = ATTACHMENT, fields = FIELDS,project = PROJECT):
    return {
        "project": PROJECT,
        "instruction": " ",
        "callback_url": "http://example.com/callback",
        "fields": FIELDS,
        "metadata": metadata,
        "attachments": [
            {"type": "text",
             "content": attachments.format(url=url)}
        ]
    }

def create_task_request(endpoint, payload, apikey = LIVE_API_KEY):
    request_url = "https://api.scale.com/v1/task/" + endpoint
    headers = {"Content-Type": "application/json"}
    request = requests.post(request_url, json=payload, headers=headers, auth=(apikey, ''))
    if request.status_code != 200:
        print("There was an error at " + request_url)
        print(request.text)
        print(payload)
        return 0
    else:
        task_response = request.json()
        tracker = payload['metadata']
        tracker['task_id'] = task_response['task_id']
        return tracker

def uploader(row,project_name):
    
    url = row['url']
    metadata = row.to_dict()
    
    tracker = create_task_request(ENDPOINT, generate_payload(url, metadata),LIVE_API_KEY)
    
    if tracker == 0:
        error_tracker.append(metadata)
    else:
        id_tracker.append(tracker)
       # to_delete.append(tracker['task_id'])
    return id_tracker, error_tracker
 


# In[65]:


to_upload = all_site_segments[all_site_segments['BRAND'].isin(sample_sites)]
tmp = np.setdiff1d(sample_sites,to_upload.BRAND.unique().tolist())
if len(tmp) != 0:
    print('Sites NOT included:')
    for i in tmp:
        print(i)

to_upload.groupby('BRAND').count()


# In[66]:


id_tracker = []
error_tracker = []

with_threads_start = time.time()
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    futures = []
    for i,row in tqdm(to_upload.iterrows()):
        futures.append(executor.submit(uploader,row=row,project_name = PROJECT))
    for future in tqdm(concurrent.futures.as_completed(futures)):
        (future.result())
print("Time elapsed: ", time.time() - with_threads_start)

