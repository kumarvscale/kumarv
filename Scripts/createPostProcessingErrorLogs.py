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


# In[2]:


## https://developers.google.com/sheets/api/quickstart/python

## CREDS
# S3
BUCKET = 'scale-crawler-enriched-csv-exports-us-west-2'
s3 = boto3.client('s3')
session = boto3.Session()

# Google Sheets
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1ycZEbsg7hEb_kKAYmIg6eK0hBIl4fvhK0FDan1f5UkE'
RANGE_NAME = 'Sheet9!A:M'
PATH_TO_SECRETS_FILE = 'credentials.json'
creds = None

con = snowflake.connector.connect(user='charlotte.cole@scale.com',
                                 account='pxa65918',
                                 authenticator='externalbrowser',
                                 warehouse='COMPUTE_WH',
                                 database='SCALE_CRAWLER',
                                 role='GENERAL_RO')

cs = con.cursor()


# In[3]:


def uploadData(data,filename):
    s3.put_object(
        ACL='bucket-owner-full-control',
        Body=data.encode('utf-8'),
        Bucket=RESULTS_BUCKET,
        Key=f'flamingo_qa_potential_issues/{filename}')

## Pull data from Google Sheet https://docs.google.com/spreadsheets/d/1UCIE1P6PbI9odzxFUjNF44s-SaPePbDUnHQKqxa9XpM/edit#gid=774020952

def pullFromGS(SCOPES,PATH_TO_SECRETS_FILE,creds,SPREADSHEET_ID,RANGE_NAME):
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(PATH_TO_SECRETS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('sheets', 'v4', credentials=creds)

        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,range=RANGE_NAME).execute()
        values = result.get('values', [])

        if not values:
            print('No data found.')
        
    except HttpError as err:
        print(err)
        
    df = pd.DataFrame(values[1:],columns = values[0])    
    return df

def getCQRResults(min_date,max_date):
    
    sql = f'''
    with cqr_result as (
      with audits as (
        select
          sa.CATALOG_ID,
          sa.domain,
          sa.BODY_S3_KEY,
          sa._id audit_id,
          date(sa.completed_at) audit_time,
          sa.grade :"scores" :"descriptionScore" :"score" as CQR_DESCRIPTION_SCORE,
          sa.grade :"scores" :"titleScore" :"score" as Title,
          sa.result
        from
          PUBLIC.SPOTTERAUDITS sa
          inner join (
            select
              max(completed_at) as max_time,
              CATALOG_ID
            from
              PUBLIC.SPOTTERAUDITS
            group by
              CATALOG_ID
          ) as cqr_max on cqr_max.CATALOG_ID = sa.CATALOG_ID
          and cqr_max.max_time = sa.completed_at
        where
          AUDIT_TYPE = 'Attributes'
          and sa.COMPLETED_AT is not null
          and sa.grade :"scores" :"descriptionScore" :"score" is not null
          and date(sa.completed_at) >= '{min_date}'
          and date(sa.completed_at) <= '{max_date}'
      )
      select
        au.CATALOG_ID,
        au.domain,
        au.audit_id,
        au.audit_time CQR_AUDIT_DATE,
        au.BODY_S3_KEY,
        a.key variant_id,
        b.key attribute,
        b.value :result :: string attribute_grade,
        b.value :reason :: string reason,
        b.value :comment :: string comment  
      from
        audits au,
        lateral flatten (input => au.result) a,
        lateral flatten (input => a.value) b
      where
        b.key in ('description')
        and b.value :result = 'Incorrect'
    )
    select 
    c.*,
    pv.pvid,
    pv.scraped_attributes:link::string link
    from cqr_result c
    join productvariants pv on pv.unique_id = c.variant_id
    '''
    print('Getting CQR data from Snowflake!')
    cs.execute(sql)
    df = cs.fetch_pandas_all()
    print('Success! Got CQR data from Snowflake. Number of rows:',len(df),'\n-------------')
    return df
        
def getCQRInputs(cqr_results):
    
    df = pd.DataFrame() 
    print('Getting CQR input data from S3!')
    for s3_file in cqr_results['BODY_S3_KEY'].unique().tolist():
        print('Pulling from', s3_file)
        response = s3.get_object(Bucket = BUCKET, Key = s3_file)
        tmp = pd.read_csv(response.get("Body"))
        df = pd.concat([df,tmp])
    print('Success! Got CQR input data from S3. Number of rows:', len(df),'\n-------------')
    return df

def mergeCQRData(cqr_results, cqr_inputs):
    if len(cqr_results) == 0 or len(cqr_inputs) == 0: 
        print('ERROR: Not enough information to complete')
        df = pd.DataFrame()
    else:
        print('Merging data!')
        df = cqr_results.merge(cqr_inputs[['pvid','description','link']], left_on = 'PVID', right_on = 'pvid')
        df = df.fillna('').rename(columns = {'description':'POST_PROCESSED_DESCRIPTION','COMMENT':'CORRECT_DESCRIPTION'})

        df = df.sort_values(['POST_PROCESSED_DESCRIPTION'])
        df = df.loc[(df['POST_PROCESSED_DESCRIPTION'] != '') & (df['CORRECT_DESCRIPTION'] != '')] 
        print('Success! Merged data. Number of rows:', len(df),'\n-------------')
    return df

def getPPQAData(relevant_pvids):
    pvids = "('" + "','".join(relevant_pvids) + "')"
#     print(pvids)
    sql_descs = f'''
    select
      user_email,
      metadata :pvids description_id,
      b.value :: string pvid,
      CREATED_AT variant_pped_at,
      metadata: auditLevel :: string audit_level,
      metadata: fieldCurrent :: string QA_DESCRIPTION
    from
      PUBLIC.QAEVENTS,
      lateral flatten(input => metadata :pvids) b
    where
      audit_level != 'Other'
      and METADATA :action in ('Save', 'SwitchItem')
      and metadata: fieldCurrent is not Null
      and pvid in {pvids}
    '''

    sql_rules = f'''
    select
      user_email,
      metadata :pvids description_id,
      b.value :: string pvid,
      CREATED_AT variant_pped_at,
      metadata: auditLevel :: string audit_level,
      metadata: flagComment :: string flagtext,
      metadata: ruleCreated :: string ruleCreated
    from
      PUBLIC.QAEVENTS,
      lateral flatten(input => metadata :pvids) b
    where
      audit_level != 'Other'
      and METADATA :action in ('CreateRule')
      and metadata: flagComment is not Null
      and pvid in {pvids}
    '''
    print('Getting descriptions data from Snowflake!')
    cs.execute(sql_descs)
    pp_desc_data = cs.fetch_pandas_all()
    print('Success! Got descriptions data from Snowflake. Number of rows:',len(pp_desc_data))    
    
    print('Getting rules data from Snowflake!')
    cs.execute(sql_rules)
    pp_rules_data = cs.fetch_pandas_all()
    print('Success! Got rules data from Snowflake. Number of rows:',len(pp_rules_data),'\n-------------')        
    
    return pp_desc_data, pp_rules_data

def generateSpeedAuditErrors(cqr_data, pp_desc_data):
    cols = ['CQR_AUDIT_DATE', 'USER_EMAIL', 'type', 'AUDIT_LEVEL',
                    'DOMAIN', 'description_PPed_at', 'sample_pvid',
                    'sample_link','CORRECT_DESCRIPTION', 'QA_DESCRIPTION',
                   'Extra text (not removed by QA)',
                   'Missing text (incorrectly removed by QA)','outcome']
        
    if len(cqr_data) == 0 or len(pp_desc_data) == 0: 
        print('ERROR: Not enough information to complete')
        dff = pd.DataFrame(columns = cols)
    else: 
        print('Generating Speed Audit errors!')
        df = cqr_data.merge(pp_desc_data, on = 'PVID')
        df = df.rename(columns = {'COMMENT':'CORRECT_DESCRIPTION'})
        df['clean_final_desc'] = df.apply(lambda x: re.sub('\\\\n|\n| ','',x['CORRECT_DESCRIPTION']),axis=1)
        df['clean_fieldcurrent'] = df.apply(lambda x: re.sub('\\\\n|\n| ','',x['QA_DESCRIPTION']),axis=1)
        df['is_correct_desc'] = df['clean_final_desc'] == df['clean_fieldcurrent']
        df = df.drop_duplicates() # .loc[df['is_correct_desc'] == False]
        if len(df) ==0:
            return df
        else:
            tmp_cols = ['CQR_AUDIT_DATE',
                'USER_EMAIL',
                'AUDIT_LEVEL',
                'DOMAIN',
                'CORRECT_DESCRIPTION',
                'QA_DESCRIPTION','is_correct_desc']

            dff = df.groupby(tmp_cols)['VARIANT_PPED_AT','PVID','LINK'].min()                .reset_index()                .rename(columns = {'VARIANT_PPED_AT':'description_PPed_at','PVID':'sample_pvid','LINK':'sample_link'})
            dff['Extra text (not removed by QA)'] = dff.apply(lambda x: np.setdiff1d([i.strip('. ').strip('! ').strip('? ').lower() for i in re.split('\. |\n|\! |\? ', x['QA_DESCRIPTION']) if i != ''],[i.strip('. ').strip('! ').strip('? ').lower() for i in re.split('\. |\n|\! |\? ', x['CORRECT_DESCRIPTION']) if i != '']), axis = 1)    
            dff['Missing text (incorrectly removed by QA)'] = dff.apply(lambda x: np.setdiff1d([i.strip('. ').strip('! ').strip('? ').lower() for i in re.split('\. |\n|\! |\? ', x['CORRECT_DESCRIPTION']) if i != ''],[i.strip('. ').strip('! ').strip('? ').lower() for i in re.split('\. |\n|\! |\? ', x['QA_DESCRIPTION']) if i != '']), axis = 1)
            dff['type'] = 'Speed Audit'
            dff['outcome'] = dff.apply(lambda x: 'incorrect speed audit' if x['is_correct_desc'] == False else 'correct speed audit', axis = 1)
           
            print('Success! Generated Speed Audit Errors\n-------------')     
        return dff.loc[:,cols]

def generateFlagAuditErrors(full_cqr_data, pp_rules_data):
    cols = ['CQR_AUDIT_DATE', 'USER_EMAIL','type','AUDIT_LEVEL',
                'DOMAIN', 'description_PPed_at', 'sample_pvid',
                'sample_link','FLAGTEXT', 'RULECREATED',
               'Extra text (not removed by QA)',
               'Missing text (incorrectly removed by QA)','outcome']
    if len(full_cqr_data) == 0 or len(pp_rules_data) == 0: 
        print('ERROR: Not enough information to complete')
        dff = pd.DataFrame(columns = cols)
    else: 
        print('Generating Flag Audit errors!')
        df = full_cqr_data.merge(pp_rules_data, on = 'PVID')
        df = df.rename(columns = {'COMMENT':'CORRECT_DESCRIPTION'})
        cols = ['CQR_AUDIT_DATE',
            'USER_EMAIL',
            'AUDIT_LEVEL',
            'DOMAIN',
            'POST_PROCESSED_DESCRIPTION',
            'CORRECT_DESCRIPTION',
               'FLAGTEXT','RULECREATED']
        dff = df.groupby(cols)['VARIANT_PPED_AT','PVID','LINK'].min()            .reset_index()            .rename(columns = {'VARIANT_PPED_AT':'description_PPed_at','PVID':'sample_pvid','LINK':'sample_link'})
        dff['Extra text (not removed by QA)'] = dff.apply(lambda x: np.setdiff1d([i.strip('. ') for i in re.split('\. |\n|\! |\? ', x['POST_PROCESSED_DESCRIPTION']) if i != ''],[i.strip('. ') for i in re.split('\. |\n|\! |\? ', x['CORRECT_DESCRIPTION']) if i != '']), axis = 1)    
        dff['Missing text (incorrectly removed by QA)'] = dff.apply(lambda x: np.setdiff1d([i.strip('. ') for i in re.split('\. |\n|\! |\? ', x['CORRECT_DESCRIPTION']) if i != ''],[i.strip('. ') for i in re.split('\. |\n|\! |\? ', x['POST_PROCESSED_DESCRIPTION']) if i != '']), axis = 1)

        dff['bad_removal'] = dff.apply(lambda x: x['RULECREATED'] == 'true' and re.sub("\.|\'|\,",'',x['FLAGTEXT'].strip().lower()) in re.sub("\.|\'|\,",'',str(x['Missing text (incorrectly removed by QA)']).strip().lower()),axis = 1)
        dff['bad_inclusion'] = dff.apply(lambda x:  x['RULECREATED'] == 'false' and re.sub("\.|\'|\,",'',x['FLAGTEXT'].strip().lower()) in re.sub("\.|\'|\,",'',str(x['Extra text (not removed by QA)']).strip().lower()),axis = 1)
        dff['outcome'] = dff.apply(lambda x: 'bad flag removal' if x['bad_removal'] == True else ('bad flag inclusion' if x['bad_inclusion'] == True else 'ok'), axis = 1)
        dff['type'] = 'Flag Audit'

        print('Success! Generated Flag Audit Errors\n-------------')    
    return dff.loc[:,cols] #dff['outcome'] != 'ok',

def completeErrorReport(speed_audit_errors,flag_audit_errors):
    df = pd.concat([speed_audit_errors,flag_audit_errors])[['CQR_AUDIT_DATE',
    'USER_EMAIL',
    'type',
    'AUDIT_LEVEL',
    'DOMAIN',
    'description_PPed_at',
    'sample_pvid',
    'sample_link',
    'CORRECT_DESCRIPTION',
    'QA_DESCRIPTION',
    'FLAGTEXT',
    'RULECREATED',
    'Extra text (not removed by QA)',
    'Missing text (incorrectly removed by QA)',
    'outcome']]
    df = df.sort_values(['DOMAIN','USER_EMAIL'])
    
    print(df.shape)
    
    df['CQR_AUDIT_DATE'] = pd.to_datetime(df['CQR_AUDIT_DATE'],utc=True)
    df['description_PPed_at'] = pd.to_datetime(df['description_PPed_at'],utc=True)
    df = df.loc[abs((df['CQR_AUDIT_DATE'] - df['description_PPed_at']).dt.days) <= 7] ## only include work done in past week
    
    df.loc[df['outcome'].isin(['incorrect speed audit','bad flag removal'])].to_clipboard(index = False)
    print('Error Report created!')
    return df
    


# In[4]:


date_in = '08/18/2022'
# date_out = date_in
date_out = '08/23/2022'

print(f'ERROR LOGS {date_in} to {date_out}\n')
cqr_results = getCQRResults(date_in,date_out)
cqr_inputs = getCQRInputs(cqr_results)
full_cqr_data = mergeCQRData(cqr_results, cqr_inputs)
pp_desc_data, pp_rules_data = getPPQAData(cqr_results['PVID'].unique().tolist())
speed_audit_errors = generateSpeedAuditErrors(cqr_results, pp_desc_data)
flag_audit_errors = generateFlagAuditErrors(full_cqr_data, pp_rules_data)
df = completeErrorReport(speed_audit_errors,flag_audit_errors)


# In[6]:


df.loc[df['outcome'].isin(['incorrect speed audit','bad flag removal'])].to_clipboard(index = False)


# In[ ]:




