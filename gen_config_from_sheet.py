import os
import re
import json
import numpy as np
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
import googleapiclient
import google.auth
from datetime import date
from oauth2client.service_account import ServiceAccountCredentials
# import gspread

def gsheet2json(gsheet):

    header = gsheet.get('values', [])[0]   # Assumes first line is header!
    values = gsheet.get('values', [])[1:]  # Everything else is data.
    if not values:
        print('No data found.')
    else:
        all_data = []
        for row in values:
            json_obj={}
            col_index = 0
            for col in row:
                if col_index < len(header):
                    if row[col_index].find('{') == -1:
                        json_obj[header[col_index]]=row[col_index]
                    else:
                        json_obj[header[col_index]] = json.loads(row[col_index])
                    col_index += 1
                else:
                    break

            all_data.append(json_obj)

    return all_data

if __name__ == '__main__':

    # emailAddress= 'wangez@google.com'
    # table_name='allen-first.mocha_dataflow.sample_data'
    path_to_credential = '/Users/wangez/Downloads/allen-first-a3f52ad630d6.json'
    sheet_url='https://docs.google.com/spreadsheets/d/1ArgVsUvRhAIDJy5KSM3jm14tmLE46wCIG37NaaWMcnY/edit?ts=60a76865#gid=1617533670'
    getid = '^.*/d/(.*)/.*$'
    pattern = re.compile(getid, re.IGNORECASE)
    sheet_id = pattern.findall(sheet_url)[0]

    # sheet_id = '18-Mch32zJ_vSbsT-5cxvSfzOXgoXNgHljcIjglAx-B4'
    range1 = 'Other Specification'
    range2= 'Mocha Feature'
    event_json = {}
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential

    credentials = service_account.Credentials.from_service_account_file(path_to_credential)
    scopes = credentials.with_scopes(
        [
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
    )
    sheet_service = build("sheets", "v4", credentials=credentials)

    gsheet = sheet_service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range1).execute()
    header = gsheet.get('values', [])[0]   # Assumes first line is header!
    values = gsheet.get('values', [])[1:]  # Everything else is data.
    if not values:
        print('No data found.')
    else:
        for row in values:
            if row[0] == 'days_look_back':
                if len(row)>1:
                    event_json[row[0]]=int(row[1])
                else:
                    event_json[row[0]] = 60
            elif row[0] == 'event_date_field':
                if len(row)>1:
                    event_json[row[0]]=row[1]
                else:
                    raise ValueError('No event date field is specified')
            elif row[0] == 'event_date_format':
                if len(row)>1:
                    event_json[row[0]]=row[1]
                else:
                    event_json[row[0]]='YYYYMMDD'
            elif row[0] == 'date_function':
                if len(row)>1:
                    event_json[row[0]]=row[1]
                else:
                    event_json[row[0]]='current_date()'
            elif row[0] == 'table_name':
                if len(row)>1:
                    event_json[row[0]]=row[1]
                else:
                    raise ValueError('No table name is specified')
            elif row[0] == 'event_name_field':
                if len(row)>1:
                    event_json[row[0]]=row[1]
                else:
                    raise ValueError('No event name field is specified')
            elif row[0] == 'universal_user_id':
                if len(row)>1:
                    event_json[row[0]]=row[1]
                else:
                    raise ValueError('No universal_user_id is specified')
            elif row[0] == 'event_date_type':
                if len(row)>1:
                    event_json[row[0]]=row[1]
                else:
                    event_json[row[0]]=''
            elif row[0] == 'pay_events':
                if len(row)>1:
                    if row[1].find(',')==-1:
                        list_events=[]
                        list_events.append(row[1])
                        event_json[row[0]]=list_events
                    else:
                        event_json[row[0]]=row[1].split(',')
                else:
                    raise ValueError("No payment events is specified")
            elif row[0] == 'login_events':
                if len(row)>1:
                    if row[1].find(',')==-1:
                        list_events = []
                        list_events.append(row[1])
                        event_json[row[0]] = list_events
                    else:
                        event_json[row[0]]=row[1].split(',')
                else:
                    raise ValueError("No login events is specified")
            else:
               if len(row)>1:
                   event_json[row[0]]=row[1]
               else:
                   event_json[row[0]]=''





    gsheet = sheet_service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range2).execute()
    event_json['event_aggregation']=gsheet2json(gsheet)
    print(event_json)

    with open('config.json.temp', 'w') as outfile:
        json.dump(event_json, outfile,indent=4)
