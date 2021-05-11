import os
import json
import numpy as np
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
import googleapiclient
import google.auth
from datetime import date


def create_sheet(title, customer, data):
    sheets_service = build("sheets", "v4", credentials=credentials)
    sheets = sheets_service.spreadsheets()

    create_body = {"properties": {"title": f"{title} {customer} {date.today()}"},
                   "sheets": list(map(lambda d: {"properties": {"title": d.get("title")}}, data))}
    res = sheets.create(body=create_body).execute()
    spreadsheet_id = res.get("spreadsheetId")
    sheet_id=res.get("sheets")[0].get('properties').get("sheetId")
    # print('1')
    def df_to_sheet(df):
        df_columns = [np.array(df.columns)]
        df_values = df.values.tolist()
        df_to_sheet = np.concatenate((df_columns, df_values)).tolist()
        return df_to_sheet

    update_body = {
        "valueInputOption": "RAW",
        "data": list(map(lambda d: {"range": d.get("title"), "values": df_to_sheet(d.get("df"))}, data)),
    }
    sheets.values().batchUpdate(spreadsheetId=spreadsheet_id, body=update_body).execute()

    request={
        'requests': [
            {
                'setDataValidation': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 1,
                        # 'endRowIndex': 1,
                        'startColumnIndex': 2,
                        'endColumnIndex': 3
                    },
                    'rule': {
                        'condition': {
                            'type': 'ONE_OF_LIST',
                            'values': [
                                {
                                    'userEnteredValue': 'flag',
                                },
                                {
                                    'userEnteredValue': 'sum',
                                },
                                {
                                    'userEnteredValue': 'count',
                                }
                            ]
                        },
                        'showCustomUi': True,
                        'strict': True
                    }
                }
            }
        ]
    }
    # request=json.dumps(request)
    # print(request)
    sheets.batchUpdate(spreadsheetId=spreadsheet_id, body=request).execute()

    print(res)
    return res

def share_spreadsheet(spreadsheet_id, options, notify=True):
    drive_service = build("drive", "v3", credentials=credentials)

    res = (
        drive_service.permissions()
        .create(
            fileId=spreadsheet_id,
            body=options,
            sendNotificationEmail=notify,
        )
        .execute()
    )

    return res

if __name__ == '__main__':
    path_to_credential='/Users/wangez/Downloads/allen-first-9d553840c659.json'
    emailAddress= 'wangez@google.com'
    table_name='allen-first.mocha_dataflow.sample_data'
    legacy_spreadsheet_id='1NYDd-Fx1ZVpP0x3S2Bt-nRDqfWMdMBZ1EeQNEm_CUTI'
    legacy_sheet_id='1348826752'
    search_ranges='A1:D1000'

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential

    credentials = service_account.Credentials.from_service_account_file(path_to_credential)
    scopes = credentials.with_scopes(
        [
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
    )

    client = bigquery.Client()
    QUERY = (
        'select distinct event_name from `{}`'.format(table_name)
    )
    df_test = client.query(QUERY).result().to_dataframe()
    df_test['Description'] = ''
    df_test['Aggregation'] = ''
    df_test['Event_value'] = ''
    df_test.head(1)

    list_extra = ['table_name', 'event_date_field', 'event_name_field', 'universal_user_id', 'days_look_back',
                  'event_date_format', 'pay_events', 'login_events', 'install_event','start_date','date_function','event_date_type']
    df_extra = pd.DataFrame({'params': list_extra})
    df_extra['value'] = ''
    df_extra.head(1)

    if legacy_spreadsheet_id is not None or legacy_spreadsheet_id != '':
        legacy_sheets_service = build("sheets", "v4", credentials=credentials)
        legacy_sheets = legacy_sheets_service.spreadsheets()
        legacy_input=legacy_sheets.values().get(spreadsheetId=legacy_spreadsheet_id,range=search_ranges).execute()
        input_value=legacy_input.get('values',[])
        if not input_value:
            print('No data found in legacy google sheet')

        df_legacy=pd.DataFrame(input_value[1:])
        columns=input_value[0]
        if len(df_legacy.columns) == len(columns):
            df_legacy.columns=columns
        else:
            df_legacy.columns=['event_name','Description','Aggregation']
            df_legacy['Event_value']=''

        df_test=pd.merge(df_legacy,df_test,on='event_name',how='outer',suffixes=('_left','_right'))


        def row_combine(row):
            def first_valid_value(x, y):
                if x is None:
                    return y
                else:
                    return x
            row['Description'] = first_valid_value(row['Description_left'],row['Description_right'])
            row['Aggregation'] = first_valid_value(row['Aggregation_left'], row['Aggregation_right'])
            row['Event_value'] = first_valid_value(row['Event_value_left'], row['Event_value_right'])
            return row

        df_test=df_test.apply(lambda row:row_combine(row),axis=1)
        df_test=df_test[['event_name','Description','Aggregation','Event_value']]
        df_test=df_test.replace(np.nan,'',regex=True)

    data = [
        {
            "title": "Mocha Feature",
            "df": df_test
        },
        {
            "title": "Other Specification",
            "df": df_extra
        }
    ]
    options = {
        "role": "writer",
        "type": "user",
        "emailAddress": emailAddress
    }
    res = create_sheet("Mocha", "Test", data=data)
    res = share_spreadsheet(res.get("spreadsheetId"), options=options, notify=False)