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

    def df_to_sheet(df):
        df_columns = [np.array(df.columns)]
        df_values = df.values.tolist()
        df_to_sheet = np.concatenate((df_columns, df_values)).tolist()
        return df_to_sheet

    update_body = {
        "valueInputOption": "RAW",
        "data": list(map(lambda d: {"range": d.get("title"), "values": df_to_sheet(d.get("df"))}, data))
    }

    sheets.values().batchUpdate(spreadsheetId=spreadsheet_id, body=update_body).execute()

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
    res = share_spreadsheet(res.get("spreadsheetId"), options=options, notify=True)