from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from loader_settings import *


def connection_to_sheets():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPES)
    return build('sheets', 'v4', credentials=credentials)


def update_table(service, cell, values):
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        valueInputOption='USER_ENTERED',
        range=LIST_NAME + '!' + cell,
        body={"values": values}
    ).execute()


def clear_table(service, cell):
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=LIST_NAME + '!' + cell,
    ).execute()
