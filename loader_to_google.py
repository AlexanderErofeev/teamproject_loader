from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from settings import CREDENTIALS_FILE, SCOPES


def connection_to_sheets():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPES)
    return build('sheets', 'v4', credentials=credentials)


def get_table(service, list_dict, range_table):
    return service.spreadsheets().values().get(
        spreadsheetId=list_dict['spreadsheet_id'],
        range=list_dict['list_name'] + '!' + range_table,
    ).execute()['values']


def update_table(service, list_dict, cell, values):
    service.spreadsheets().values().update(
        spreadsheetId=list_dict['spreadsheet_id'],
        valueInputOption='USER_ENTERED',
        range=list_dict['list_name'] + '!' + cell,
        body={"values": values}
    ).execute()


def clear_table(service, list_dict, cell):
    service.spreadsheets().values().clear(
        spreadsheetId=list_dict['spreadsheet_id'],
        range=list_dict['list_name'] + '!' + cell,
    ).execute()
