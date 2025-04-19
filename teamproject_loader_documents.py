import datetime
import pandas as pd
from dateutil.tz import tzoffset
from loader_to_google import connection_to_sheets, clear_table, update_table
from settings import *
from teamproject_loader_temp_result import get_teams
from utils import print_log, requests_get, multiprocessing_map


def get_results_team(x):
    title, sub_title, id = x['title'], x['instanceNumber'], x['id']

    title = title.replace('. ', '.')
    sub_title = str(sub_title).zfill(2)

    documents = requests_get(f'{DOMAIN}/api/v2/workspaces/{id}/documents/results').json()

    report = 'Есть' if documents['reportId'] is not None else 'Нет'
    presentation = 'Есть' if documents['presentationId'] is not None else 'Нет'

    title_str = f'=ГИПЕРССЫЛКА("{DOMAIN}/#/{id}/documents/results"; "{title}.{sub_title}")'
    return title_str, report, presentation


if __name__ == '__main__':
    get_data = get_teams()

    teams = pd.DataFrame(get_data, columns=['title', 'instanceNumber', 'id'])

    print_log(f"Загрузка документов команд")
    result = multiprocessing_map(get_results_team, teams.to_dict('records'))

    result = pd.DataFrame(result, columns=['title', 'report', 'presentation'])

    print_log(f"Выгрузка документов в Гугл таблицу")
    service = connection_to_sheets()
    clear_table(service, DOCUMENTS_LIST, DOCUMENTS_LIST['range'])
    update_table(service, DOCUMENTS_LIST, 'A2', [result.columns.tolist()] + result.values.tolist())
    tzinfo = tzoffset(None, 5 * 3600)
    update_table(service, DOCUMENTS_LIST, 'A1', [[f"Обновлено: {datetime.datetime.now(tzinfo).strftime('%d-%m-%Y %H:%M')}"]])
    print_log(f"Выгрузка успешно завершена")
