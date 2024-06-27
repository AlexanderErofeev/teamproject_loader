import datetime
import pandas as pd
from dateutil.tz import tzoffset
from loader_to_google import connection_to_sheets, clear_table, update_table
from settings import *
from teamproject_loader_temp_result import get_teams
from utils import print_log, requests_get, multiprocessing_map, sum_mas


def get_results_team(x):
    title, sub_title, id = x['title'], x['instanceNumber'], x['id']

    result = []
    iterations = requests_get(f'{DOMAIN}/api/v2/workspaces/{id}/widgets/progress').json()['iterations']

    if len(iterations) != 4:
        print_log(f'Неправильное количество итераций у команды {title}')

    for iteration in iterations:
        result += [iteration['dates']['beginning'], iteration['dates']['ending']]

    title_str = f'=ГИПЕРССЫЛКА("{DOMAIN}/#/{id}/about"; "{title} ({sub_title})")'
    return title_str, *result


if __name__ == '__main__':
    get_data = get_teams()

    teams = pd.DataFrame(get_data, columns=['title', 'instanceNumber', 'id'])

    print_log(f"Загрузка дат итераций команд")
    result = multiprocessing_map(get_results_team, teams.to_dict('records'))

    result = pd.DataFrame(result, columns=['title', *['beginning', 'ending'] * 4])

    print_log(f"Выгрузка дат итераций в Гугл таблицу")
    service = connection_to_sheets()
    clear_table(service, DATES_LIST, DATES_LIST['range'])
    update_table(service, DATES_LIST, 'A2', [result.columns.tolist()] + result.values.tolist())
    tzinfo = tzoffset(None, 5 * 3600)
    update_table(service, DATES_LIST, 'B1', [['Анализ']])
    update_table(service, DATES_LIST, 'D1', [['Проектирование']])
    update_table(service, DATES_LIST, 'F1', [['Разработка']])
    update_table(service, DATES_LIST, 'H1', [['Защита']])
    update_table(service, DATES_LIST, 'A1', [[f"Обновлено: {datetime.datetime.now(tzinfo).strftime('%d-%m-%Y %H:%M')}"]])
    print_log(f"Выгрузка успешно завершена")
