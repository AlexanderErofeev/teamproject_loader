import datetime
import multiprocessing.dummy
from loader_to_google import *
import math
import pandas as pd
from settings import *
from utils import print_log, requests_get, sum_mas


def get_results_team(x):
    title, sub_title, id = x['title'], x['sub_title'], x['id']
    print_log(f"Загрузка команды: {title} ({sub_title})")

    if math.isnan(id):
        print_log(f"Нет в тимпроджекте: {title} ({sub_title})", is_error=True)
        return

    result = requests_get(f'{DOMAIN}/api/projects/{str(int(id))}/results/').json()['members']
    count_members = len(result)
    result = {student['fullname']: student['final'] for student in result}

    df = pd.DataFrame({'fio': result.keys(), 'result': result.values()})

    df.insert(1, 'title', [title] * count_members)
    df.insert(2, 'sub_title', ['л' + sub_title] * count_members)

    return df


if __name__ == '__main__':
    print_log(f"Загрузка команд из ведомости")
    service = connection_to_sheets()
    teams_list = get_table(service, TEAMS_INPUT_LIST, TEAMS_INPUT_LIST['range'])
    teams_list = [el[0].split('\\л') for el in teams_list if len(el) > 0]
    print_log(f"Всего команд в ведомости: {len(teams_list)}")

    print_log(f"Поиск команд в teamproject")
    teamproject_counts = requests_get(f'{DOMAIN}/api/projects/?year={YEAR}&semester={SEMESTR}&search={SEARCH_PREFIX}').json()['count']
    print_log(f"Всего команд в teamproject: {teamproject_counts}")

    get_data = []
    for i in range(1, math.ceil(teamproject_counts / PER_PAGE_RESULTS) + 1):
        get_data.append(requests_get(f'{DOMAIN}/api/projects/?year={YEAR}&semester={SEMESTR}&size={PER_PAGE_RESULTS}&page={i}&search={SEARCH_PREFIX}').json()['items'])
    get_data = sum_mas(get_data)

    for team in teams_list:
        mas = [t['id'] for t in get_data if t['title'] == team[0] and t['instance_number'] == int(team[1])]
        if len(mas) == 1:
            team.append(mas[0])

    teams = pd.DataFrame(teams_list, columns=['title', 'sub_title', 'id'])

    if IS_USES_THREADING:
        with multiprocessing.dummy.Pool(THREAD_COUNT) as p:
            result = p.map(get_results_team, teams.to_dict('records'))
    else:
        result = [get_results_team(el) for el in teams.to_dict('records')]

    result = pd.concat(result)

    print_log(f"Выгрузка результатов в Гугл таблицу")

    clear_table(service, RESULT_LIST, RESULT_LIST['range'])
    update_table(service, RESULT_LIST, 'A2', [result.columns.tolist()] + result.values.tolist())
    update_table(service, RESULT_LIST, 'A1', [[f"Обновлено: {datetime.datetime.now().strftime('%d-%m-%Y %H:%M')}"]])

    print_log(f"Выгрузка успешно завершена")
