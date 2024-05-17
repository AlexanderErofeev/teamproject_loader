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

    documents = requests_get(f'{DOMAIN}/api/projects/{str(int(id))}/documents/').json()['items']
    report = [d for d in documents if d['id'] == "DTRP"]
    presentation = [d for d in documents if d['id'] == "DTPN"]

    report = len(report)
    presentation = len(presentation)

    title_str = f'=ГИПЕРССЫЛКА("{DOMAIN}/#/{str(int(id))}/rating/estimate"; "{title} ({sub_title})")'
    return title_str, report, presentation


if __name__ == '__main__':
    print_log(f"Загрузка команд из ведомости")
    service = connection_to_sheets()
    teams_list = get_table(service, INPUT_LIST_NAME, f'{INPUT_COLUMN}:{INPUT_COLUMN}')
    teams_list = [el[0].split('\\л') for el in teams_list if len(el) > 0]
    print_log(f"Всего команд в ведомости: {len(teams_list)}")

    print_log(f"Поиск команд в teamproject")
    teamproject_counts = requests_get(f'{DOMAIN}/api/projects/?status={STATUS}&year={YEAR}&semester={SEMESTR}&search={SEARCH_PREFIX}').json()['count']
    print_log(f"Всего команд в teamproject: {teamproject_counts}")

    get_data = []
    for i in range(1, math.ceil(teamproject_counts / PER_PAGE_RESULTS) + 1):
        get_data.append(requests_get(f'{DOMAIN}/api/projects/?status={STATUS}&year={YEAR}&semester={SEMESTR}&size={PER_PAGE_RESULTS}&page={i}&search={SEARCH_PREFIX}').json()['items'])
    get_data = sum_mas(get_data)

    for team in teams_list:
        mas = [t['id'] for t in get_data if t['title'] == team[0] and t['instance_number'] == int(team[1])]
        if len(mas) == 1:
            team.append(mas[0])

    teams = pd.DataFrame(teams_list, columns=['title', 'sub_title', 'id'])
    teams.to_csv(TEAMS_ID_FILE, index=False)

    if IS_USES_THREADING:
        with multiprocessing.dummy.Pool(THREAD_COUNT) as p:
            result = p.map(get_results_team, teams.to_dict('records'))
    else:
        result = [get_results_team(el) for el in teams.to_dict('records')]

    result = pd.DataFrame(result, columns=['title', 'report', 'presentation'])

    result.to_csv('documents', index=False)

    print_log(f"Выгрузка результатов в Гугл таблицу")

    clear_table(service, 'Документы', 'A:C')
    update_table(service, 'Документы', 'A2', [result.columns.tolist()] + result.values.tolist())
    update_table(service, 'Документы', 'A1', [[f"Обновлено: {datetime.datetime.now().strftime('%d-%m-%Y %H:%M')}"]])

    print_log(f"Выгрузка успешно завершена")
