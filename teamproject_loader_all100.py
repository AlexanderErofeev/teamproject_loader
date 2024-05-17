import multiprocessing.dummy
from loader_to_google import *
import math
import pandas as pd
from settings import *
from utils import requests_get, print_log, requests_put


def load_100(id):
    team = requests_get(f"{DOMAIN}/api/projects/{id}/results/").json()
    users_id = [el['id'] for el in team["members"]]

    if requests_get(f"{DOMAIN}/api/projects/{id}/results/").json()["curator"]['fullname'] != "Шадрин Денис Борисович":
        return

    iter_id = requests_get(f"{DOMAIN}/api/projects/{id}/iterations/").json()["items"]
    iter_id = [el['id'] for el in iter_id]

    if len(iter_id) != 4:
        print_log(f'Итераций не 4 у команды {id}', is_error=True)
        return

    for user_id in users_id:
        req = requests_put(f'{DOMAIN}/api/projects/{id}/iterations/{iter_id[2]}/estimate/members/{user_id}/', {
            "score": "100",
            "comment": ""
        })

        if 'success' in req.json() and req.json()['success'] == True:
            print_log(f"Загрузка 100 для команды {id} и прользователья {user_id}: {req.json()}")
        else:
            print_log(f"Загрузка 100 для команды {id} и прользователья {user_id}: {req.json()}", is_error=True)
            error_mas.append(id)


if __name__ == '__main__':
    print_log(f"Загрузка команд из ведомости")
    service = connection_to_sheets()
    teams_list = get_table(service, TEAMS_INPUT_LIST, TEAMS_INPUT_LIST['range'])
    teams_list = [el[0].split('\\л') for el in teams_list if len(el) > 0]
    print_log(f"Всего команд в ведомости: {len(teams_list)}")

    print_log(f"Поиск команд в teamproject")
    teamproject_counts = \
    requests_get(f'{DOMAIN}/api/projects/?year={YEAR}&semester={SEMESTR}&search={SEARCH_PREFIX}').json()['count']
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

    error_mas = []

    if IS_USES_THREADING:
        with multiprocessing.dummy.Pool(THREAD_COUNT) as p:
            result = p.map(load_100, teams['id'].tolist())
    else:
        result = [load_100(el) for el in teams.to_dict('records')]

    print(list(set(error_mas)))




