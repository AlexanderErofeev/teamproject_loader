import datetime
import multiprocessing
from loader_to_google import *
import math
import requests
import pandas as pd
from bs4 import BeautifulSoup
from loader_settings import *


def get_authorization():
    print_log(f"Авторизация пользователя: {USERNAME}")

    session = requests.Session()
    login_page = session.get(AUTH_PAGE_URL)
    login_form = BeautifulSoup(login_page.text, 'html.parser')\
        .body\
        .find('form', attrs={'id': 'kc-form-login'})\
        .get('action')
    logining_post = session.post(login_form, allow_redirects=False, data={
        'username': USERNAME,
        'password': PASSWORD
    })
    code = logining_post.headers['Location'].split('&code=')[1]
    authorization = session.post(AUTH_TOKEN_URL, data={
        'code': code,
        'grant_type': 'authorization_code',
        'client_id': 'teampro',
        'redirect_uri': f'{DOMAIN}/#/'
    }).json()
    authorization = f"{authorization['token_type']} {authorization['access_token']}"

    print_log(f"Авторизация успешно завершена")
    return authorization


def requests_put(url, post_data):
    r = None
    while r is None:
        try:
            r = requests.put(url, json=post_data, headers=HEADERS)
        except requests.exceptions.Timeout:
            print_log(f"Ошибка timeout с URL: {url}", is_error=True)
        except requests.exceptions.ConnectionError:
            print_log(f"Ошибка с URL: {url}", is_error=True)
    return r


def requests_get(url):
    r = None
    while r is None:
        try:
            r = requests.get(url, headers=HEADERS, timeout=5)
        except requests.exceptions.Timeout:
            print_log(f"Ошибка timeout с URL: {url}", is_error=True)
        except requests.exceptions.ConnectionError:
            print_log(f"Ошибка с URL: {url}", is_error=True)
    return r


def sum_mas(list_lists):
    ans_mas = []
    for el in list_lists:
        ans_mas += el
    return ans_mas


def print_log(log_string, is_error=False):
    text = f"[{datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')}] {log_string}"
    print("\033[31m{}\033[0m".format(text)) if is_error else print(text)


def get_result_iteration(members, s2s, c2s):
    correct_count_score = len(members) - 1

    if len(s2s.index) != 0:
        s2s_dict = s2s.groupby(['dst_user_id']).apply(lambda grup: grup['score'].mean()).to_dict()
        s2s_dict = {member: s2s_dict[member] if member in s2s_dict else 0 for member in members.keys()}

        bad_student = s2s.groupby(['src_user_id']).apply(lambda grup: len(grup.index) < correct_count_score).to_dict()
        bad_student = {member: bad_student[member] if member in bad_student else True for member in members.keys()}
    else:
        s2s_dict = {member: 0 for member in members.keys()}
        bad_student = {member: True for member in members.keys()}

    if len(c2s.index) != 0:
        c2s_dict = c2s.set_index('dst_user_id')['score'].to_dict()
        c2s_dict = {member: c2s_dict[member] if member in c2s_dict else 0 for member in members.keys()}
    else:
        c2s_dict = {member: 0 for member in members.keys()}

    students_result = {fio: round(c2s_dict[id] * 0.7) if bad_student[id] else round(s2s_dict[id] * 0.3 + c2s_dict[id] * 0.7) for id, fio in members.items()}
    return pd.DataFrame([students_result])


def get_results_team(x):
    title, sub_title, id = x['title'], x['sub_title'], x['id']
    print_log(f"Выгрузка команды: {title} ({sub_title})")

    if math.isnan(id):
        print_log(f"Нет в тимпроджекте: {title} ({sub_title})", is_error=True)
        return

    details = requests_get(f'{DOMAIN}/api/projects/{str(int(id))}/estimation/details/').json()
    iterations = details['iterations']
    members = pd.DataFrame(details['members']).set_index('user_id')['fullname'].to_dict()

    if len(iterations) == 0:
        print_log(f"Нет итераций: {title} ({sub_title})", is_error=True)
        return

    s2s_scores = [el['scores']['s2s'] for el in iterations]
    s2s_scores = [pd.DataFrame(el) for el in s2s_scores]

    c2s_scores = [el['scores']['c2s'] for el in iterations]
    c2s_scores = [pd.DataFrame(el) for el in c2s_scores]

    result_mas = []
    for i in range(len(iterations)):
        result_mas.append(get_result_iteration(members, s2s_scores[i], c2s_scores[i]))

    df = pd.concat(result_mas).T
    df.columns = range(1, 1 + len(df.columns))

    df['coefficient'] = df.sum(axis=1)
    max_score = df['coefficient'].max()
    df['coefficient'] = df['coefficient'].apply(lambda el: round(el / max_score, 2))

    title_str = f'=ГИПЕРССЫЛКА("{DOMAIN}/#/{str(int(id))}/rating/estimate"; "{title} ({sub_title})")'
    df.insert(0, 'title', [title_str] * len(members))
    return df


HEADERS = {
    'Authorization': STATIC_TOKEN if IS_STATIC_TOKEN else get_authorization()
}


if __name__ == '__main__':
    # requests_put('{DOMAIN}/api/projects/38129/iterations/30828/estimate/curator/', {
    #   "score": "54",
    #   "comment": "Нормик"
    # })

    df = pd.read_csv(TEAMS_FILE, usecols=['title_id', 'Куратор для ИТС'])
    # df = df[df['Куратор для ИТС'] == 'Шадрин Д.Б.']
    teams_list = [el.split('\\л') for el in df['title_id'].tolist()]
    print_log(f"Всего студентов в ведомости: {len(teams_list)}")

    teamproject_counts = requests_get(f'{DOMAIN}/api/projects/?status={STATUS}&year={YEAR}&semester={SEMESTR}&search={SEARCH_PREFIX}').json()['count']
    print_log(f"Всего студентов в teamproject: {teamproject_counts}")

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

    if IS_USES_MULTIPROCESSING:
        with multiprocessing.Pool(PROCESSOR_COUNT) as p:
            result = p.map(get_results_team, teams.to_dict('records'))
    else:
        result = [get_results_team(el) for el in teams.to_dict('records')]

    result = pd.concat(result)\
        .reset_index(names=['fio'])\
        .fillna(0)\
        .convert_dtypes()

    result.to_csv(RESULT_FILE, index=False)

    print_log(f"Загрузка результатов в Гугл таблицы")
    service = connection_to_sheets()
    clear_table(service, 'A:G')
    update_table(service, 'A2', [result.columns.tolist()] + result.values.tolist())
    update_table(service, 'A1', [[f"Обновлено: {datetime.datetime.now().strftime('%d-%m-%Y %H:%M')}"]])

    print_log(f"Загрузка успешно завершена")
