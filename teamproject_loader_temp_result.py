import datetime
from dateutil.tz import tzoffset
from loader_to_google import *
import math
import pandas as pd
from settings import *
from utils import print_log, requests_get, sum_mas, multiprocessing_map


def get_result_iteration(members, s2s, c2s):
    bad_student = []
    for i, row in s2s.iterrows():
        if row.isna().sum() > 1:
            bad_student.append(i)

    s2s_dict = s2s.T.apply(lambda s: s.dropna().mean(), axis=1).fillna(0).to_dict()
    c2s_dict = c2s.iloc[:, 0].fillna(0).to_dict()

    students_result = {fio: round(c2s_dict[id] * 0.7, 2) if id in bad_student else round(s2s_dict[id] * 0.3 + c2s_dict[id] * 0.7, 2) for id, fio in members.items()}
    return pd.DataFrame([students_result])


def get_results_team(x):
    title, sub_title, id = x['title'], x['instanceNumber'], x['id']
    # print_log(f"Загрузка команды: {title} ({sub_title})")

    details = requests_get(f'{DOMAIN}/api/v2/workspaces/{id}/scores/details').json()
    iterations = details['iterations']
    members = pd.DataFrame(details['students'])['fullname'].to_dict()

    if len(iterations) == 0:
        print_log(f"Нет итераций: {title} ({sub_title})", is_error=True)
        return

    s2s_scores = [el['scores']['studentToStudent'] for el in iterations]
    s2s_scores = [pd.DataFrame(el) for el in s2s_scores]

    c2s_scores = [el['scores']['curatorToStudent'] for el in iterations]
    c2s_scores = [pd.DataFrame(el) for el in c2s_scores]

    result_mas = []
    for i in range(len(iterations)):
        result_mas.append(get_result_iteration(members, s2s_scores[i], c2s_scores[i]))

    df = pd.concat(result_mas).T
    df.columns = range(1, 1 + len(df.columns))

    df['coefficient'] = df.sum(axis=1)
    max_score = df['coefficient'].max()
    df['coefficient'] = df['coefficient'].apply(lambda el: round(el / max_score, 2))

    title_str = f'=ГИПЕРССЫЛКА("{DOMAIN}/#/{id}/rating/result"; "{title} ({sub_title})")'
    df.insert(0, 'title', [title_str] * len(members))
    return df


def get_teams():
    print_log(f"Поиск команд в teamproject")
    teamproject_counts = 0
    get_data = []
    for search_prefix in SEARCH_PREFIXS:
        temp_teamproject_counts = requests_get(f'{DOMAIN}/api/v2/workspaces/?status=any&year={YEAR}&semester={SEMESTR}&search={search_prefix}').json()['total']
        for i in range(1, math.ceil(temp_teamproject_counts / PER_PAGE_RESULTS) + 1):
            get_data.append(requests_get(f'{DOMAIN}/api/v2/workspaces/?status=any&year={YEAR}&semester={SEMESTR}&size={PER_PAGE_RESULTS}&page={i}&search={search_prefix}').json()['items'])
        teamproject_counts += temp_teamproject_counts
    get_data = sum_mas(get_data)
    print_log(f"Всего команд в teamproject: {teamproject_counts}")
    print_log(f"Получено команд из teamproject: {len(get_data)}")
    return get_data


if __name__ == '__main__':
    get_data = get_teams()

    teams = pd.DataFrame(get_data, columns=['title', 'instanceNumber', 'id'])

    print_log(f"Загрузка результатов команд")
    result = multiprocessing_map(get_results_team, teams.to_dict('records'))

    result = pd.concat(result)\
        .reset_index(names=['fio'])\
        .fillna(0)\
        .convert_dtypes()

    print_log(f"Выгрузка результатов в Гугл таблицу")
    service = connection_to_sheets()
    clear_table(service, TEMP_RESULT_LIST, TEMP_RESULT_LIST['range'])
    update_table(service, TEMP_RESULT_LIST, 'A2', [result.columns.tolist()] + result.values.tolist())
    tzinfo = tzoffset(None, 5 * 3600)
    update_table(service, TEMP_RESULT_LIST, 'A1', [[f"Обновлено: {datetime.datetime.now(tzinfo).strftime('%d-%m-%Y %H:%M')}"]])
    print_log(f"Выгрузка успешно завершена")
