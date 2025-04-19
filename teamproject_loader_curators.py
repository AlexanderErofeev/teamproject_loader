import datetime
from dateutil.tz import tzoffset
from loader_to_google import *
import pandas as pd
from settings import *
from teamproject_loader_temp_result import get_teams
from utils import print_log, requests_get, multiprocessing_map, requests_put

UNKNOWN_SCORE = 'Нет оценки'
TYPICAL_TEAM_ID = '0cd2a112-bcee-4734-80e5-2ae5082d922f'


def get_grading_iterations():
    grading_iterations = []
    iterations = requests_get(f'{DOMAIN}/api/v2/workspaces/{TYPICAL_TEAM_ID}/iterations').json()
    for iteration in iterations:
        iteration_detale = requests_get(f"{DOMAIN}/api/v2/iterations/{iteration['id']}/scores").json()['iteration']
        if iteration_detale['isGradingOpened']:
            grading_iterations.append(iteration['title'])
    return grading_iterations


def get_result_iteration(members, c2s):
    c2s_dict = c2s.iloc[:,0].to_dict()
    students_result = {fio: c2s_dict[id] for id, fio in members.items()}
    return pd.DataFrame([students_result])


def get_results_team(x):
    title, sub_title, id = x['title'], x['instanceNumber'], x['id']

    title = title.replace('. ', '.')
    sub_title = str(sub_title).zfill(2)

    details = requests_get(f'{DOMAIN}/api/v2/workspaces/{id}/scores/details').json()
    iterations = details['iterations']
    members = pd.DataFrame(details['students'])['fullname'].to_dict()

    if len(iterations) != len(ITERATIONS):
        print_log(f"Неправильное количество итераций: {title} ({sub_title})", is_error=True)
        return

    c2s_scores = [el['scores']['curatorToStudent'] for el in iterations]
    c2s_scores = [pd.DataFrame(el) for el in c2s_scores]

    result_mas = []
    for i in range(len(iterations)):
        result_mas.append(get_result_iteration(members, c2s_scores[i]))

    df = pd.concat(result_mas).T
    df.columns = [iter['title'] for iter in ITERATIONS]

    linc_str = f'=ГИПЕРССЫЛКА("{DOMAIN}/#/{id}/rating/estimate"; "{title}.{sub_title}")'
    df.insert(0, 'Ссылка teamproject', [linc_str] * len(members))
    df.insert(0, 'team id teamproject', [id] * len(members))
    return df


def get_curator_scores():
    get_data = get_teams()
    get_data = [team for team in get_data if team['mainCurator']['fullname'] == 'Шадрин Денис Борисович']
    print_log(f"Получено команд Шадрина Дениса Борисовича из teamproject: {len(get_data)}")

    teams = pd.DataFrame(get_data, columns=['title', 'instanceNumber', 'id'])
    print_log(f"Загрузка результатов команд")
    result = multiprocessing_map(get_results_team, teams.to_dict('records'))

    result = pd.concat(result)\
        .reset_index(names=['fio'])\
        .fillna(UNKNOWN_SCORE)\
        .convert_dtypes()

    return result


def update_scores(global_start_scores, global_target_scores):
    iterations = get_grading_iterations()
    print_log(f"Обновление баллов за итерации: {', '.join(list(iterations))}")
    for iteration in iterations:
        print_log(f"Обновление итерации: {iteration}")
        start_scores = global_start_scores[global_start_scores[iteration] != UNKNOWN_SCORE]
        target_scores = global_target_scores[global_target_scores[iteration] != UNKNOWN_SCORE]

        old_students = set(target_scores['fio']) & set(start_scores['fio'])
        new_students = set(target_scores['fio']) - old_students

        missing_students = set(start_scores['fio']) - old_students
        if len(missing_students) != 0:
            print_log(f"Нет студентов: {', '.join(list(missing_students))}", is_error=True)
            # return

        updated_students = []
        for student in old_students:
            ss = start_scores[start_scores['fio'] == student].iloc[0]
            ts = target_scores[target_scores['fio'] == student].iloc[0]
            if int(ss[iteration]) != int(ts[iteration]):
                updated_students.append(student)

        updated_students += new_students
        print_log(f"Количество обновляемых студентов: {len(updated_students)}")
        if len(updated_students) > 0:
            print_log(f"Обновление данных для студентов: {', '.join(updated_students)}")
            updated_students = target_scores[target_scores['fio'].isin(updated_students)]
            updated_students.apply(lambda row: update_student_score(row, iteration), axis=1)


def update_student_score(row, iteration):
    print_log(f"Загрузка обновлённого балла для студента {row['fio']}: {row[iteration]}")

    if not (row[iteration].isdigit() and 0 <= int(row[iteration]) <= 100):
        print_log(f"Uncorrected score for {row['fio']}: \"{row[iteration]}\"", is_error=True)
        return

    iterations = requests_get(f"{DOMAIN}/api/v2/workspaces/{row['team id teamproject']}/iterations").json()
    iteration_id = next(x['id'] for x in iterations if x['title'] == iteration)

    thematic_groups = requests_get(f"{DOMAIN}/api/v2/iterations/{iteration_id}/scores").json()['thematicGroups']
    if len(thematic_groups) != 1:
        print_log(f"len thematic_groups {len(thematic_groups)}", is_error=True)
        return
    student_id = next(x['studentId'] for x in thematic_groups[0]['students'] if x['person']['fullname'] == row['fio'])

    requests_put(f'{DOMAIN}/api/v2/iterations/{iteration_id}/grades/students/{student_id}/', {
        "score": row[iteration],
    })


def main():
    service = connection_to_sheets()

    start_scores = get_curator_scores()

    teams_list = get_table(service, CURATOR_INPUT_LIST, CURATOR_INPUT_LIST['range'])[1:]
    target_scores = pd.DataFrame(teams_list[1:], columns=teams_list[0])

    update_scores(start_scores, target_scores)

    finish_scores = get_curator_scores()

    print_log(f"Выгрузка результатов в Гугл таблицу")
    clear_table(service, CURATOR_OUTPUT_LIST, CURATOR_OUTPUT_LIST['range'])
    update_table(service, CURATOR_OUTPUT_LIST, 'A2', [finish_scores.columns.tolist()] + finish_scores.values.tolist())
    tzinfo = tzoffset(None, 5 * 3600)
    update_table(service, CURATOR_OUTPUT_LIST, 'A1', [[f"Обновлено: {datetime.datetime.now(tzinfo).strftime('%d-%m-%Y %H:%M')}"]])
    update_table(service, CURATOR_OUTPUT_LIST, 'D1:G1', [[f"Оценка куратора за итерации"]])
    print_log(f"Выгрузка успешно завершена")


if __name__ == '__main__':
    main()
