import multiprocessing.dummy
import math
from settings import *
from utils import requests_get, sum_mas, requests_post, print_log


def create_iterations(workspace_id):
    print_log(f"Создание итераций в команде {workspace_id}")
    for iteration in ITERATIONS:
        requests_post(f'{DOMAIN}/api/v2/workspaces/{workspace_id}/iterations', data=iteration)


if __name__ == '__main__':
    print_log(f"Поиск команд в teamproject")
    workspaces = []
    for search_prefix in SEARCH_PREFIXS:
        teamproject_counts = requests_get(f'{DOMAIN}/api/v2/workspaces?status=active&year={YEAR}&semester={SEMESTR}&search={search_prefix}').json()['total']
        print_log(f"Всего команд {search_prefix} в teamproject: {teamproject_counts}")

        for i in range(1, math.ceil(teamproject_counts / PER_PAGE_RESULTS) + 1):
            workspaces.append(requests_get(f'{DOMAIN}/api/v2/workspaces?status=active&year={YEAR}&semester={SEMESTR}&size={PER_PAGE_RESULTS}&page={i}&search={search_prefix}').json()['items'])

    workspaces = sum_mas(workspaces)
    workspace_ids = [workspace['id'] for workspace in workspaces]

    print_log(f"Создание итераций в {len(workspace_ids)} команд")

    if IS_USES_THREADING:
        with multiprocessing.dummy.Pool(THREAD_COUNT) as p:
            p.map(create_iterations, workspace_ids)
    else:
        list(map(create_iterations, workspace_ids))
