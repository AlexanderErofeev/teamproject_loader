from settings import *
from utils import requests_post, requests_put, print_log, multiprocessing_map, requests_get
from teamproject_loader_temp_result import get_teams


def create_iterations(workspace_id):
    print_log(f"Создание итераций в команде {workspace_id}")
    for iteration in ITERATIONS:
        requests_post(f'{DOMAIN}/api/v2/workspaces/{workspace_id}/iterations', data=iteration)


def update_iterations(workspace_id):
    print_log(f"Обновление итераций в команде {workspace_id}")
    iterations = requests_get(f'{DOMAIN}/api/v2/workspaces/{workspace_id}/iterations').json()

    target_iteration = [iteration for iteration in iterations if iteration['title'] == 'Защита'][0]
    print_log(target_iteration)
    target_iteration_id = target_iteration['id']

    new_target_iteration = ITERATIONS[-1]
    new_target_iteration['id'] = target_iteration_id
    print_log(new_target_iteration)

    requests_put(f'{DOMAIN}/api/v2/iterations/{target_iteration_id}', data=new_target_iteration)


if __name__ == '__main__':
    workspaces = get_teams()

    workspaces = [workspace for workspace in workspaces if workspace['mainCurator'] and workspace['mainCurator']['fullname'] == 'Шадрин Денис Борисович']
    print_log(f"Получено команд Шадрина Дениса Борисовича из teamproject: {len(workspaces)}")

    # workspace_ids = [workspace['id'] for workspace in workspaces]
    # print_log(f"Создание итераций в {len(workspace_ids)} команд")
    # multiprocessing_map(create_iterations, workspace_ids)

    # print_log(f"Обновление итераций в {len(workspace_ids)} команд")
    # list(map(update_iterations, workspace_ids))
