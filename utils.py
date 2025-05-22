import multiprocessing.dummy
import time
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from settings import USERNAME, AUTH_PAGE_URL, PASSWORD, AUTH_TOKEN_URL, DOMAIN, STATIC_TOKEN, IS_STATIC_TOKEN, \
    IS_USES_THREADING, THREAD_COUNT


def print_log(log_string, is_error=False):
    text = f"[{datetime.now().strftime('%d-%m-%Y %H:%M:%S')}] {log_string}"
    print("\033[31m{}\033[0m".format(text)) if is_error else print(text)


def sum_mas(list_lists):
    ans_mas = []
    for el in list_lists:
        ans_mas += el
    return ans_mas


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


HEADERS = {
    'Authorization': STATIC_TOKEN if IS_STATIC_TOKEN else get_authorization()
}


def requests_get(url, params=None):
    if params is None:
        params = {}
    r = None
    while r is None:
        try:
            r = requests.get(url, headers=HEADERS, timeout=15, params=params)
        except requests.exceptions.Timeout:
            print_log(f"Ошибка timeout с URL: {url}", is_error=True)
        except requests.exceptions.ConnectionError:
            print_log(f"Ошибка с URL: {url}", is_error=True)
    return r


def requests_post(url, data):
    r = None
    for i in range(5):
        try:
            r = requests.post(url, headers=HEADERS, json=data, timeout=5)
            if r.status_code not in {200, 201, 204}:
                print_log(f"Ошибка {r.request.body} с URL: {url}", is_error=True)
            else:
                break
        except requests.exceptions.Timeout:
            print_log(f"Ошибка timeout с URL: {url}", is_error=True)
        except requests.exceptions.ConnectionError:
            print_log(f"Ошибка с URL: {url}", is_error=True)
        time.sleep(5)
    return r


def requests_put(url, data):
    r = None
    for i in range(5):
        try:
            r = requests.put(url, headers=HEADERS, json=data, timeout=5)
            if r.status_code not in {200, 201, 204}:
                print_log(f"Ошибка {r.status_code} с URL: {url}", is_error=True)
            else:
                break
        except requests.exceptions.Timeout:
            print_log(f"Ошибка timeout с URL: {url}", is_error=True)
        except requests.exceptions.ConnectionError:
            print_log(f"Ошибка с URL: {url}", is_error=True)
        time.sleep(5)
    return r


def multiprocessing_map(func, elements):
    if IS_USES_THREADING:
        with multiprocessing.dummy.Pool(THREAD_COUNT) as p:
            result = p.map(func, elements)
    else:
        result = [func(el) for el in elements]
    return result
