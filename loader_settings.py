# Глобальные настройки выгрузки
DOMAIN = 'https://teamproject.urfu.ru'
IS_USES_MULTIPROCESSING = True
PROCESSOR_COUNT = 16

# Данные пользователя
USERNAME = ''
PASSWORD = ''
IS_STATIC_TOKEN = True
STATIC_TOKEN = 'Bearer ...'
# Авторизация
AUTH_PAGE_URL = 'https://keys.urfu.ru/auth/realms/urfu-lk/protocol/openid-connect/auth?client_id=teampro&redirect_uri=https%3A%2F%2Fteamproject.urfu.ru%2F%23%2F&response_mode=fragment&response_type=code&scope=openid'
AUTH_TOKEN_URL = 'https://keys.urfu.ru/auth/realms/urfu-lk/protocol/openid-connect/token'

# Настройки входных данных
TEAMS_FILE = 'teams.csv'

# Настройки выходных данных
TEAMS_ID_FILE = 'teams_id.csv'
RESULT_FILE = 'result.csv'

# Настройки поиска teamproject
STATUS = 'active'
YEAR = 2022
SEMESTR = 2
SEARCH_PREFIX = '1С23S'
PER_PAGE_RESULTS = 100  # Максимум 100

# Настройки Гугл таблиц
CREDENTIALS_FILE = 'credentials.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
spreadsheet_id = '1WbgmS2K_UtbrQBDsng_V-2ufw7JHVXanJ6rdZ68dPSI'
LIST_NAME = 'Выгрузка teamproject (копия)'
sheetId = '135833361'
