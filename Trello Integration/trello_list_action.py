from config import API_KEY_TRELLO, API_TOKEN_TRELLO


def trello_list_action(arguments: dict) -> str:
    """
    Выполняет действия над списками Trello: создание, получение и обновление.
    Функция работает как универсальный маршрутизатор: в зависимости от значения параметра "action"
    формируется запрос к соответствующему эндпоинту Trello API.

    Авторизация:
        - Для работы функции требуется ключ и токен Trello API.
        - Они загружаются из переменных окружения и доступны в коде как:
            - API_KEY_TRELLO   = os.getenv("API_KEY_TRELLO")
            - API_TOKEN_TRELLO = os.getenv("API_TOKEN_TRELLO")
        - Эти значения автоматически добавляются в каждый запрос.
        - При отсутствии ключа/токена запросы будут отклоняться (например, HTTP 401).

    Поддерживаемые действия (action):
        - "create": Создать новый список.
            Эндпоинт: POST /lists
            Обязательные параметры:
                - idBoard (str): ID доски.
                - nameList (str): Имя нового списка.

        - "get": Получить информацию о списке.
            Эндпоинт: GET /lists/{idList}
            Обязательные параметры:
                - idList (str): ID списка.
            Дополнительные параметры:
                - fields (str): Список полей (по умолчанию "id,name,closed").

        - "update": Обновить список.
            Эндпоинт: PUT /lists/{idList}
            Обязательные параметры:
                - idList (str): ID списка.
            Дополнительные параметры:
                - newNameList (str): Новое имя списка.
                - idBoard (str): ID доски, если нужно переместить список.
                - closed (bool): True — архивировать, False — разархивировать.

    Аргументы:
        arguments (dict): Словарь с параметрами запроса.
            - action (str, обязательно): Тип действия ("create", "get", "update").
            - idBoard (str): ID доски (для "create", опционально для "update").
            - nameList (str): Имя нового списка (для "create").
            - idList (str): ID списка (для "get" и "update").
            - newNameList (str): Новое имя списка (для "update").
            - closed (bool): Архивировать или разархивировать список (для "update").

    Возвращает:
        str: JSON-строка с результатом запроса.
            - При успешном запросе: данные, полученные от Trello API (созданный список,
              информация о списке, результат обновления).
            - При ошибке: объект с ключами:
                - "error": описание ошибки,
                - "status": HTTP-код,
                - "body": текст ответа сервера.

    Примеры:
        Создание списка:
            trello_list_action({
                "action": "create",
                "idBoard": "62f9876543210abcd9876543",
                "nameList": "To Do"
            })

        Получение списка:
            trello_list_action({
                "action": "get",
                "idList": "64f1234567890abcd1234567"
            })

        Обновление списка (переименование):
            trello_list_action({
                "action": "update",
                "idList": "64f1234567890abcd1234567",
                "newNameList": "In Progress"
            })

        Архивирование списка:
            trello_list_action({
                "action": "update",
                "idList": "64f1234567890abcd1234567",
                "closed": True
            })
    """


    import json
    import requests
    # from dotenv import load_dotenv

    # load_dotenv()

    # API_KEY_TRELLO = os.getenv("API_KEY_TRELLO")
    # API_TOKEN_TRELLO = os.getenv("API_TOKEN_TRELLO")


    action: str = arguments.get("action")
    if action not in ["create", "get", "update"]:
        return json.dumps({"error": f"Unknown or missing action '{action}'"}, ensure_ascii=False)

    base_url = "https://api.trello.com/1/lists"

    # Проверка обязательных параметров
    if action == "create":
        if not arguments.get("idBoard") or not arguments.get("nameList"):
            return json.dumps({"error": "Missing 'idBoard' or 'nameList' for create"}, ensure_ascii=False)

    if action == "get":
        if not arguments.get("idList"):
            return json.dumps({"error": "Missing 'idList' for get"}, ensure_ascii=False)

    if action == "update":
        if not arguments.get("idList"):
            return json.dumps({"error": "Missing 'idList' for update"}, ensure_ascii=False)

    # Универсальная функция отправки запроса
    def send(method: str, url: str, params=None, data=None) -> dict:
        if params:
            params = {k: v for k, v in params.items() if v is not None}
        if data:
            data = {k: v for k, v in data.items() if v is not None}

        response = requests.request(method, url, params=params, data=data)
        try:
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {
                "error": str(e),
                "status": response.status_code,
                "body": response.text
            }

    # Маршруты по действиям
    routes = {
        "create": {
            "method": "POST",
            "url": base_url,
            "params": {
                "name": arguments.get("nameList"),
                "idBoard": arguments.get("idBoard"),
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO
            }
        },
        "get": {
            "method": "GET",
            "url": f"{base_url}/{arguments.get('idList')}",
            "params": {
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO,
                "fields": "id,name,closed"
            }
        },
        "update": {
            "method": "PUT",
            "url": f"{base_url}/{arguments.get('idList')}",
            "params": {
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO,
                "name": arguments.get("newNameList"),
                "idBoard": arguments.get("idBoard"),
                "closed": arguments.get("closed"),
            }
        }
    }

    route = routes[action]
    result = send(route["method"], route["url"], params=route["params"])
    return json.dumps(result, ensure_ascii=False, indent=2)
