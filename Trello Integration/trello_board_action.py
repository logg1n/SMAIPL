from config import API_KEY_TRELLO, API_TOKEN_TRELLO

def trello_board_action(arguments: dict) -> str:
    """
    Выполняет действия над досками Trello: создание, получение, обновление и удаление.
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
        - "create": Создать новую доску.
            Эндпоинт: POST /boards
            Обязательные параметры:
                - name (str): Имя новой доски.
            Дополнительные параметры:
                - desc (str): Описание.
                - prefs_permissionLevel (str): Уровень доступа ("private", "public", "org").
                - defaultLists (str): Создавать стандартные списки ("true"/"false").
                - defaultLabels (str): Создавать стандартные метки ("true"/"false").

        - "get": Получить информацию о доске и вложенных ресурсах.
            Эндпоинт: GET /boards/{idBoard}
            Обязательные параметры:
                - idBoard (str): ID доски.
            Дополнительные параметры:
                - fields (str): Список полей доски (по умолчанию "name,desc,prefs,url,shortLink").
                - cards (str): "all", "open", "closed", "visible", "none".
                - lists (str): "all", "open", "closed", "none".
                - labels (str): "all", "none".
                - members (str): "all", "owners", "admins", "normal", "none".
                - organization (bool|str): Включить организацию (True/False).
                - actions (str): "all" или CSV-строка типов (например: "commentCard,updateCard:idList").
                - powerUps (str): "all", "enabled", "none".

        - "update": Обновить свойства доски.
            Эндпоинт: PUT /boards/{idBoard}
            Обязательные параметры:
                - idBoard (str): ID доски.
            Дополнительные параметры:
                - new_name (str): Новое имя.
                - desc (str): Новое описание.
                - prefs_permissionLevel (str): Новый уровень доступа ("private", "public", "org").
            Примечание:
                - хотя бы один из параметров ("new_name", "desc", "prefs_permissionLevel") должен быть указан.

        - "delete": Удалить доску.
            Эндпоинт: DELETE /boards/{idBoard}
            Обязательные параметры:
                - idBoard (str): ID доски.

    Аргументы:
        arguments (dict): Словарь с параметрами запроса.
            - action (str, обязательно): Тип действия ("create", "get", "update", "delete").
            - idBoard (str): ID доски (для "get", "update", "delete").
            - name (str): Имя новой доски (для "create").
            - new_name (str): Новое имя доски (для "update").
            - desc (str): Описание (для "create"/"update").
            - prefs_permissionLevel (str): Уровень доступа ("private", "public", "org").
            - defaultLists (str): Создавать стандартные списки ("true"/"false").
            - defaultLabels (str): Создавать стандартные метки ("true"/"false").
            - fields, cards, lists, labels, members, organization, actions, powerUps: параметры для включения вложенных ресурсов (для "get").

    Возвращает:
        str: JSON-строка с результатом запроса.
            - При успешном запросе: данные, полученные от Trello API (созданная доска,
              информация о доске, результат обновления или удаления).
            - При ошибке: объект с ключами:
                - "error": описание ошибки,
                - "status": HTTP-код,
                - "body": текст ответа сервера.
    """


    import json
    import requests
    # from dotenv import load_dotenv

    # load_dotenv()

    # API_KEY_TRELLO = os.getenv("API_KEY_TRELLO")
    # API_TOKEN_TRELLO = os.getenv("API_TOKEN_TRELLO")


    action: str = arguments.get("action")
    if action not in ["create", "get", "update", "delete"]:
        return json.dumps({"error": f"Unknown or missing action '{action}'"}, ensure_ascii=False)

    base_url = "https://api.trello.com/1/boards"
    board_id = arguments.get("idBoard")

    # Проверка обязательных аргументов
    if action == "create":
        if not arguments.get("name"):
            return json.dumps({"error": "Missing required parameter 'name' for create"}, ensure_ascii=False)

    if action in ["get", "update", "delete"]:
        if not board_id:
            return json.dumps({"error": f"Missing required parameter 'idBoard' for {action}"}, ensure_ascii=False)

    if action == "update":
        if not arguments.get("new_name") and not arguments.get("desc") and not arguments.get("prefs_permissionLevel"):
            return json.dumps({"error": "Nothing to update: provide 'new_name', 'desc' or 'prefs_permissionLevel'"}, ensure_ascii=False)

    def send(method, url, params=None, data=None):
        if params:
            params = {k: v for k, v in params.items() if v is not None}
        if data:
            data = {k: v for k, v in data.items() if v is not None}
        try:
            response = requests.request(method, url, params=params, data=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {
                "error": str(e),
                "status": getattr(e.response, "status_code", None),
                "body": getattr(e.response, "text", None)
            }

    routes = {
        "create": {
            "method": "POST",
            "url": base_url,
            "params": {
                "name": arguments.get("name"),
                "desc": arguments.get("desc", ""),
                "prefs_permissionLevel": arguments.get("prefs_permissionLevel", "private"),
                "defaultLists": arguments.get("defaultLists", "false"),
                "defaultLabels": arguments.get("defaultLabels", "false"),
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO
            }
        },
        "get": {
            "method": "GET",
            "url": f"{base_url}/{board_id}",
            "params": {
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO,
                "fields": "name,desc,prefs,url,shortLink",
                "cards": arguments.get("cards", "all"),
                "lists": arguments.get("lists", "all"),
                "labels": arguments.get("labels", "all"),
                "members": arguments.get("members", "all"),
                "organization": arguments.get("organization", "false"),
                "actions": arguments.get("actions"),
                "powerUps": arguments.get("powerUps", "none")
            }
        },
        "update": {
            "method": "PUT",
            "url": f"{base_url}/{board_id}",
            "params": {
                "name": arguments.get("new_name"),
                "desc": arguments.get("desc"),
                "prefs_permissionLevel": arguments.get("prefs_permissionLevel"),
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO
            }
        },
        "delete": {
            "method": "DELETE",
            "url": f"{base_url}/{board_id}",
            "params": {
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO
            }
        }
    }

    route = routes[action]
    result = send(route["method"], route["url"], params=route["params"])
    return json.dumps(result, ensure_ascii=False, indent=2)
