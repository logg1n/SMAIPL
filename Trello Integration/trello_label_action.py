from config import API_KEY_TRELLO, API_TOKEN_TRELLO

def trello_label_action(arguments: dict) -> str:
    """
    Выполняет действия над метками Trello: создание, получение, обновление и удаление.
    Функция работает как универсальный маршрутизатор: в зависимости от значения параметра "action"
    формируется запрос к соответствующему эндпоинту Trello API.

    Авторизация:
        - Для работы функции требуется ключ и токен Trello API.
        - Они загружаются напрямую из переменных окружения через load_dotenv() и os.getenv():
            - API_KEY_TRELLO   = os.getenv("API_KEY_TRELLO")
            - API_TOKEN_TRELLO = os.getenv("API_TOKEN_TRELLO")
        - При отсутствии ключа/токена запросы будут отклоняться (например, HTTP 401).

    Поддерживаемые действия (action):
        - "create": Создать новую метку.
            Эндпоинт: POST /labels
            Обязательные параметры:
                - idBoard (str): ID доски, к которой будет привязана метка.
                - name (str): Имя метки.
            Дополнительный параметр:
                - color (str): Цвет метки. Допустимые значения:
                  "green", "yellow", "orange", "red", "purple", "blue", "sky",
                  "lime", "pink", "black", "none".
                  По умолчанию: "none".

        - "get": Получить информацию о метке.
            Эндпоинт: GET /labels/{idLabel}
            Обязательные параметры:
                - idLabel (str): ID метки.
            Дополнительные параметры:
                - fields (str): Список полей для возврата. По умолчанию: "id,name,color".

        - "update": Обновить метку.
            Эндпоинт: PUT /labels/{idLabel}
            Обязательные параметры:
                - idLabel (str): ID метки.
            Дополнительные параметры:
                - name (str): Новое имя метки.
                - color (str): Новый цвет метки.
            Примечание:
                - хотя бы один из параметров ("name" или "color") должен быть указан,
                  иначе вернётся ошибка "Nothing to update".

        - "delete": Удалить метку.
            Эндпоинт: DELETE /labels/{idLabel}
            Обязательные параметры:
                - idLabel (str): ID метки.

    Аргументы:
        arguments (dict): Словарь с параметрами запроса.
            - action (str, обязательно): Тип действия ("create", "get", "update", "delete").
            - idBoard (str): ID доски. Обязателен для "create".
            - idLabel (str): ID метки. Обязателен для "get", "update", "delete".
            - name (str): Имя метки. Используется для "create" и "update".
            - color (str): Цвет метки. Используется для "create" и "update".
              Допустимые значения: "green", "yellow", "orange", "red", "purple",
              "blue", "sky", "lime", "pink", "black", "none".

    Возвращает:
        str: JSON-строка с результатом запроса.
            - При успешном запросе: данные, полученные от Trello API (созданная метка,
              информация о метке, результат обновления или удаления).
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

    # Проверка обязательных параметров
    if action == "create":
        if not arguments.get("idBoard") or not arguments.get("name"):
            return json.dumps({"error": "Missing 'idBoard' or 'name' for create"}, ensure_ascii=False)

    if action in ["get", "update", "delete"]:
        if not arguments.get("idLabel"):
            return json.dumps({"error": f"Missing 'idLabel' for {action}"}, ensure_ascii=False)

    if action == "update":
        if not arguments.get("name") and not arguments.get("color"):
            return json.dumps({"error": "Nothing to update: provide 'name' or 'color'"}, ensure_ascii=False)

    base_url = "https://api.trello.com/1/labels"

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
                "idBoard": arguments.get("idBoard"),
                "name": arguments.get("name"),
                "color": arguments.get("color", "none"),
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO
            }
        },
        "get": {
            "method": "GET",
            "url": f"{base_url}/{arguments.get('idLabel')}",
            "params": {
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO,
                "fields": "id,name,color"
            }
        },
        "update": {
            "method": "PUT",
            "url": f"{base_url}/{arguments.get('idLabel')}",
            "params": {
                "name": arguments.get("name"),
                "color": arguments.get("color"),
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO
            }
        },
        "delete": {
            "method": "DELETE",
            "url": f"{base_url}/{arguments.get('idLabel')}",
            "params": {
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO
            }
        }
    }

    route = routes[action]
    result = send(route["method"], route["url"], params=route["params"])
    return json.dumps(result, ensure_ascii=False, indent=2)
