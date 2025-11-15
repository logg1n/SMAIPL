from config import API_KEY_TRELLO, API_TOKEN_TRELLO


def trello_card_action(arguments: dict) -> str:
    """
    Выполняет действия над карточками Trello: создание, получение, обновление и удаление.
    Функция работает как универсальный маршрутизатор: в зависимости от значения параметра "action"
    формируется запрос к соответствующему эндпоинту Trello API.

    Авторизация:
        - Для работы функции требуется ключ и токен Trello API.
        - Они загружаются напрямую из переменных окружения через load_dotenv() и os.getenv():
            - API_KEY_TRELLO   = os.getenv("API_KEY_TRELLO")
            - API_TOKEN_TRELLO = os.getenv("API_TOKEN_TRELLO")
        - При отсутствии ключа/токена запросы будут отклоняться (например, HTTP 401).

    Поддерживаемые действия (action):
        - "create": Создать новую карточку.
            Эндпоинт: POST /cards
            Обязательные параметры:
                - idList (str): ID списка, в который будет добавлена карточка.
            Дополнительные параметры:
                - name (str): Название карточки.
                - desc (str): Описание карточки.
                - pos (str|int): Позиция ("top", "bottom" или число).
                - due (str): Срок выполнения (ISO‑дата).
                - start (str): Дата начала (ISO‑дата).
                - idMembers (list[str]): Список ID участников.
                - idLabels (list[str]): Список ID меток.

        - "get": Получить информацию о карточке.
            Эндпоинт: GET /cards/{idCard}
            Обязательные параметры:
                - idCard (str): ID карточки.
            Дополнительные параметры:
                - fields (str): Список полей для возврата. По умолчанию: "id,name,desc,due,start,idList,idBoard".

        - "update": Обновить карточку.
            Эндпоинт: PUT /cards/{idCard}
            Обязательные параметры:
                - idCard (str): ID карточки.
            Дополнительные параметры (любые из ниже):
                - name (str): Новое название.
                - desc (str): Новое описание.
                - pos (str|int): Новая позиция ("top", "bottom" или число).
                - due (str): Новый срок (ISO‑дата).
                - start (str): Новая дата начала (ISO‑дата).
                - idMembers (list[str]): Список ID участников.
                - idLabels (list[str]): Список ID меток.

        - "delete": Удалить карточку.
            Эндпоинт: DELETE /cards/{idCard}
            Обязательные параметры:
                - idCard (str): ID карточки.

    Аргументы:
        arguments (dict): Словарь с параметрами запроса.
            - action (str, обязательно): Тип действия ("create", "get", "update", "delete").
            - idList (str): ID списка. Обязателен для "create".
            - idCard (str): ID карточки. Обязателен для "get", "update", "delete".
            - name (str): Название карточки.
            - desc (str): Описание карточки.
            - pos (str|int): Позиция ("top", "bottom" или число).
            - due (str): Срок выполнения (ISO‑дата).
            - start (str): Дата начала (ISO‑дата).
            - idMembers (list[str]): Список ID участников.
            - idLabels (list[str]): Список ID меток.

    Возвращает:
        str: JSON‑строка с результатом запроса.
            - При успешном запросе: данные, полученные от Trello API (созданная карточка,
              информация о карточке, результат обновления или удаления).
            - При ошибке: объект с ключами:
                - "error": описание ошибки,
                - "status": HTTP‑код,
                - "body": текст ответа сервера.

    Примеры:
        Создание карточки:
            trello_card_action({
                "action": "create",
                "idList": "62f9876543210abcd9876543",
                "name": "Implement payment API",
                "desc": "Реализовать интеграцию с платёжным сервисом",
                "pos": "top",
                "due": "2025-11-20T12:00:00.000Z",
                "start": "2025-11-15T09:00:00.000Z",
                "idMembers": ["64f1234567890abcd1234567"],
                "idLabels": ["5e1234567890abcdef123456"]
            })

        Получение карточки:
            trello_card_action({
                "action": "get",
                "idCard": "64f1234567890abcd1234567"
            })

        Обновление карточки:
            trello_card_action({
                "action": "update",
                "idCard": "64f1234567890abcd1234567",
                "name": "Fix payment API bug",
                "desc": "Исправить баг в обработке транзакций",
                "due": "2025-11-25T12:00:00.000Z"
            })

        Удаление карточки:
            trello_card_action({
                "action": "delete",
                "idCard": "64f1234567890abcd1234567"
            })
    """


    import json
    import requests
    # from dotenv import load_dotenv

    # load_dotenv()

    # API_KEY_TRELLO = os.getenv("API_KEY_TRELLO")
    # API_TOKEN_TRELLO = os.getenv("API_TOKEN_TRELLO")

    action = arguments.get("action")
    if action not in ["create", "get", "update", "delete"]:
        return json.dumps({"error": f"Unknown or missing action '{action}'"}, ensure_ascii=False)

    base_url = "https://api.trello.com/1/cards"

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
                "idList": arguments.get("idList"),
                "name": arguments.get("name"),
                "desc": arguments.get("desc"),
                "pos": arguments.get("pos"),
                "due": arguments.get("due"),
                "start": arguments.get("start"),
                "idMembers": arguments.get("idMembers"),
                "idLabels": arguments.get("idLabels"),
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO
            }
        },
        "get": {
            "method": "GET",
            "url": f"{base_url}/{arguments.get('idCard')}",
            "params": {
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO,
                "fields": "id,name,desc,due,start,idList,idBoard"
            }
        },
        "update": {
            "method": "PUT",
            "url": f"{base_url}/{arguments.get('idCard')}",
            "params": {
                "name": arguments.get("name"),
                "desc": arguments.get("desc"),
                "pos": arguments.get("pos"),
                "due": arguments.get("due"),
                "start": arguments.get("start"),
                "idMembers": arguments.get("idMembers"),
                "idLabels": arguments.get("idLabels"),
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO
            }
        },
        "delete": {
            "method": "DELETE",
            "url": f"{base_url}/{arguments.get('idCard')}",
            "params": {
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO
            }
        }
    }

    route = routes[action]
    result = send(route["method"], route["url"], params=route["params"])
    return json.dumps(result, ensure_ascii=False, indent=2)
