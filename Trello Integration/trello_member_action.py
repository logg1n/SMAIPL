from config import API_KEY_TRELLO, API_TOKEN_TRELLO


def trello_member_action(arguments: dict) -> str:
    """
    Выполняет GET-запросы к Trello API для получения информации об участниках и связанных с ними сущностях.

    Поддерживаемые действия (значение ключа "action"):
        - "get_info": Получить информацию об участнике по ID.
            URL: GET /members/{id}
            Доп. параметры:
                - fields (str): Список полей через запятую. Примеры: "fullName,username,url"

        - "get_me": Получить информацию о текущем пользователе (авторизованном по токену).
            URL: GET /members/me
            Параметры не требуются.

        - "get_boards": Получить список досок участника.
            URL: GET /members/{id}/boards

        - "get_cards": Получить карточки участника.
            URL: GET /members/{id}/cards
            Доп. параметры:
                - filter (str): Фильтр карточек. Допустимые значения: "visible", "open", "closed", "all", "none"

        - "get_actions": Получить действия участника.
            URL: GET /members/{id}/actions
            Доп. параметры:
                - filter (str): Типы действий. Примеры: "commentCard", "updateCard", "createCard", ...
                - limit (int): Максимальное количество результатов (до 1000)
                - since (str): Начальная дата в формате ISO 8601
                - before (str): Конечная дата в формате ISO 8601

        - "get_organizations": Получить список организаций, в которых состоит участник.
            URL: GET /members/{id}/organizations

        - "get_notifications": Получить уведомления участника.
            URL: GET /members/{id}/notifications

    Аргументы:
        arguments (dict): Словарь с параметрами запроса. Поддерживаются следующие ключи:
            - action (str): Обязательный. Один из поддерживаемых типов действий (см. выше).
            - idMember (str): ID участника Trello или "me". Не требуется для действия "get_me".
            - fields (str): Только для "get_info". Список полей через запятую.
            - filter (str): Для "get_cards", "get_actions", "get_notifications".
            - limit (int): Для "get_actions" и "get_notifications".
            - since (str): Для "get_actions". Начальная дата (ISO 8601).
            - before (str): Для "get_actions". Конечная дата (ISO 8601).

    Возвращает:
        str: JSON-строка с результатом запроса или описанием ошибки. Ответ может содержать:
            - данные, полученные от Trello API (список досок, карточек, действий и т.д.)
            - или словарь с ключами "error", "status", "body" в случае ошибки запроса

    Пример использования:
        trello_member_action.json({
            "action": "get_cards",
            "idMember": "me",
            "filter": "visible"
        })
    """


    import json
    import requests
    # from dotenv import load_dotenv

    # load_dotenv()

    # API_KEY_TRELLO = os.getenv("API_KEY_TRELLO")
    # API_TOKEN_TRELLO = os.getenv("API_TOKEN_TRELLO")

    action = arguments.get("action")
    if action not in [
        "get_info", "get_me", "get_boards", "get_cards",
        "get_actions", "get_organizations", "get_notifications"
    ]:
        return json.dumps({"error": f"Unknown or missing action '{action}'"}, ensure_ascii=False)

    id_member = arguments.get("idMember", "me")

    def send(method, url, params=None):
        if params:
            params = {k: v for k, v in params.items() if v is not None}
        try:
            response = requests.request(method, url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {
                "error": str(e),
                "status": getattr(e.response, "status_code", None),
                "body": getattr(e.response, "text", None)
            }

    routes = {
        "get_info": {
            "method": "GET",
            "url": f"https://api.trello.com/1/members/{id_member}",
            "params": {
                "fields": arguments.get("fields"),
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO
            }
        },
        "get_me": {
            "method": "GET",
            "url": "https://api.trello.com/1/members/me",
            "params": {
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO
            }
        },
        "get_boards": {
            "method": "GET",
            "url": f"https://api.trello.com/1/members/{id_member}/boards",
            "params": {
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO
            }
        },
        "get_cards": {
            "method": "GET",
            "url": f"https://api.trello.com/1/members/{id_member}/cards",
            "params": {
                "filter": arguments.get("filter"),
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO
            }
        },
        "get_actions": {
            "method": "GET",
            "url": f"https://api.trello.com/1/members/{id_member}/actions",
            "params": {
                "filter": arguments.get("filter"),
                "limit": arguments.get("limit"),
                "since": arguments.get("since"),
                "before": arguments.get("before"),
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO
            }
        },
        "get_organizations": {
            "method": "GET",
            "url": f"https://api.trello.com/1/members/{id_member}/organizations",
            "params": {
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO
            }
        },
        "get_notifications": {
            "method": "GET",
            "url": f"https://api.trello.com/1/members/{id_member}/notifications",
            "params": {
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO
            }
        }
    }

    route = routes[action]
    result = send(route["method"], route["url"], params=route["params"])
    return json.dumps(result, ensure_ascii=False, indent=2)
