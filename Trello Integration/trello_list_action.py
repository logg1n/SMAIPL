from config import API_KEY_TRELLO, API_TOKEN_TRELLO


def trello_list_action(arguments: dict) -> str:
    """
    Выполняет действия над списками Trello.

    Аргументы:
        arguments (dict):
            - action (str): Тип операции. Возможные значения:
                - "create" : создать список
                - "get"    : получить список по ID
                - "update" : обновить список по ID (переименовать, переместить, архивировать)

            --- create ---
            Обязательные ключи:
                - idBoard (str): идентификатор доски
                - nameList (str): имя создаваемого списка

            --- get ---
            Обязательные ключи:
                - idList (str): идентификатор списка

            --- update ---
            Обязательные ключи:
                - idList (str): идентификатор списка
            Необязательные ключи:
                - newNameList (str): новое имя списка
                - idBoard (str): ID доски, если нужно переместить
                - closed (bool): True — архивировать, False — разархивировать

    Возвращает:
        str: JSON-строка с результатом выполнения запроса к Trello API.
    """

    import json
    import requests


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
