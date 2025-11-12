import json
import requests
from config import API_KEY_TRELLO, API_TOKEN_TRELLO

def trello_label_action(arguments: dict) -> str:
    """
    Выполняет действия над метками Trello: создание, получение, обновление, удаление.

    Аргументы:
        - action (str): "create", "get", "update", "delete"
        - idBoard (str): ID доски (обязателен для create)
        - idLabel (str): ID метки (обязателен для get/update/delete)
        - name (str): имя метки (для create/update)
        - color (str): цвет метки (для create/update). Допустимые значения:
            "green", "yellow", "orange", "red", "purple", "blue", "sky",
            "lime", "pink", "black", "none"

    Примечание:
        Для авторизации используются переменные окружения API_KEY_TRELLO и API_TOKEN_TRELLO.
        Они импортируются из config.py и автоматически добавляются во все запросы.

    Возвращает:
        str: JSON-строка с результатом запроса к Trello API.
    """

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
