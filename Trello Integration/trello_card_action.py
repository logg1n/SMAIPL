import json
import requests
from config import API_KEY_TRELLO, API_TOKEN_TRELLO

def trello_card_action(arguments: dict) -> str:
    """
    Выполняет действия над карточками Trello: create, get, update, delete.

    Аргументы:
        - action (str): "create", "get", "update", "delete"
        - idList (str): ID списка (обязателен для create)
        - idCard (str): ID карточки (обязателен для get/update/delete)
        - name (str): название карточки
        - desc (str): описание карточки
        - pos (str|int): позиция ("top", "bottom" или число)
        - due (str): срок выполнения (ISO‑дата)
        - start (str): дата начала
        - idMembers (list[str]): список ID участников
        - idLabels (list[str]): список ID меток

    Возвращает:
        str: JSON‑строка с результатом запроса.
    """

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
