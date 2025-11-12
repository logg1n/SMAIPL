import json
from typing import Any

import requests
from config import API_KEY_TRELLO, API_TOKEN_TRELLO

def trello_board_action(arguments: dict) -> str:
    """
    Выполняет действия над досками Trello: создание, получение, обновление, удаление.

    Аргументы:
        - action (str): "create", "get", "update", "delete"
        - idBoard (str): ID доски (для get/update/delete)
        - name (str): имя новой доски (для create)
        - new_name (str): новое имя доски (для update)
        - desc (str): описание доски (для create/update)
        - prefs_permissionLevel (str): уровень доступа ("private", "public", "org")

        --- GET ---
        Допустимые значения для вложенных ресурсов:
            - cards: "all", "open", "closed", "visible", "none" (по умолчанию "all")
            - lists: "all", "open", "closed", "none" (по умолчанию "all")
            - labels: "all", "none" (по умолчанию "all")
            - members: "all", "owners", "admins", "normal", "none" (по умолчанию "all")
            - organization: boolean (True/False, по умолчанию False)
            - actions: "all" или список типов (например: "commentCard,updateCard:idList"). По умолчанию не включается.
            - powerUps: "all", "enabled", "none" (по умолчанию "none")

        Примечание:
        Для авторизации в Trello API используются переменные окружения:
        - API_KEY_TRELLO
        - API_TOKEN_TRELLO
        Они импортируются из config.py и автоматически добавляются во все запросы.
        Пользователю не нужно передавать их в arguments.

    Возвращает:
        str: JSON-строка с результатом запроса к Trello API.
    """

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
