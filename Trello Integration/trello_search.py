from config import API_KEY_TRELLO, API_TOKEN_TRELLO

def trello_search(arguments: dict) -> str:
    """
    Выполняет поиск в Trello через Search API.

    Аргументы:
        - query (str): строка поиска (обязательный параметр)
        - modelTypes (str, optional): сущности для поиска (например: "boards,cards").
          Если не указано, ищем во всех: boards,cards,members,organizations.
        - board_fields (str, optional): поля для досок (по умолчанию "id,name")
        - card_fields (str, optional): поля для карточек (по умолчанию "id,name,idBoard")
        - member_fields (str, optional): поля для участников (по умолчанию "id,username,fullName")
        - organization_fields (str, optional): поля для организаций (по умолчанию "id,name,displayName")

    Возвращает:
        str: JSON-строка с нормализованным результатом поиска.
    """

    import json
    import requests

    query = arguments.get("query")
    if not query:
        return json.dumps({"error": "Missing 'query'"}, ensure_ascii=False)

    model_types = arguments.get("modelTypes", "boards,cards,members,organizations")

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

    def normalize_search_result(result: dict) -> dict:
        """
        Приводит результат поиска Trello к структуре со списками.
        """
        normalized = {}
        for entity in ["boards", "cards", "members", "organizations"]:
            if entity in result and result[entity]:
                normalized[entity] = result[entity]  # просто список объектов
        return normalized

    url = "https://api.trello.com/1/search"
    params = {
        "key": API_KEY_TRELLO,
        "token": API_TOKEN_TRELLO,
        "query": query,
        "modelTypes": model_types,
        "board_fields": arguments.get("board_fields", "id,name"),
        "card_fields": arguments.get("card_fields", "id,name,idBoard"),
        "member_fields": arguments.get("member_fields", "id,username,fullName"),
        "organization_fields": arguments.get("organization_fields", "id,name,displayName")
    }

    raw_result = send("GET", url, params=params)

    # Нормализация: превращаем списки в словари с индексами
    normalized = normalize_search_result(raw_result)

    return json.dumps(normalized, ensure_ascii=False, indent=2)