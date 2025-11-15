from config import API_KEY_TRELLO, API_TOKEN_TRELLO

def trello_search(arguments: dict) -> str:
    """
    Выполняет поиск по Trello через Search API и возвращает нормализованный результат в виде JSON-строки.

    Авторизация:
        - Ключ и токен загружаются напрямую из переменных окружения через load_dotenv() и os.getenv():
            - API_KEY_TRELLO   = os.getenv("API_KEY_TRELLO")
            - API_TOKEN_TRELLO = os.getenv("API_TOKEN_TRELLO")
        - При отсутствии ключа/токена запросы будут отклоняться (например, HTTP 401).

    Эндпоинт:
        - GET https://api.trello.com/1/search

    Входные аргументы (arguments: dict):
        - query (str, обязательно):
          Строка поиска (поддерживает слова, ID, хэши, спецсимволы).
        - modelTypes (str, опционально):
          CSV-строка типов сущностей для поиска. Допустимые значения: "boards", "cards", "members", "organizations".
          По умолчанию: "boards,cards,members,organizations".
        - board_fields (str, опционально):
          CSV-строка полей для объектов boards. По умолчанию: "id,name".
          Часто используемые поля: "id", "name", "desc", "url", "shortLink", "closed", "pinned", "starred", "idOrganization", "prefs", "dateLastActivity".
        - card_fields (str, опционально):
          CSV-строка полей для объектов cards. По умолчанию: "id,name,idBoard".
          Часто используемые поля: "id", "name", "desc", "url", "shortLink", "idBoard", "idList", "idMembers", "labels", "due", "start", "dueComplete", "closed", "dateLastActivity".
        - member_fields (str, опционально):
          CSV-строка полей для объектов members. По умолчанию: "id,username,fullName".
          Часто используемые поля: "id", "username", "fullName", "initials", "avatarUrl", "bio", "confirmed", "memberType", "status", "url".
        - organization_fields (str, опционально):
          CSV-строка полей для объектов organizations. По умолчанию: "id,name,displayName".
          Часто используемые поля: "id", "name", "displayName", "desc", "website", "logoHash", "url", "products", "prefs".

    Поведение:
        - Формирует запрос к Search API с указанными типами сущностей и наборами полей.
        - В случае HTTP/сетевой ошибки возвращает JSON со структурой:
          {"error": <str>, "status": <int|None>, "details" | "body": <obj|str>}
        - В случае успеха нормализует результат к виду:
          {
            "boards": [...],
            "cards": [...],
            "members": [...],
            "organizations": [...]
          }
          Отсутствующие сущности заполняются пустыми списками.

    Возвращает:
        - str: Отформатированная JSON-строка (indent=2) с нормализованным результатом или ошибкой.

    Примеры:
        trello_search({
            "query": "payment bot",
            "modelTypes": "boards,cards",
            "board_fields": "id,name,url",
            "card_fields": "id,name,idBoard,url,labels"
        })

        trello_search({
            "query": "user:me",
            "member_fields": "id,username,fullName,avatarUrl"
        })

        trello_search({
            "query": "org:my-company",
            "modelTypes": "organizations,boards",
            "organization_fields": "id,name,displayName,url",
            "board_fields": "id,name,closed,idOrganization"
        })
    """

    import json
    import requests
    # from dotenv import load_dotenv

    # load_dotenv()

    # API_KEY_TRELLO = os.getenv("API_KEY_TRELLO")
    # API_TOKEN_TRELLO = os.getenv("API_TOKEN_TRELLO")

    query = arguments.get("query")
    if not query:
        return json.dumps({"error": "Missing 'query'"}, ensure_ascii=False, indent=2)

    model_types = arguments.get("modelTypes", "boards,cards,members,organizations")

    def send(method, url, params=None):
        if params:
            params = {k: v for k, v in params.items() if v is not None}
        try:
            response = requests.request(method, url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            error_data = {
                "error": str(e),
                "status": getattr(e.response, "status_code", None)
            }
            if e.response is not None:
                try:
                    error_data["details"] = e.response.json()
                except:
                    error_data["body"] = e.response.text
            return error_data

    def normalize_search_result(result: dict) -> dict:
        """
        Возвращает словарь, где ключи — типы сущностей ("boards", "cards", "members", "organizations"),
        а значения — списки объектов (или пустые списки, если данных нет).
        """
        normalized = {}
        for entity in ["boards", "cards", "members", "organizations"]:
            normalized[entity] = result.get(entity, [])
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

    # Если ошибка — сразу возвращаем JSON
    if isinstance(raw_result, dict) and "error" in raw_result:
        return json.dumps(raw_result, ensure_ascii=False, indent=2)

    normalized = normalize_search_result(raw_result)
    return json.dumps(normalized, ensure_ascii=False, indent=2)
