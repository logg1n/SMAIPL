import json
import requests
from config import API_KEY_TRELLO, API_TOKEN_TRELLO
from trello_search import trello_search   # используем нашу функцию

BASE = "https://api.trello.com/1"

def send(method, url, params=None):
    if params:
        params = {k: v for k, v in params.items() if v is not None}
    try:
        r = requests.request(method, url, params=params)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e), "status": getattr(e.response, "status_code", None)}

def main():
    # 1. Найти доску "Test Board" через нашу функцию trello_search
    search_result = trello_search({
        "query": "Test Board",
        "modelTypes": "boards",
        "board_fields": "id,name"
    })
    boards = json.loads(search_result).get("boards", [])
    if not boards:
        print("Доска 'Test Board' не найдена")
        return
    board_id = boards[0]["id"]
    print("Board ID:", board_id)

    # 2. Получить списки на доске
    lists = send("GET", f"{BASE}/boards/{board_id}/lists", {
        "key": API_KEY_TRELLO,
        "token": API_TOKEN_TRELLO,
        "fields": "id,name"
    })
    list_id = lists[0]["id"]
    print("List ID:", list_id)

    # 3. Получить метки на доске
    labels = send("GET", f"{BASE}/boards/{board_id}/labels", {
        "key": API_KEY_TRELLO,
        "token": API_TOKEN_TRELLO,
        "fields": "id,name,color"
    })
    label_id = labels[0]["id"]
    print("Label ID:", label_id)

    # 4. Создать карточку
    card = send("POST", f"{BASE}/cards", {
        "key": API_KEY_TRELLO,
        "token": API_TOKEN_TRELLO,
        "idList": list_id,
        "name": "Test Card",
        "desc": "Created via API",
        "idLabels": label_id
    })
    card_id = card["id"]
    print("Card created:", card_id)

    # 5. Получить карточку
    card_info = send("GET", f"{BASE}/cards/{card_id}", {
        "key": API_KEY_TRELLO,
        "token": API_TOKEN_TRELLO,
        "fields": "id,name,desc,idList,idBoard"
    })
    print("Card info:", json.dumps(card_info, ensure_ascii=False, indent=2))

    # 6. Обновить карточку
    updated = send("PUT", f"{BASE}/cards/{card_id}", {
        "key": API_KEY_TRELLO,
        "token": API_TOKEN_TRELLO,
        "desc": "Updated description"
    })
    print("Card updated:", json.dumps(updated, ensure_ascii=False, indent=2))

    # 7. Удалить карточку
    deleted = send("DELETE", f"{BASE}/cards/{card_id}", {
        "key": API_KEY_TRELLO,
        "token": API_TOKEN_TRELLO
    })
    print("Card deleted:", deleted)

if __name__ == "__main__":
    main()
