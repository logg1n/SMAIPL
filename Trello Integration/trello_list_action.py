def trello_list_action(arguments: dict) -> str:
    """
    Выполняет действия над списками Trello: создание, получение и обновление.
    Функция работает как универсальный маршрутизатор: в зависимости от значения параметра "action"
    формируется запрос к соответствующему эндпоинту Trello API.

    Особенности:
            - Поддерживает одиночное обращение: к 1-му листу из 1-ой доски.
            - Если неизвестно id Board и/или id List, поиск и вытягивание нужного id производится с Api Trello Members
            c endpoint members/me
            -Если есть несколько одинаковых досок/листов по имени, можно указать позицию листа 1,2,5.
            Если будет неверная позиция будет взят 1-ый доска/лист из найденых.
            Если указано не будет позиция, по умолчанию берется 1-ая из найденых.

    Авторизация:
            - Для работы функции требуется ключ и токен Trello API.
            - Они загружаются из переменных окружения и доступны в коде как:
                    - API_KEY_TRELLO   = os.getenv("API_KEY_TRELLO")
                    - API_TOKEN_TRELLO = os.getenv("API_TOKEN_TRELLO")
            - Эти значения автоматически добавляются в каждый запрос.
            - При отсутствии ключа/токена запросы будут отклоняться .

    Поддерживаемые действия (action):
            - "create": Создать новый список.
                    Эндпоинт: POST /lists
                    Обязательные параметры:
                            - id_board (str): ID доски.
                            - new_name_list (str): Имя нового списка.

            - "get": Получить информацию о списке.
                    Эндпоинт: GET /lists/{id_list}
                    Обязательные параметры:
                            - id_list (str): ID списка.

            - "update": Обновить список.
                    Эндпоинт: PUT /lists/{id_list}
                    Обязательные параметры:
                            - id_list (str): ID списка.
                    Дополнительные параметры:
                            - new_name_list (str): Новое имя списка.
                            - id_board (str): ID доски, если нужно переместить список.
                            - closed (str): "true" — архивировать, "false" — разархивировать.

    Аргументы:
            arguments (dict): Словарь с параметрами запроса.
                    - action (str, обязательно): Тип действия ("create", "get", "update").
                    - id_board (str): ID доски (для "create", опционально для "update").
                    - name_board (str): Имя Доски если неизвестен id_board.
                    - id_list (str): ID списка (для "get" и "update").
                    - name_list (str): Имя списка (если неизвестен id_list).
                    - new_name_list (str): Новое имя списка (для "action", "update").
                    - closed (str): Архивировать или разархивировать список (для "update").

    Возвращает:
            str: JSON-строка с результатом запроса.
                    - При успешном запросе: данные, полученные от Trello API (созданный список,
                      информация о списке, результат обновления).
                    - При ошибке: объект с ключами:
                            - "error": описание ошибки,
                            - "status": HTTP-код,
                            - "body": текст ответа сервера.
    """

    import json
    import requests
    from typing import Optional, Dict, List

    import os
    from dotenv import load_dotenv

    load_dotenv()

    API_KEY_TRELLO = os.getenv("API_KEY_TRELLO")
    API_TOKEN_TRELLO = os.getenv("API_TOKEN_TRELLO")

    # --- Класс исключений ---
    class TrelloListError(Exception):
        pass

    # create
    # Создай лист "Имя листа" на доске "Имя Доски"
    # (not support) Создай лист "Имя листа 1, Имя листа 2" на доске "Имя Доски"

    # get, update(*)
    # * Покажи Лист "Имя листа" на доске "Имя доски"
    # (not support) Покажи Листы на доске "Имя доски"
    # (not support) Покажи Листы на доске "Имя листа 1, Имя листа 2" "Имя доски"
    # (not support) Покажи Лист "Имя листа"
    # (not support) Покажи Листы "Имя листа 1, Имя листа 2"

    class TrelloHelper:
        @staticmethod
        def get_boards(name_board: str) -> Dict:
            if not name_board:
                raise TrelloListError(
                    json.dumps({"error": "Missing 'name_board'"}, ensure_ascii=False)
                )

            url = "https://api.trello.com/1/members/me"
            params = {
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO,
                "boards": "all",
                "board_lists": "all",
            }
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            res = response.json()["boards"]

            board = next(
                (
                    {
                        "id": b["id"],
                        "name": b["name"],
                        "lists": [
                            {"id": l["id"], "name": l["name"], "pos": l["pos"]}
                            for l in b.get("lists", [])
                        ],
                    }
                    for b in res
                    if b["name"] == name_board
                ),
                None,
            )

            if not board:
                raise TrelloListError(
                    json.dumps(
                        {"error": f"Board '{name_board}' not found"}, ensure_ascii=False
                    )
                )

            return board

        @staticmethod
        def get_lists(name_list: str, id_board: str, lists: List) -> Dict:
            if not name_list:
                raise TrelloListError(
                    json.dumps({"error": "Missing 'name_list'"}, ensure_ascii=False)
                )

            if not id_board:
                raise TrelloListError(
                    json.dumps({"error": "Missing 'id_board'"}, ensure_ascii=False)
                )

            if not lists:
                raise TrelloListError(
                    json.dumps(
                        {"error": f"Lists not found in {id_board}"}, ensure_ascii=False
                    )
                )

            flist = next((l for l in lists if l["name"] == name_list), None)
            if not flist:
                raise TrelloListError(
                    json.dumps(
                        {"error": f"List '{name_list}' not found"}, ensure_ascii=False
                    )
                )

            # get_one_list = lambda l, c: (l[count_list - 1] if c is not None and 0 <= c - 1 < len(l) else l[0])

            # if count_list is not None:
            #     lists: List = get_one_list(lists, count_list)

            return flist
            # return lists[count_list if count_list < len(lists) else 0]

        # Функция отправки запроса
        @staticmethod
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
                    "body": response.text,
                }

    #           -----------------------------------------------------------------------------------------------            #

    action = arguments.get("action")
    if action is None or action not in {"create", "get", "update"}:
        raise TrelloListError(
            json.dumps(
                {"error": f"Unknown or missing action '{action}'"}, ensure_ascii=False
            )
        )

    base_url = "https://api.trello.com/1/lists"

    # --- Проверки на отсутствие обязательных аргументов ---
    if action == "create":
        if not arguments.get("id_board") and not arguments.get("name_board"):
            raise TrelloListError(
                json.dumps(
                    {"error": "Missing 'id_board' or 'name_board' for create"},
                    ensure_ascii=False,
                )
            )
        if not arguments.get("new_name_list"):
            raise TrelloListError(
                json.dumps(
                    {"error": "Missing 'new_name_list' for create"},
                    ensure_ascii=False,
                )
            )

    elif action in ["get", "update"]:
        if not arguments.get("id_list") and not arguments.get("name_list"):
            raise TrelloListError(
                json.dumps(
                    {"error": f"Missing 'id_list' or 'name_list' for {action}"},
                    ensure_ascii=False,
                )
            )

    id_board = arguments.get("id_board")
    name_board = arguments.get("name_board")
    id_list = arguments.get("id_list")
    name_list = arguments.get("name_list")
    board = None

    if not id_board and name_board:
        board = TrelloHelper.get_boards(name_board=arguments.get("name_board"))
        id_board = board["id"]

    if not id_board:
        raise TrelloListError(
            json.dumps({"error": f"не удалось вытянуть id Доски"}, ensure_ascii=False)
        )

    if action in ["get", "update"]:
        if not id_list and name_list:
            lists = board["lists"]
            id_list = TrelloHelper.get_lists(
                name_list=arguments.get("name_list"), id_board=id_board, lists=lists
            )["id"]

        if not id_list:
            raise TrelloListError(
                json.dumps(
                    {"error": f"не удалось вытянуть id Списка"}, ensure_ascii=False
                )
            )

    action_url = f"{base_url}/{id_list}"

    # Маршруты по действиям
    routes = {
        "create": {
            "method": "POST",
            "url": base_url,
            "params": {
                "name": arguments.get("new_name_list"),
                "idBoard": id_board,
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO,
            },
        },
        "get": {
            "method": "GET",
            "url": action_url,
            "params": {
                "cards": "all",
                "card_fields": "id,name",
                "fields": "id,name,idBoard",
                "actions": "all",
                "action_fields": "id,type,date,data",
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO,
            },
        },
        "update": {
            "method": "PUT",
            "url": action_url,
            "params": {
                "key": API_KEY_TRELLO,
                "token": API_TOKEN_TRELLO,
                "name": arguments.get("new_name_list"),
                "idBoard": id_board,
                "closed": arguments.get("closed"),
            },
        },
    }

    route = routes[action]

    result = TrelloHelper.send(route["method"], route["url"], params=route["params"])

    return json.dumps(result, ensure_ascii=False, indent=2)


# if __name__ == "__main__":
#     # Пример полного рабочего потока
#     import json

    # # 1. Создаем список
    # create_result = trello_list_action(
    #     {
    #         "action": "create",
    #         "name_board": "Test Board",
    #         "new_name_list": "Новые задачи 2",
    #     }
    # )
    #
    # create_data = json.loads(create_result)
    # print(f"Создан список с ID: {create_data.get('id')}")
    #
    # # 2. Получаем информацию о списке
    # get_result = trello_list_action(
    #     {
    #         "action": "get",
    #         "name_board": "Test Board",  # укажи доску
    #         "name_list": "Новые задачи 2",  # укажи список
    #     }
    # )
    #
    # get_data = json.loads(get_result)
    # print(f"В списке карточек: {len(get_data.get('cards', []))}")

    # # 3. Переименовываем список
    # update_result = trello_list_action(
    #     {
    #         "action": "update",
    #         "name_board": "Test Board",  # укажи доску
    #         "name_list": "Новые задачи 2",  # укажи список
    #         "new_name_list": "Активные задачи",
    #     }
    # )

    # print(f"Список переименован: {update_result}")

    # 4. Разархивируем список
    # unarchive_result = trello_list_action(
    #     {
    #         "action": "update",
    #         "name_board": "Test Board",
    #         "name_list": "Новые задачи",
    #         "closed": "false",  # или False, если хочешь булево
    #     }
    # )
    # print(f"Список разархивирован: {unarchive_result}")
