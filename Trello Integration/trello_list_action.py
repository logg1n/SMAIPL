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
                - newNameList (str): Имя нового списка.

        - "get": Получить информацию о списке.
            Эндпоинт: GET /lists/{idList}
            Обязательные параметры:
                - idList (str): ID списка.
            Дополнительные параметры:
                - fields (str): Список полей (по умолчанию "id,name,closed").

        - "update": Обновить список.
            Эндпоинт: PUT /lists/{idList}
            Обязательные параметры:
                - idList (str): ID списка.
            Дополнительные параметры:
                - newNameList (str): Новое имя списка.
                - id_board (str): ID доски, если нужно переместить список.
                - closed (str): "true" — архивировать, "false" — разархивировать.

    Аргументы:
        arguments (dict): Словарь с параметрами запроса.
            - action (str, обязательно): Тип действия ("create", "get", "update").
            - id_board (str): ID доски (для "create", опционально для "update").
            - nameBoard (str): Имя Доски если неизвестен id_board.
            - idList (str): ID списка (для "get" и "update").
            - nameList (str): Имя списка (если неизвестен idList).
            - newNameList (str): Новое имя списка (для "action", "update").
            - closed (str): Архивировать или разархивировать список (для "update").

    Возвращает:
        str: JSON-строка с результатом запроса.
            - При успешном запросе: данные, полученные от Trello API (созданный список,
              информация о списке, результат обновления).
            - При ошибке: объект с ключами:
                - "error": описание ошибки,
                - "status": HTTP-код,
                - "body": текст ответа сервера.

    Примеры:
        Создание списка:
            trello_list_action({
                "action": "create",
                "nameboard": "Test Board",
                "nameList": "To Do"
            })

        Получение списка:
            trello_list_action({
                "action": "get",
                "nameList": "List",
                "nameBoard": "Test Board"
                "count": 4
            })

        Обновление списка (переименование):
            trello_list_action({
                "action": "update",
                "nameList": "List",
                "nameBoard": "Test Board"
                "newNameList": "In Progress"
                "count": 2
            })

        Архивирование списка:
            trello_list_action({
                "action": "update",
                "nameList": "List",
                "nameBoard": "Test Board"
                "closed": True
                "count": 5
            })
    """

	import json
	import requests
	from typing import Optional, Dict, List
	# from dotenv import load_dotenv

	# load_dotenv()

	# API_KEY_TRELLO = os.getenv("API_KEY_TRELLO")
	# API_TOKEN_TRELLO = os.getenv("API_TOKEN_TRELLO")

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
		def get_boards(nameBoard: str, count_board: int = 0) -> Dict:
			if not nameBoard:
				raise TrelloListError(json.dumps({"error": "Missing 'nameBoard'"}, ensure_ascii=False))

			url = "https://api.trello.com/1/members/me"
			params = {
				"key": API_KEY_TRELLO,
				"token": API_TOKEN_TRELLO,
				"boards": "all",
				"board_lists": "all"
			}
			response = requests.get(url, params=params, timeout=10)
			response.raise_for_status()
			res = response.json()["boards"]

			boards = [
				{
					"id": b["id"],
					"name": b["name"],
					"lists": [{"id": l["id"], "name": l["name"], "pos": l["pos"], } for l in b.get("lists", [])]
				}
				for b in res if b["name"] == nameBoard
			]

			if not boards:
				raise TrelloListError(json.dumps({"error": f"Board '{nameBoard}' not found"}, ensure_ascii=False))

			return boards[count_board if count_board < len(boards) else 0]

		@staticmethod
		def get_lists(nameList: str, id_board: str, lists: List, count_list: int = 0) -> Dict:
			if not nameList:
				raise TrelloListError(json.dumps({"error": "Missing 'nameList'"}, ensure_ascii=False))

			if not id_board:
				raise TrelloListError(json.dumps({"error": "Missing 'id_board'"}, ensure_ascii=False))

			if not lists:
				raise TrelloListError(
					json.dumps({"error": f"Lists not found in {id_board}"}, ensure_ascii=False))

			# фильтрация по имени списка
			lists: List = [l for l in lists if l["name"] == nameList]
			# фильтрация по порядковому номеру
			if not lists:
				raise TrelloListError(json.dumps({"error": f"Lists '{nameList}' not found"}, ensure_ascii=False))

			# get_one_list = lambda l, c: (l[count_list - 1] if c is not None and 0 <= c - 1 < len(l) else l[0])

			# if count_list is not None:
			#     lists: List = get_one_list(lists, count_list)

			return lists[count_list if count_list < len(lists) else 0]

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
					"body": response.text
				}

	#           -----------------------------------------------------------------------------------------------            #

	action: Optional[str] = a if (a := arguments.get("action")) in ["create", "get", "update"] else None
	if action is None:
		raise TrelloListError(json.dumps({"error": f"Unknown or missing action '{action}'"}, ensure_ascii=False))

	base_url = "https://api.trello.com/1/lists"

	# --- Проверки на отсутствие обязательных аргументов ---
	if not arguments.get("id_board") and not arguments.get("nameBoard"):
		raise TrelloListError(json.dumps(
			{"error": "Missing 'id_board' or 'nameBoard' for create"},
			ensure_ascii=False
		))

	if action in ["get", "update"] and not arguments.get("idList") and not arguments.get("nameList"):
		raise TrelloListError(json.dumps(
			{"error": f"Missing 'idList' or 'nameList' for {action}"},
			ensure_ascii=False
		))

	id_board = arguments.get("id_board")
	name_board = arguments.get("nameBoard")
	id_list = arguments.get("idList")
	name_list = arguments.get("nameList")
	board = None

	if not id_board and name_board:
		board = TrelloHelper.get_boards(nameBoard=arguments.get("nameBoard"))
		id_board = board["id"]

	if not id_board:
		raise TrelloListError(json.dumps(
			{"error": f"не удалось вытянуть id Доски"},
			ensure_ascii=False
		))

	if not id_list and name_list:
		lists = board["lists"]
		id_list = TrelloHelper.get_lists(nameList=arguments.get("nameList"), id_board=id_board, lists=lists)["id"]

	if not id_list:
		raise TrelloListError(json.dumps(
			{"error": f"не удалось вытянуть id Списка"},
			ensure_ascii=False
		))

	action_url = f"{base_url}/{id_list}"

	# Маршруты по действиям
	routes = {
		"create": {
			"method": "POST",
			"url": base_url,
			"params": {
				"name": arguments.get("newNameList"),
				"idBoard": id_board,
				"key": API_KEY_TRELLO,
				"token": API_TOKEN_TRELLO
			}
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
			}
		},
		"update": {
			"method": "PUT",
			"url": action_url,
			"params": {
				"key": API_KEY_TRELLO,
				"token": API_TOKEN_TRELLO,
				"name": arguments.get("newNameList"),
				"idBoard": id_board,
				"closed": arguments.get("closed"),
			}
		}
	}

	route = routes[action]

	result = TrelloHelper.send(route["method"], route["url"], params=route["params"])

	return json.dumps(result, ensure_ascii=False, indent=2)
