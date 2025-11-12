import json
from trello_board_action import trello_board_action

# 1. CREATE — создаём тестовую доску
# print("=== CREATE ===")
# create_result = trello_board_action({
#     "action": "create",
#     "name": "Test Board",
#     "desc": "Board created for testing",
#     "prefs_permissionLevel": "private",
#     "defaultLists": "false",
#     "defaultLabels": "false"
# })
# print(create_result)

# Получаем id созданной доски
# created_board = json.loads(create_result)
# board_id = created_board.get("id")

# 2. GET — получаем информацию о доске
# print("\n=== GET ===")
# get_result = trello_board_action({
#     "action": "get",
#     "idBoard": board_id,
#     "cards": "all",
#     "lists": "all",
#     "labels": "all",
#     "members": "all",
#     "organization": "false",
#     "powerUps": "none"
# })
# print(get_result)

# 3. UPDATE — обновляем имя и описание доски
# print("\n=== UPDATE ===")
# update_result = trello_board_action({
#     "action": "update",
#     "idBoard": board_id,
#     "new_name": "Test Board Updated",
#     "desc": "Updated description",
#     "prefs_permissionLevel": "private"
# })
# print(update_result)

# 4. DELETE — удаляем доску
# print("\n=== DELETE ===")
# delete_result = trello_board_action({
#     "action": "delete",
#     "idBoard": board_id
# })
# print(delete_result)

from find_board_id_by_name import find_board_id_by_name
print(json.loads(find_board_id_by_name({
	"nameBoard": "Test Board"
}))["0"]["id"])