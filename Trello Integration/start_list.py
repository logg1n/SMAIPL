# import json
# from trello_list_action import trello_list_action
#
# # ⚠️ Укажи ID тестовой доски, где можно безопасно создавать списки
# TEST_BOARD_ID = "6913598a9f8ae2b31eabff0e"
#
# print("=== CREATE LIST ===")
# create_result = trello_list_action({
#     "action": "create",
#     "idBoard": TEST_BOARD_ID,
#     "nameList": "Test List"
# })
# print(create_result)
#
# # Получаем id созданного списка
# created_list = json.loads(create_result)
# list_id = created_list.get("id")
#
# print("\n=== GET LIST ===")
# get_result = trello_list_action({
#     "action": "get",
#     "idList": list_id
# })
# print(get_result)
#
# print("\n=== UPDATE LIST (rename) ===")
# update_result = trello_list_action({
#     "action": "update",
#     "idList": list_id,
#     "newNameList": "Renamed Test List"
# })
# print(update_result)
#
# print("\n=== UPDATE LIST (archive) ===")
# archive_result = trello_list_action({
#     "action": "update",
#     "idList": list_id,
#     "closed": "true"   # Trello API ожидает строку "true"/"false"
# })
# print(archive_result)
#
# print("\n=== UPDATE LIST (unarchive) ===")
# unarchive_result = trello_list_action({
#     "action": "update",
#     "idList": list_id,
#     "closed": "false"
# })
# print(unarchive_result)
import json

from trello_search import trello_search

idBoard: str = trello_search({
	"query": "Test Board",
	"modelTypes": "boards",
	"board_fields": "id"
})
print(json.loads(idBoard).get("boards")[0].get("id"))