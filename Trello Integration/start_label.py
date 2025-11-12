import json
from trello_label_action import trello_label_action

# ⚠️ Укажи ID тестовой доски, где можно безопасно создавать метки
TEST_BOARD_ID = "6913598a9f8ae2b31eabff0e"

print("=== CREATE LABEL ===")
create_result = trello_label_action({
    "action": "create",
    "idBoard": TEST_BOARD_ID,
    "name": "Test Label",
    "color": "blue"
})
print(create_result)

# Получаем id созданной метки
created_label = json.loads(create_result)
label_id = created_label.get("id")

print("\n=== GET LABEL ===")
get_result = trello_label_action({
    "action": "get",
    "idLabel": label_id
})
print(get_result)

print("\n=== UPDATE LABEL (rename + color) ===")
update_result = trello_label_action({
    "action": "update",
    "idLabel": label_id,
    "name": "Updated Label",
    "color": "red"
})
print(update_result)

print("\n=== DELETE LABEL ===")
delete_result = trello_label_action({
    "action": "delete",
    "idLabel": label_id
})
print(delete_result)

print("=== CREATE LABEL ===")
create_result = trello_label_action({
    "action": "create",
    "idBoard": TEST_BOARD_ID,
    "name": "Test Label",
    "color": "green"
})
print(create_result)