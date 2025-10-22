import requests
import pandas as pd
import io


def get_report_visits_yandex_metric(ids: str, date1: str, date2: str) -> str:
    """
    Формирование отчёта по посещаемости сайта из Яндекс.Метрики (Reports API).

    Функция отправляет запрос к API, получает данные по визитам, пользователям,
    просмотрам и качеству трафика, преобразует их в таблицу и возвращает результат
    в формате CSV-строки.

    Args:
        ids (str): ID счётчика Яндекс.Метрики (например, "44147844" для демо-счётчика).
        date1 (str): Начальная дата периода в формате YYYY-MM-DD.
        date2 (str): Конечная дата периода в формате YYYY-MM-DD.

    Returns:
        str: Данные отчёта в виде CSV-строки (разделитель — запятая, кодировка UTF-8).
             Включает разрезы по дате, источнику трафика, устройствам и странам.
    """

    # Базовый URL для Reports API
    url = "https://api-metrika.yandex.net/stat/v1/data"

    # Формируем параметры запроса
    params = {
        "ids": ids,  # ID счётчика
        "metrics": ",".join([  # список метрик для анализа посещаемости
            "ym:s:visits",                  # визиты
            "ym:s:users",                   # пользователи
            "ym:s:pageviews",               # просмотры страниц
            "ym:s:bounceRate",              # показатель отказов
            "ym:s:avgVisitDurationSeconds", # средняя длительность визита
            "ym:s:pageDepth",               # глубина просмотра
            "ym:s:percentNewVisitors"       # доля новых посетителей
        ]),
        # Разрезы (dimensions) — группировка данных
        "dimensions": "ym:s:date,ym:s:trafficSource,ym:s:deviceCategory,ym:s:regionCountry",
        "date1": date1,  # начальная дата
        "date2": date2,  # конечная дата
        "limit": 10000   # максимальное количество строк (по умолчанию 100, максимум 100000)
    }

    # Отправляем GET-запрос к API
    r = requests.get(url, params=params)

    # Проверяем, что запрос выполнен успешно (иначе вызовет исключение)
    r.raise_for_status()

    # Преобразуем ответ в JSON
    data = r.json()

    # Преобразуем JSON в DataFrame
    df = pd.DataFrame([
        {
            "date": row["dimensions"][0]["name"],          # дата
            "trafficSource": row["dimensions"][1]["name"], # источник трафика
            "device": row["dimensions"][2]["name"],        # устройство
            "country": row["dimensions"][3]["name"],       # страна
            "visits": row["metrics"][0],                   # визиты
            "users": row["metrics"][1],                    # пользователи
            "pageviews": row["metrics"][2],                # просмотры страниц
            "bounceRate": row["metrics"][3],               # показатель отказов
            "avgDurationSec": row["metrics"][4],           # средняя длительность визита
            "pageDepth": row["metrics"][5],                # глубина просмотра
            "percentNewVisitors": row["metrics"][6],       # доля новых посетителей
        }
        for row in data["data"]  # перебираем строки ответа
    ])

    # Сохраняем DataFrame в строку CSV
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
    return csv_buffer.getvalue()


# 🔹 Пример вызова функции
csv_data = get_report_visits_yandex_metric("44147844", "2025-10-01", "2025-10-21")
print(csv_data[:200])  # выводим первые 200 символов CSV
