def get_report_visits_yandex_metric(arguments: dict):
    """
    Формирование отчёта по посещаемости сайта из Яндекс.Метрики (Reports API).

    Функция выполняет:
    1. Постраничную загрузку данных из API (с учётом limit/offset).
    2. Сбор всех строк в общий список.
    3. Преобразование результата в pandas.DataFrame с читаемыми колонками.

    Args:
        arguments (dict): словарь с параметрами запроса:
            - ids (str): ID счётчика Яндекс.Метрики (обязательный).
            - date1 (str): Начальная дата периода (YYYY-MM-DD).
            - date2 (str): Конечная дата периода (YYYY-MM-DD).
            - lang (str): Язык интерфейса (опционально).

    Returns:
        pd.DataFrame: Таблица с данными отчёта.
    """
    # Импорты внутри функции (по твоему требованию)
    import requests
    import pandas as pd

    from pandas import DataFrame
    from typing import ClassVar

    class Utils:
        """
        Вспомогательный класс для хранения параметров постраничной загрузки
        и преобразования ответа API в DataFrame.
        """
        OFFSET: ClassVar[int] = 1      # смещение (начинается с 1 в API Метрики)
        TOTAL_ROWS: ClassVar[int] = 0  # общее количество строк в отчёте
        LIMIT: ClassVar[int] = 1000    # максимальное количество строк за один запрос

        @staticmethod
        def build_dataframe(data: dict) -> pd.DataFrame:
            """
            Универсальное преобразование JSON-ответа Яндекс.Метрики в DataFrame.

            - Колонки формируются на основе query.dimensions и query.metrics.
            - Значения берутся из массива data.
            - Автоматически очищаются имена колонок (ym:s:date → date).

            Args:
                data (dict): JSON-ответ от API Метрики, содержащий ключи "query" и "data".

            Returns:
                pd.DataFrame: Таблица с данными отчёта.
            """
            # Берём список измерений и метрик из запроса
            dim_names = data.get("query", {}).get("dimensions", [])
            metric_names = data.get("query", {}).get("metrics", [])

            rows = []
            for row in data.get("data", []):
                record = {}

                # dimensions → значения по именам из query
                for i, dim in enumerate(row.get("dimensions", [])):
                    # если имя есть в query, используем его, иначе подставляем dimension_i
                    col_name = dim_names[i] if i < len(dim_names) else f"dimension_{i}"
                    record[col_name] = dim.get("name")

                # metrics → значения по именам из query
                for j, val in enumerate(row.get("metrics", [])):
                    # если имя есть в query, используем его, иначе подставляем metric_j
                    col_name = metric_names[j] if j < len(metric_names) else f"metric_{j}"
                    record[col_name] = val

                rows.append(record)

            # Собираем DataFrame из списка словарей
            df = pd.DataFrame(rows)

            # Очистка имён колонок: убираем префиксы ym:s:
            df = df.rename(columns={c: c.split(":")[-1] for c in df.columns})

            return df

    # Проверяем наличие обязательного параметра ids
    if "ids" not in arguments:
        raise ValueError('Не найден обязательный параметр "ids"')

    url = "https://api-metrika.yandex.net/stat/v1/data"
    all_rows = []  # сюда будем собирать все строки отчёта

    # Цикл постраничной загрузки
    while True:
        # Формируем параметры запроса
        params = {
            "ids": arguments["ids"],
            "metrics": ",".join([
                "ym:s:visits",
                "ym:s:users",
                "ym:s:pageviews",
                "ym:s:bounceRate",
                "ym:s:avgVisitDurationSeconds",
                "ym:s:pageDepth",
                "ym:s:percentNewVisitors"
            ]),
            "dimensions": "ym:s:date,ym:s:trafficSource,ym:s:deviceCategory,ym:s:regionCountry",
            "limit": Utils.LIMIT,
            "offset": Utils.OFFSET,
        }

        # Добавляем опциональные параметры (date1, date2, lang)
        for p in ["date1", "date2", "lang"]:
            if p in arguments:
                params[p] = arguments[p]

        # Выполняем запрос
        r = requests.get(url, params=params)
        r.raise_for_status()
        result = r.json()

        # Добавляем строки в общий список
        rows = result.get("data", [])
        all_rows.extend(rows)

        # Обновляем общее количество строк
        Utils.TOTAL_ROWS = result.get("total_rows", len(all_rows))

        # Проверяем, все ли строки загружены
        if Utils.OFFSET + Utils.LIMIT >= Utils.TOTAL_ROWS:
            break

        # Смещаем offset для следующего запроса
        Utils.OFFSET += Utils.LIMIT

    # Преобразуем в DataFrame с читаемыми колонками
    return Utils.build_dataframe({"data": all_rows, "query": result.get("query")})
