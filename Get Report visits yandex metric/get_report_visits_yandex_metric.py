def get_report_visits_yandex_metric(arguments: dict) -> str:
    """
	Формирование отчёта из Яндекс.Метрики (Reports API).
	Используются готовые шаблоны API Отчётов Яндекс.Метрики.

	Функция выполняет:
	    - Определение нужного preset (либо из аргументов, либо по аннотациям).
	    - Постраничную загрузку данных из API (limit/offset).
	    - Сбор всех строк в общий список.
	    - Преобразование результата в pandas.DataFrame с читаемыми колонками.
	    - Сохранение результата во временный CSV и возврат ссылки на файл.

	Особенности:
	    * Если в окружении задан OAuth‑токен (YANDEX_METRIKA_TOKEN), запрос выполняется с авторизацией.
	    * Если токен отсутствует, используется публичный доступ (работает только для открытых счётчиков).

	Доступные шаблоны (preset):
	    - traffic — посещаемость
	    - conversion — конверсии
	    - hourly — посещаемость по времени суток
	    - deepness_time — глубина визитов по времени
	    - deepness_depth — глубина визитов по страницам
	    - loyalty_newness — новые и возвратные пользователи
	    - loyalty_period — периодичность визитов
	    - tech_browsers — браузеры
	    - tech_platforms — операционные системы
	    - tech_devices — устройства
	    - age — возраст пользователей
	    - age_gender — возраст и пол
	    - gender — пол пользователей
	    - search_engines — поисковые системы
	    - sources_search_phrases — поисковые фразы
	    - sources_sites — сайты-источники
	    - sources_social — социальные сети
	    - geo_country — география по странам
	    - sources_summary — источники трафика

	Args:
	    arguments (dict): словарь с параметрами запроса:
	        ids (str): ID счётчика Яндекс.Метрики (обязательный).
	        date1 (str): Начальная дата периода (YYYY-MM-DD).
	        date2 (str): Конечная дата периода (YYYY-MM-DD).
	        lang (str, optional): Язык интерфейса (по умолчанию "ru").
	        preset (str, optional): Название шаблона отчёта (см. список выше).
	        query (str, optional): Человеческий запрос (например: "отчет по посещаемости").
	            Используется для автоматического выбора preset.

	Returns:
	    str: Ссылка на CSV‑файл с данными отчёта, загруженный на tmpfiles.org.

	Examples:
	    >>> get_report_visits_yandex_metric({
	    ...     "ids": "44147844",
	    ...     "date1": "2023-01-01",
	    ...     "date2": "2023-01-31",
	    ...     "preset": "conversion"
	    ... })
	    'https://tmpfiles.org/abc123'

    	>>> get_report_visits_yandex_metric({
    	...     "ids": "44147844",
    	...     "date1": "2023-01-01",
    	...     "date2": "2023-01-31",
    	...     "query": "отчет по социальным сетям"
    	... })
    	'https://tmpfiles.org/xyz789'
	"""


    import requests
    import re
    import tempfile
    import pandas as pd
    import os
    from typing import ClassVar

    class Utils:
        OFFSET: ClassVar[int] = 1      # смещение (начинается с 1 в API Метрики)
        TOTAL_ROWS: ClassVar[int] = 0  # общее количество строк в отчёте
        LIMIT: ClassVar[int] = 10000   # максимальное количество строк за один запрос

        # словарь: ключевые слова/синонимы → preset
        ANNOTATIONS: ClassVar[dict[str, str]] = {
            "отчет по посещаемости": "traffic",
            "отчет по конверсиям": "conversion",
            "отчет по посещаемости по времени суток": "hourly",
            "отчет по глубине визитов по времени": "deepness_time",
            "отчет по глубине визитов по страницам": "deepness_depth",
            "отчет по новым и возвратным пользователям": "loyalty_newness",
            "отчет по периодичности визитов": "loyalty_period",
            "отчет по браузерам": "tech_browsers",
            "отчет по операционным системам": "tech_platforms",
            "отчет по устройствам": "tech_devices",
            "отчет по возрасту пользователей": "age",
            "отчет по возрасту и полу": "age_gender",
            "отчет по полу пользователей": "gender",
            "отчет по поисковым системам": "search_engines",
            "отчет по поисковым фразам": "sources_search_phrases",
            "отчет по сайтам-источникам": "sources_sites",
            "отчет по социальным сетям": "sources_social",
            "отчет по географии пользователей по странам": "geo_country",
            "отчет по источникам трафика": "sources_summary"
        }

        @staticmethod
        def detect_preset(user_request: str) -> str | None:
            """Определяет preset по тексту запроса пользователя."""
            text = user_request.lower()
            for key, preset in Utils.ANNOTATIONS.items():
                if key in text:
                    return preset
            for key, preset in Utils.ANNOTATIONS.items():
                if re.search(rf"\b{re.escape(key)}\b", text):
                    return preset
            return None

        @staticmethod
        def build_dataframe(data: dict) -> pd.DataFrame:
            """Преобразует JSON-ответ Метрики в DataFrame."""
            query = data.get("query", {}) or {}
            dim_names = [d.split(":")[-1] for d in query.get("dimensions", [])]
            met_names = [m.split(":")[-1] for m in query.get("metrics", [])]

            rows = []
            for row in data.get("data", []):
                record = {}
                # добавляем измерения
                for j, dim in enumerate(row.get("dimensions", [])):
                    record[dim_names[j] if j < len(dim_names) else f"dimension_{j}"] = dim.get("name")
                # добавляем метрики
                metrics = row.get("metrics", [])
                if len(metrics) == 1 and isinstance(metrics[0], list):
                    metrics = metrics[0]
                for j, val in enumerate(metrics):
                    record[met_names[j] if j < len(met_names) else f"metric_{j}"] = val
                rows.append(record)
            return pd.DataFrame(rows)

        @staticmethod
        def upload_to_tmpfiles(df: pd.DataFrame) -> str:
            """
            Сохраняет DataFrame во временный CSV и загружает на tmpfiles.org.
            Возвращает ссылку для скачивания.
            """
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
                df.to_csv(tmp.name, index=False, encoding="utf-8")
                tmp.flush()
                with open(tmp.name, "rb") as f:
                    response = requests.post("https://tmpfiles.org/api/v1/upload", files={"file": f})
                    response.raise_for_status()
                    data = response.json()
            return data["data"]["url"]

    # --- Проверка обязательных параметров ---
    if "ids" not in arguments:
        raise ValueError('Не найден обязательный параметр "ids"')

    url = "https://api-metrika.yandex.net/stat/v1/data"
    all_rows = []

    # --- Определяем preset ---
    preset = arguments.get("preset")
    if not preset and "query" in arguments:
        preset = Utils.detect_preset(arguments["query"])
    if not preset:
        preset = "traffic"  # дефолтный вариант

    # --- Цикл постраничной загрузки ---
    while True:
        params = {
            "ids": arguments["ids"],
            "preset": preset,
            "limit": Utils.LIMIT,
            "offset": Utils.OFFSET,
        }
        for p in ["date1", "date2", "lang"]:
            if p in arguments:
                params[p] = arguments[p]

        # токен берём из переменной окружения
        headers = {}
        token = os.getenv("YANDEX_METRIKA_TOKEN")
        if token:
            headers["Authorization"] = f"OAuth {token}"

        # выполняем запрос
        r = requests.get(url, params=params, headers=headers)
        r.raise_for_status()
        result = r.json()

        # добавляем строки в общий список
        rows = result.get("data", [])
        all_rows.extend(rows)

        # проверяем, нужно ли грузить следующую страницу
        Utils.TOTAL_ROWS = result.get("total_rows", len(all_rows))
        if Utils.OFFSET + Utils.LIMIT >= Utils.TOTAL_ROWS:
            break
        Utils.OFFSET += Utils.LIMIT

    # преобразуем в DataFrame
    df = Utils.build_dataframe({"data": all_rows, "query": result.get("query")})

    # сохраняем и возвращаем ссылку
    return Utils.upload_to_tmpfiles(df)


df = get_report_visits_yandex_metric({
    "ids": "44147844",
    "lang": "ru",
    "date1": "2023-01-01",
    "date2": "2023-01-31",
    "query": "отчет по конверсиям"
})


print(df)
