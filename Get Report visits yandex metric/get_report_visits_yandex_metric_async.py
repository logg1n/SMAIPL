def get_report_visits_yandex_metric_async(arguments: dict) -> str:
    """
    Формирование отчёта из Яндекс.Метрики (Reports API) в асинхронном режиме.

    Функция автоматически определяет параметры отчёта (preset, даты, язык, ID счётчика)
    из аргументов или текстового запроса, загружает данные параллельно по годовым диапазонам
    через API, преобразует результат в pandas.DataFrame и сохраняет его во временный CSV‑файл,
    возвращая ссылку на файл.

    Основные шаги:
        - Определение preset (явно или по ключевым словам в query).
        - Проверка наличия двух корректных дат в формате YYYY-MM-DD.
        - Разбиение диапазона на годовые интервалы.
        - Асинхронная загрузка данных по каждому интервалу через aiohttp.
        - Сбор всех строк в общий список.
        - Преобразование результата в DataFrame с читаемыми колонками.
        - Сохранение результата во временный CSV и возврат ссылки.

    Особенности:
        * Если в окружении задан OAuth‑токен (YANDEX_METRIKA_TOKEN), запрос выполняется с авторизацией.
        * Если токен отсутствует, используется публичный доступ (только для открытых счётчиков).
        * Если preset не указан явно и не извлечён из query — используется "traffic".
        * Явно переданные параметры имеют приоритет над извлечёнными из query.
        * Если заданы metrics и dimensions — они перекрывают preset (ручной режим).
        * filters применяются только в ручном режиме.
        * Множественные периоды (comparisonMode) не поддерживаются.
        * Если API возвращает метрики как вложенные списки ([[...]]), используется только первый набор.

    Доступные шаблоны (preset):
        traffic — посещаемость
        conversion — конверсии
        hourly — посещаемость по времени суток
        deepness_time — глубина визитов по времени
        deepness_depth — глубина визитов по страницам
        loyalty_newness — новые и возвратные пользователи
        loyalty_period — периодичность визитов
        tech_browsers — браузеры
        tech_platforms — операционные системы
        tech_devices — устройства
        age — возраст пользователей
        age_gender — возраст и пол
        gender — пол пользователей
        search_engines — поисковые системы
        sources_search_phrases — поисковые фразы
        sources_sites — сайты-источники
        sources_social — социальные сети
        geo_country — география по странам
        sources_summary — источники трафика

    Args:
        arguments (dict): словарь с параметрами запроса. Все поля опциональны:
            ids (str): ID счётчика. Может быть указан явно или извлечён из query.
            token (str, optional): OAuth-токен для авторизированного доступа.
              Если не указан используется переменная окружения YANDEX-METRIKA-TOKEN.
            metrics (str, optional): Список метрик через запятую (например: "ym:s:visits,ym:s:pageviews").
            dimensions (str, optional): Список группировок через запятую (например: "ym:s:trafficSource,ym:s:deviceCategory").
            filters (str, optional): Строка фильтрации в формате API (например: "ym:s:deviceCategory=='mobile'").
            date1 (str): Начальная дата периода (YYYY-MM-DD).
            date2 (str): Конечная дата периода (YYYY-MM-DD).
            lang (str, optional): Язык интерфейса (по умолчанию "ru").
            preset (str, optional): Название шаблона отчёта (см. список выше).
            query (str, optional): Человеческий запрос на естественном языке.
                Поддерживается ограниченный парсинг:
                    - тип отчёта (preset) по ключевым словам,
                    - ID счётчика (рядом со словом "счётчик"),
                    - даты в формате YYYY-MM-DD,
                    - язык (русский, английский).
                Остальные параметры (metrics, dimensions, filters) должны задаваться явно.

    Примеры:
        # Вариант 1 — все параметры заданы явно (preset):
        get_report_visits_yandex_metric_async({
            "ids": "44147844",
            "date1": "2024-01-01",
            "date2": "2024-05-01",
            "preset": "conversion"
        })

        # Вариант 2 — ручная конфигурация с метриками, группировками и фильтрами:
        get_report_visits_yandex_metric_async({
            "ids": "44147844",
            "date1": "2024-01-01",
            "date2": "2024-05-01",
            "lang": "ru",
            "metrics": "ym:s:visits,ym:s:pageviews,ym:s:bounceRate",
            "dimensions": "ym:s:trafficSource,ym:s:deviceCategory",
            "filters": "ym:s:deviceCategory=='mobile' AND ym:s:trafficSource=='organic'"
        })

        # Вариант 3 — только query, параметры извлекаются автоматически:
        get_report_visits_yandex_metric_async({
            "query": "Отчёт по устройствам для счётчика 44147844 на русском языке за период с 2024-01-01 по 2024-05-01"
        })

    Returns:
        str: Ссылка на CSV‑файл с данными отчёта.
    """

    import aiohttp
    import asyncio
    import os
    import re
    import tempfile
    import requests
    import json
    import pandas as pd
    from datetime import datetime

    LIMIT = 10000

    ANNOTATIONS = {
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

    LANG_MAP = {
        "русск": "ru", "английск": "en", "english": "en",
    }

    def detect_preset(text: str):
        """
        Определяет тип отчёта (preset) по ключевым словам в тексте.
        """

        t = text.lower()
        for key, preset in ANNOTATIONS.items():
            if key in t:
                return preset
        return None

    def detect_lang(text: str):
        """
        Определяет язык (ru, en) по ключевым словам в тексте.
        """

        t = text.lower()
        for key, code in LANG_MAP.items():
            if key in t:
                return code
        return None

    def detect_ids(text: str) -> str | None:
        """
        Ищет ID счётчика в тексте, если рядом встречается слово, начинающееся на 'счётчик'
        (в любой форме: 'счётчику', 'счётчик:', 'счётчик-44147844' и т.п.)

        Возвращает первое найденное число от 6 до 9 цифр, если оно связано с ключевым словом.
        """

        pattern = r"(сч[её]тчик[\w\-]*)\D{0,5}\b(\d{6,9})\b"
        match = re.search(pattern, text, flags=re.IGNORECASE)

        if match:
            return match.group(2)
        return None


    def detect_dates(text: str) -> tuple[str, str]:
        """
        Ищет ровно две даты в формате YYYY-MM-DD в тексте.
        Если не найдено две корректные даты — выбрасывает ValueError.
        """

        # Ищем все подстроки вида 2024-01-31
        candidates = re.findall(r"\d{4}-\d{2}-\d{2}", text)

        if len(candidates) < 2:
            raise ValueError("Не удалось определить обе даты (date1 и date2)")

        # Пробуем распарсить и нормализовать
        try:
            d1 = datetime.strptime(candidates[0], "%Y-%m-%d")
            d2 = datetime.strptime(candidates[1], "%Y-%m-%d")
        except ValueError:
            raise ValueError("Неверный формат даты, используйте YYYY-MM-DD")

        # Сортируем по возрастанию
        if d1 > d2:
            d1, d2 = d2, d1

        return d1.strftime("%Y-%m-%d"), d2.strftime("%Y-%m-%d")

    def parse_request(user_request: str) -> dict:
        """
        Разбирает текстовый запрос и извлекает из него параметры:
        ID счётчика, preset, язык, даты и сам исходный запрос.
        """

        d1, d2 = detect_dates(user_request)
        return {
            "ids": detect_ids(user_request),
            "preset": detect_preset(user_request),
            "lang": detect_lang(user_request) or "en",
            "date1": d1,
            "date2": d2,
            "query": user_request
        }

    def build_dataframe(data: dict) -> pd.DataFrame:
        """
        Преобразует ответ API Яндекс.Метрики в pandas.DataFrame
        с колонками по измерениям и метрикам.
        """

        query = data.get("query", {}) or {}

        # Имена колонок из запроса
        dim_names = [d.split(":")[-1] for d in query.get("dimensions", [])]
        met_names = [m.split(":")[-1] for m in query.get("metrics", [])]

        rows = []
        for row in data.get("data", []):
            record = {}

            # Измерения
            for j, dim in enumerate(row.get("dimensions", [])):
                col_name = dim_names[j] if j < len(dim_names) else f"dimension_{j}"
                record[col_name] = dim.get("name")

            # Метрики
            metrics = row.get("metrics", [])
            if len(metrics) == 1 and isinstance(metrics[0], list):
                metrics = metrics[0]

            for j, val in enumerate(metrics):
                col_name = met_names[j] if j < len(met_names) else f"metric_{j}"
                record[col_name] = val

            rows.append(record)

        return pd.DataFrame(rows, columns=dim_names + met_names)

    def upload_to_tmpfiles(df: pd.DataFrame) -> str:
        """
        Сохраняет DataFrame во временный CSV‑файл и загружает его на tmpfiles.org,
        возвращая ссылку на файл.
        """

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w", encoding="utf-8",
                                         errors="replace") as tmp:
            df.to_csv(tmp.name, index=False)

        try:
            with open(tmp.name, "rb") as f:
                response = requests.post("https://tmpfiles.org/api/v1/upload", files={"file": f}, timeout=30)
                response.raise_for_status()
                data = response.json()
            return data["data"]["url"]
        finally:
            os.remove(tmp.name)

    def split_by_year(date1: str, date2: str):
        """
        Делит заданный период на годовые интервалы.
        """

        start = datetime.strptime(date1, "%Y-%m-%d")
        end = datetime.strptime(date2, "%Y-%m-%d")
        chunks = []
        current = start
        while current.year <= end.year:
            year_end = datetime(current.year, 12, 31)
            if year_end > end:
                year_end = end
            chunks.append((current.strftime("%Y-%m-%d"), year_end.strftime("%Y-%m-%d")))
            current = datetime(current.year + 1, 1, 1)
        return chunks

    async def fetch_page(session, url, params, headers):
        """
        Загружает одну страницу отчёта из API Яндекс.Метрики.
        """

        async with session.get(url, params=params, headers=headers, timeout=60) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def fetch_all(url, params, headers, chunks):
        """
        Асинхронно загружает данные за несколько периодов (годовых интервалов).
        """
        async with aiohttp.ClientSession() as session:
            tasks = []
            for d1, d2 in chunks:
                p = params.copy()
                p["date1"], p["date2"] = d1, d2
                tasks.append(fetch_page(session, url, p, headers))
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return [r for r in results if isinstance(r, dict)]

    # --- Основная логика ---

    # Если передан текстовый запрос (query), пробуем извлечь из него параметры:
    # preset, даты, язык, ids счётчика и т.д.
    if arguments.get("query"):
        parsed = parse_request(arguments["query"])
        # Добавляем только те параметры, которых ещё нет в arguments
        for k, v in parsed.items():
            if k not in arguments:
                arguments[k] = v

    # Проверяем, что указан ID счётчика (обязательный параметр)
    if not arguments.get("ids"):
        raise ValueError("Не найден параметр 'ids'")

    # Базовый URL для Reports API Яндекс.Метрики
    url = "https://api-metrika.yandex.net/stat/v1/data"

    # Извлекаем параметры ручной конфигурации (metrics, dimensions, filters)
    metrics, dimensions, filters = arguments.get("metrics"), arguments.get("dimensions"), arguments.get("filters")

    # Если заданы и метрики, и группировки → включаем ручной режим (игнорируем preset)
    use_manual_config = bool(metrics and dimensions)

    # Базовые параметры запроса
    params = {"ids": arguments["ids"], "limit": LIMIT}

    if use_manual_config:
        # Ручной режим: используем metrics + dimensions (+ filters при наличии)
        params["metrics"] = metrics
        params["dimensions"] = dimensions
        if filters:
            params["filters"] = filters
    else:
        # Автоматический режим: используем preset (или "traffic" по умолчанию)
        params["preset"] = arguments.get("preset") or "traffic"

    # Добавляем даты и язык, если они заданы
    for p in ["date1", "date2", "lang"]:
        if arguments.get(p):
            params[p] = arguments[p]

    # Заголовки запроса (авторизация через OAuth-токен)
    headers = {}
    token = arguments.get("token") or os.getenv("YANDEX_METRIKA_TOKEN")
    if token:
        headers["Authorization"] = f"OAuth {token}"

    # Проверяем, что обе даты заданы
    date1 = arguments.get("date1")
    date2 = arguments.get("date2")
    if not date1 or not date2:
        raise ValueError("Не заданы даты (date1/date2)")

    # Разбиваем диапазон на годовые интервалы (чтобы не перегружать API)
    chunks = split_by_year(date1, date2)

    # Асинхронно загружаем данные по всем интервалам
    results = asyncio.run(fetch_all(url, params, headers, chunks))

    # Собираем все строки данных в единый список
    all_rows, query = [], None
    for r in results:
        if query is None:
            # Сохраняем описание запроса (query) из первого ответа
            query = r.get("query", {})
        # Добавляем строки данных
        all_rows.extend(r.get("data", []))

    # Преобразуем результат в DataFrame с читаемыми колонками
    df = build_dataframe({"data": all_rows, "query": query})

    # Сохраняем DataFrame во временный CSV и возвращаем ссылку на файл
    return upload_to_tmpfiles(df)

import time

if __name__ == "__main__":
    test_cases = [
        # 1. Базовый рабочий сценарий (preset)
        {"ids": "44147844", "date1": "2024-01-01", "date2": "2024-01-31", "preset": "traffic"},

        # 2. Ручной режим (metrics + dimensions)
        {"ids": "44147844", "date1": "2024-01-01", "date2": "2024-01-31",
         "metrics": "ym:s:visits,ym:s:pageviews", "dimensions": "ym:s:trafficSource"},

        # 3. Ручной режим с фильтром
        {"ids": "44147844", "date1": "2024-01-01", "date2": "2024-01-31",
         "metrics": "ym:s:visits", "dimensions": "ym:s:deviceCategory",
         "filters": "ym:s:deviceCategory=='mobile'"},

        # 4. Только query (извлечение параметров)
        {"query": "Отчёт по устройствам для счётчика 44147844 за период с 2024-01-01 по 2024-01-31"},

        # 5. Query без ID (ошибка ids)
        {"query": "Отчёт по устройствам за январь 2024"},

        # 6. Query с некорректным ID
        {"query": "Отчёт по устройствам для счётчика 123"},

        # 7. Query с перепутанными датами
        {"query": "Отчёт по посещаемости для счётчика 44147844 за период с 2024-02-01 по 2024-01-01"},

        # 8. Query с невалидной датой (месяц 13)
        {"query": "Отчёт по устройствам для счётчика 44147844 за период с 2024-13-01 по 2024-01-31"},

        # 9. Query с указанием языка (русский)
        {"query": "Отчёт по устройствам для счётчика 44147844 на русском языке за январь 2024"},

        # 10. Query на английском
        {"query": "Report by devices for counter 44147844 in English from 2024-01-01 to 2024-01-31"},

        # 11. Query на испанском (не поддерживается → lang=en по умолчанию)
        {"query": "Informe por dispositivos para el contador 44147844 en español desde 2024-01-01 hasta 2024-01-31"},

        # 12. Query на китайском (не поддерживается → lang=en по умолчанию)
        {"query": "Отчёт по устройствам для счётчика 44147844 на китайском языке за январь 2024"},

        # 13. Нет дат (ошибка)
        {"ids": "44147844", "preset": "traffic"},

        # 14. Нет preset и нет metrics (ошибка)
        {"ids": "44147844", "date1": "2024-01-01", "date2": "2024-01-31"},

        # 15. Альтернативный формат дат (DD-MM-YYYY)
        {"ids": "44147844", "date1": "01-01-2024", "date2": "31-01-2024", "preset": "traffic"},

        # 16. Неверные metrics/dimensions (API вернёт ошибку)
        {"ids": "44147844", "date1": "2024-01-01", "date2": "2024-01-31",
         "metrics": "visits,pageviews", "dimensions": "trafficSource"},

        # 17. Неверный фильтр (API вернёт ошибку)
        {"ids": "44147844", "date1": "2024-01-01", "date2": "2024-01-31",
         "metrics": "ym:s:visits", "dimensions": "ym:s:trafficSource",
         "filters": "ym:s:deviceCategory=mobile"},

        # 18. Большой диапазон (2010–2024, проверка нарезки по годам)
        {"ids": "44147844", "date1": "2010-01-01", "date2": "2024-01-31", "preset": "traffic"},

        # 19. Неверный preset
        {"ids": "44147844", "date1": "2024-01-01", "date2": "2024-01-31", "preset": "unknown_preset"},

        # 20. Пустой словарь (ошибка ids)
        {},

        # 21. Неверный токен (ожидаем 403)
        {"ids": "44147844", "date1": "2024-01-01", "date2": "2024-01-31", "preset": "traffic", "token": "fake_token"},

        # 22. Пустой ответ (фильтр, который ничего не вернёт)
        {"ids": "44147844", "date1": "2024-01-01", "date2": "2024-01-31",
         "metrics": "ym:s:visits", "dimensions": "ym:s:deviceCategory",
         "filters": "ym:s:deviceCategory=='nonexistent'"},
        # 23. Очень короткий диапазон (один день)
        {"ids": "44147844", "date1": "2024-01-01", "date2": "2024-01-01", "preset": "traffic"},

        # 24. Диапазон в будущем (данных ещё нет)
        {"ids": "44147844", "date1": "2030-01-01", "date2": "2030-01-31", "preset": "traffic"},

        # 25. Диапазон в прошлом (до появления счётчика, пустой ответ)
        {"ids": "44147844", "date1": "1990-01-01", "date2": "1990-12-31", "preset": "traffic"},

        # 26. Огромный диапазон (проверка нарезки и устойчивости)
        {"ids": "44147844", "date1": "2000-01-01", "date2": "2025-01-01", "preset": "traffic"},

        # 27. Некорректный ID (слишком короткий)
        {"ids": "123", "date1": "2024-01-01", "date2": "2024-01-31", "preset": "traffic"},

        # 28. Некорректный ID (слишком длинный)
        {"ids": "1234567890123", "date1": "2024-01-01", "date2": "2024-01-31", "preset": "traffic"},

        # 29. Некорректный язык (lang="xx")
        {"ids": "44147844", "date1": "2024-01-01", "date2": "2024-01-31", "preset": "traffic", "lang": "xx"},

        # 30. Ограничение по страницам (MAX_PAGES=1 → должно упасть)
        {"ids": "44147844", "date1": "2010-01-01", "date2": "2024-01-31", "preset": "traffic", "max_pages": 1}

    ]


    for i, args in enumerate(test_cases, start=1):
        print(f"\n--- Тест {i} ---")
        start_time = time.time()
        try:
            result = get_report_visits_yandex_metric_async(args)
            elapsed = time.time() - start_time
            print(f"✅ Успех: {result}")
            print(f"⏱ Время выполнения: {elapsed:.2f} сек.")
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"❌ Ошибка: {e}")
            print(f"⏱ Время до ошибки: {elapsed:.2f} сек.")
