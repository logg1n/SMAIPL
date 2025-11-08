def get_report_visits_yandex_metric(arguments: dict) -> str:
    """
    Формирует отчёт из Яндекс.Метрики (Reports API) в синхронном режиме
    и возвращает результат в виде строки (CSV или JSON).

    Args:
        arguments (dict): словарь параметров.

        Основные параметры (боевые, соответствуют API Метрики):
            - ids (str): ID счётчика (**обязательный**).
            - date1 (str, формат YYYY-MM-DD): начальная дата периода.
            - date2 (str, формат YYYY-MM-DD): конечная дата периода.
            - metrics (str, optional): список метрик через запятую.
            - dimensions (str, optional): список группировок через запятую.
            - filters (str, optional): строка фильтрации.
            - preset (str, optional): название предустановленного отчёта.
            - sort (str, optional): поле сортировки (например "-ym:s:pageviews").
            - lang (str, optional): язык интерфейса (по умолчанию "en").
            - token (str, optional): OAuth‑токен.

        Надстройки / тестовые параметры (служебные, не являются частью API, а управляют логикой выгрузки):
            - split (bool, optional): включить авто‑дробление диапазона дат (по умолчанию True).
            - timeout (int, optional): общий лимит времени выполнения функции в секундах (по умолчанию 60).
                • Все подзапросы делят этот лимит; если он исчерпан — выполнение прерывается.
            - batch_size (int, optional): лимит строк на страницу (по умолчанию 10000).
            - max_rows (int, optional): максимальное количество строк для выгрузки (по умолчанию без лимита).
            - output_format (str, optional): формат результата: "csv" или "json" (по умолчанию "csv").

    Returns:
        str: результат отчёта в формате CSV или JSON (в зависимости от параметра output_format).

    Замечания:
        * Если заданы metrics и dimensions — они перекрывают preset (работает ручной режим).
        * В параметре filters можно использовать только dimensions, метрики там не поддерживаются.
        * В параметре sort можно указывать только поля, которые реально присутствуют в metrics или dimensions.
        * API возвращает максимум 10000 строк за один запрос; для больших выборок используется offset.
        * Даже при использовании offset общее количество строк может быть ограничено самим API.
        * Множественные периоды (comparisonMode) не поддерживаются.
        * Если API возвращает метрики как вложенные списки ([[...]]),
          функция обрабатывает только первый массив значений.
          Поддержка нескольких наборов метрик (например, при сравнении периодов) не реализована.
        * Параметры split, timeout, batch_size, max_rows и output_format являются надстройками функции
          и не поддерживаются напрямую API Метрики.
        * Если счётчик приватный и токен не указан — запрос завершится ошибкой авторизации.
        * Если счётчик публичный — можно работать без токена.
        * Таймаут контролируется глобально: все подзапросы делят один общий лимит времени.

    """

    # def upload_to_tmpfiles(df: pd.DataFrame, output_format: str = "csv") -> str:
    #     """
	# 	Сохраняет DataFrame во временный файл и загружает его на tmpfiles.org
	# 	"""
    #
    # def fetch_chunk_all_pages_streaming(url, params, headers, batch_size, output_format="csv", file_path="report.csv"):
    #     """
	# 	Стриминговая выгрузка: постранично пишет результат в файл (CSV или NDJSON),
	# 	не накапливая все строки в памяти.
	# 	"""

    import os
    import requests
    import pandas as pd
    import json
    import logging
    import time
    from datetime import datetime, timedelta

    # --- Класс исключений ---
    class YandexMetrikaError(Exception):
        """Специализированное исключение для ошибок работы с API Яндекс.Метрики"""
        pass

    # --- Настройка логирования ---
    log_level = arguments.get("log_level", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    API_URL = "https://api-metrika.yandex.net/stat/v1/data"
    start_time = time.perf_counter()  # ⏱ старт замера общего времени выполнения
    global_timeout = int(arguments.get("timeout", 60))  # общий лимит времени на всю функцию

    # --- Функция вычисления остатка времени ---
    def remaining_timeout() -> float:
        """
        Возвращает количество секунд, оставшихся до истечения глобального лимита.
        Если лимит исчерпан — выбрасывает исключение.
        """
        elapsed = time.perf_counter() - start_time
        left = global_timeout - elapsed
        if left <= 0:
            raise YandexMetrikaError("Превышен общий лимит времени на выполнение запроса")
        return left

    # --- Разбиение диапазона дат на чанки ---
    def auto_date_chunks(date1: str, date2: str, split: bool = True):
        """
        Делит диапазон дат на оптимальные чанки:
          - до 365 дней → месяцы
          - до 5 лет → кварталы
          - больше 5 лет → годы
        """
        try:
            start = datetime.strptime(date1, "%Y-%m-%d")
            end = datetime.strptime(date2, "%Y-%m-%d")
        except ValueError as e:
            raise YandexMetrikaError(f"Неверный формат даты: {e}")

        if start > end:
            raise YandexMetrikaError("date1 не может быть больше date2")

        if not split:
            yield date1, date2
            return

        delta_days = (end - start).days

        # до 1 года → месяцы
        if delta_days <= 365:
            while start <= end:
                next_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
                chunk_end = min(next_month - timedelta(days=1), end)
                yield start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
                start = next_month

        # до 5 лет → кварталы
        elif delta_days <= 1825:
            while start <= end:
                # вычисляем начало следующего квартала
                month = ((start.month - 1) // 3 + 1) * 3 + 1
                year = start.year
                if month > 12:
                    month = 1
                    year += 1
                next_quarter = datetime(year, month, 1)
                chunk_end = min(next_quarter - timedelta(days=1), end)
                yield start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
                start = next_quarter

        # больше 5 лет → годы
        else:
            while start <= end:
                next_year = start.replace(month=1, day=1, year=start.year + 1)
                chunk_end = min(next_year - timedelta(days=1), end)
                yield start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
                start = next_year

    # --- Построение DataFrame из ответа API ---
    def build_dataframe(data: dict) -> pd.DataFrame:
        """
        Преобразует JSON-ответ API в pandas.DataFrame.
        Корректно обрабатывает вложенные массивы metrics.
        """
        query = data.get("query", {}) or {}
        dim_names = [d.split(":")[-1] for d in query.get("dimensions", [])]
        met_names = [m.split(":")[-1] for m in query.get("metrics", [])]

        rows = []
        for row in data.get("data", []):
            record = {}
            # Обработка dimensions
            for j, dim in enumerate(row.get("dimensions", [])):
                record[dim_names[j] if j < len(dim_names) else f"dimension_{j}"] = dim.get("name")

            # Обработка metrics (учёт вложенных списков)
            metrics = row.get("metrics", [])
            if metrics and isinstance(metrics[0], list):
                metrics = metrics[0]

            for j, val in enumerate(metrics):
                record[met_names[j] if j < len(met_names) else f"metric_{j}"] = val

            rows.append(record)

        return pd.DataFrame(rows, columns=dim_names + met_names)

    # --- Запрос страницы ---
    def fetch_page(url, params, headers):
        resp = None
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=remaining_timeout())
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            raise YandexMetrikaError("Превышен общий лимит времени (timeout)")
        except requests.exceptions.ConnectionError:
            raise YandexMetrikaError("Ошибка соединения с API Яндекс.Метрики")
        except requests.exceptions.HTTPError:
            if resp is not None:
                try:
                    error_json = resp.json()
                    message = (
                        error_json.get("message")
                        or error_json.get("errors", [{}])[0].get("message", "")
                    )
                except Exception:
                    message = resp.text
                code = resp.status_code
                if code == 400:
                    raise YandexMetrikaError(f"[400] Неверные параметры запроса: {message}")
                elif code == 401:
                    raise YandexMetrikaError(f"[401] Неавторизован: {message}")
                elif code == 402:
                    raise YandexMetrikaError(f"[402] Превышена квота или требуется оплата: {message}")
                elif code == 403:
                    raise YandexMetrikaError(f"[403] Доступ запрещён: {message}")
                elif code == 404:
                    raise YandexMetrikaError(f"[404] Счётчик или ресурс не найден: {message}")
                elif code == 413:
                    raise YandexMetrikaError(f"[413] Слишком большой запрос: {message}")
                elif code == 429:
                    raise YandexMetrikaError(f"[429] Превышен лимит запросов: {message}")
                elif code >= 500:
                    raise YandexMetrikaError(f"[{code}] Ошибка сервера: {message}")
                else:
                    raise YandexMetrikaError(f"[{code}] Неизвестная ошибка API: {message}")
            else:
                raise YandexMetrikaError("HTTPError, но объект ответа не создан")
        finally:
            time.sleep(0.11)

    def fetch_chunk_all_pages(url, params, headers, batch_size, max_rows=None):
        """
        Загружает все страницы данных (limit+offset) для заданного диапазона дат.
        Учитывает total_rows и sampled из ответа API.
        """
        all_rows = []
        offset = 1
        query = None
        total_rows = None
        sampled_global = False

        while True:
            p = params.copy()
            p.update({"limit": batch_size, "offset": offset})
            payload = fetch_page(url, p, headers)

            if "data" not in payload:
                raise YandexMetrikaError(f"Некорректный ответ API при offset={offset}: {payload}")

            if query is None:
                query = payload.get("query", {})

            # читаем total_rows и sampled
            total_rows = payload.get("total_rows", total_rows)
            if payload.get("sampled"):
                sampled_global = True

            data_rows = payload.get("data", []) or []
            all_rows.extend(data_rows)

            # проверка max_rows
            if max_rows and len(all_rows) >= max_rows:
                logging.warning(f"Достигнут лимит max_rows={max_rows}, обрезаем результат")
                all_rows = all_rows[:max_rows]
                break

            # ✅ ЕДИНСТВЕННАЯ проверка окончания данных
            if len(data_rows) < batch_size:
                break

            offset += batch_size

        if sampled_global:
            logging.warning("⚠️ Данные усечены (sampled=True) — отчёт может быть неполным")

        return {"query": query, "data": all_rows}

    # --- Валидация входных параметров ---
    if not arguments.get("ids"):
        raise YandexMetrikaError("Не найден параметр 'ids'")
    if not arguments.get("date1") or not arguments.get("date2"):
        raise YandexMetrikaError("Не заданы даты (date1/date2)")

    # --- Подготовка параметров запроса ---
    metrics, dimensions, filters = arguments.get("metrics"), arguments.get("dimensions"), arguments.get("filters")
    preset, sort = arguments.get("preset"), arguments.get("sort")
    lang = arguments.get("lang", "en")
    token = arguments.get("token") or os.getenv("YANDEX_METRIKA_TOKEN")
    split = arguments.get("split", True)
    batch_size = int(arguments.get("batch_size", 10000))
    max_rows = int(arguments.get("max_rows", 0)) or None
    output_format = arguments.get("output_format", "csv")

    use_manual_config = bool(metrics and dimensions)
    params = {"ids": arguments["ids"], "lang": lang, "date1": arguments["date1"], "date2": arguments["date2"]}

    if use_manual_config:
        # Преобразуем строки в списки, убираем пробелы и пустые элементы
        if isinstance(metrics, str):
            params["metrics"] = [m.strip() for m in metrics.split(",") if m.strip()]
        else:
            params["metrics"] = metrics

        if isinstance(dimensions, str):
            params["dimensions"] = [d.strip() for d in dimensions.split(",") if d.strip()]
        else:
            params["dimensions"] = dimensions

        # Фильтры передаются как строка (API принимает именно строку)
        if filters:
            params["filters"] = filters
    else:
        # Если metrics/dimensions не заданы — используем готовый пресет
        params["preset"] = preset or "traffic"

    if sort:
        params["sort"] = sort

    headers = {}
    if token:
        headers["Authorization"] = f"OAuth {token}"

    # --- Основная логика выгрузки ---
    # Разбиваем общий диапазон дат на чанки (недели/месяцы/годы)
    date_chunks = list(auto_date_chunks(params["date1"], params["date2"], split=split))
    if not date_chunks:
        raise YandexMetrikaError("Не удалось сформировать диапазоны дат для запроса")

    all_rows, query = [], None

    # Проходим по каждому диапазону дат
    for d1, d2 in date_chunks:
        chunk_params = params.copy()
        chunk_params.update({"date1": d1, "date2": d2})

        # Загружаем все страницы данных для текущего диапазона
        payload = fetch_chunk_all_pages(API_URL, chunk_params, headers, batch_size, max_rows)

        # Сохраняем query (структуру запроса) только один раз
        if query is None:
            query = payload.get("query", {})

        # Добавляем строки в общий список
        all_rows.extend(payload.get("data", []))

        # Если достигнут общий лимит строк — останавливаем выгрузку
        if max_rows and len(all_rows) >= max_rows:
            logging.warning(f"Достигнут общий лимит max_rows={max_rows}, остановка выгрузки")
            all_rows = all_rows[:max_rows]
            break

    # Если данных нет — выбрасываем исключение
    if not all_rows:
        raise YandexMetrikaError("API вернул пустой результат — данных нет по заданным параметрам")

    # Преобразуем результат в DataFrame
    df = build_dataframe({"data": all_rows, "query": query})

    # Замеряем общее время выполнения
    elapsed = time.perf_counter() - start_time
    mem_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)

    logging.info(
        f"Финальная сводка: строк={len(df)}, "
        f"время={elapsed:.2f} сек, "
        f"память={mem_mb:.2f} МБ"
    )

    # Возвращаем результат в нужном формате
    if output_format == "json":
        return df.to_json(orient="records", force_ascii=False)
    else:
        return df.to_csv(index=False)


if __name__ == "__main__":
    tests = [
        {"ids": "44147844", "date1": "2024-01-01", "date2": "2024-01-07"},
        {"ids": "44147844", "date1": "2024-02-01", "date2": "2024-02-07", "metrics": "ym:s:visits"},
        {"ids": "44147844", "date1": "2024-03-01", "date2": "2024-03-31", "dimensions": "ym:s:regionCityName"},
        {"ids": "44147844", "date1": "2024-04-01", "date2": "2024-04-30", "preset": "traffic"},
        {"ids": "44147844", "date1": "2024-05-01", "date2": "2024-05-31", "sort": "-ym:s:visits"},
        {"ids": "44147844", "date1": "2024-06-01", "date2": "2024-06-30", "output_format": "json"},
        {"ids": "44147844", "date1": "2024-07-01", "date2": "2024-07-31", "batch_size": 5000},
        {"ids": "44147844", "date1": "2024-08-01", "date2": "2024-08-31", "max_rows": 100},
        {"ids": "44147844", "date1": "2024-09-01", "date2": "2024-09-30", "filters": "ym:s:regionCityName=='Москва'"},
        {"ids": "44147844", "date1": "2024-10-01", "date2": "2024-10-31", "lang": "ru"},

        # 🔥 Нагруженный запрос: фильтрация + сортировка + несколько метрик и измерений
        {
            "ids": "44147844",
            "date1": "2024-01-01",
            "date2": "2024-12-31",
            "metrics": "ym:s:visits,ym:s:users,ym:s:pageviews",
            "dimensions": "ym:s:regionCityName,ym:s:deviceCategory",
            "filters": "ym:s:regionCityName=='Москва' AND ym:s:deviceCategory=='desktop'",
            "sort": "-ym:s:visits",
            "batch_size": 10000,
            "output_format": "csv"
        },

        # ❌ Негативный тест: фильтрация по метрике (API не поддерживает)
        {
            "ids": "44147844",
            "date1": "2024-01-01",
            "date2": "2024-01-31",
            "metrics": "ym:s:visits",
            "filters": "ym:s:visits>100"  # Ошибка: фильтры работают только с dimensions
        },
    ]

    for i, args in enumerate(tests, start=1):
        print(f"\n=== Тест {i} ===")
        try:
            result = get_report_visits_yandex_metric(args)
            print("✅ Успех, первые символы ответа:")
            print(str(result)[:300])
        except Exception as e:
            print(f"❌ Ошибка: {e}")