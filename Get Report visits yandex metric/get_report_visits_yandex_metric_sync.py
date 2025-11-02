def get_report_visits_yandex_metric(arguments: dict) -> str:
    """
    Формирует отчёт из Яндекс.Метрики (Reports API) в синхронном режиме
    и возвращает результат в виде строки (CSV или JSON).

    Args:
        arguments (dict): словарь параметров.

        Основные параметры (боевые, соответствуют API Метрики):
            - ids (str): ID счётчика (**обязательный**).
                • Если счётчик публичный — токен не требуется.
                • Если счётчик приватный — обязателен OAuth‑токен.
            - date1 (str): начальная дата периода (формат YYYY-MM-DD).
            - date2 (str): конечная дата периода (формат YYYY-MM-DD).
            - metrics (str, optional): список метрик через запятую.
            - dimensions (str, optional): список группировок через запятую.
            - filters (str, optional): строка фильтрации.
                • В фильтрах можно использовать только измерения (dimensions).
                • Метрики в фильтрах не поддерживаются.
            - preset (str, optional): название предустановленного отчёта.
            - sort (str, optional): поле сортировки (например "-ym:s:pageviews").
                • Можно сортировать только по тем полям, которые указаны в metrics или dimensions.
            - lang (str, optional): язык интерфейса (по умолчанию "en").
            - token (str, optional): OAuth‑токен.
                • Если не указан, берётся из переменной окружения YANDEX_METRIKA_TOKEN.
                • Если токена нет, запрос возможен только к публичным счётчикам.

        Надстройки / тестовые параметры (служебные, не являются частью API, а управляют логикой выгрузки):
            - split (bool, optional): включить авто‑дробление диапазона дат (по умолчанию True).
            - timeout (int, optional): таймаут запроса в секундах (по умолчанию 60).
            - batch_size (int, optional): лимит строк на страницу (по умолчанию 10000).
            - max_rows (int, optional): максимальное количество строк для выгрузки (ограничитель, по умолчанию без лимита).
            - output_format (str, optional): "csv" или "json" (по умолчанию "csv").

    Замечания:
        * Если заданы metrics и dimensions — они перекрывают preset (работает ручной режим).
        * В параметре filters можно использовать только dimensions, метрики там не поддерживаются.
        * В параметре sort можно указывать только поля, которые реально присутствуют в metrics или dimensions.
        * API возвращает максимум 10000 строк за один запрос; для больших выборок используется offset.
        * Даже при использовании offset общее количество строк может быть ограничено самим API.
        * Множественные периоды (comparisonMode) не поддерживаются.
        * Если API возвращает метрики как вложенные списки ([[...]]), используется только первый набор.
        * Параметры split, timeout, batch_size, max_rows и output_format являются надстройками функции
          и не поддерживаются напрямую API Метрики.
        * Если счётчик приватный и токен не указан — запрос завершится ошибкой авторизации.
        * Если счётчик публичный — можно работать без токена.
    """

    import os, requests, pandas as pd, json
    from datetime import datetime, timedelta

    API_URL = "https://api-metrika.yandex.net/stat/v1/data"

    # --- Вспомогательные функции ---
    def auto_date_chunks(date1: str, date2: str, split: bool = True):
        start = datetime.strptime(date1, "%Y-%m-%d")
        end = datetime.strptime(date2, "%Y-%m-%d")

        if not split:
            yield date1, date2
            return

        delta_days = (end - start).days
        if delta_days <= 90:
            step = timedelta(days=7)
            while start <= end:
                chunk_end = min(start + step - timedelta(days=1), end)
                yield start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
                start = chunk_end + timedelta(days=1)
        elif delta_days <= 730:
            while start <= end:
                next_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
                chunk_end = min(next_month - timedelta(days=1), end)
                yield start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
                start = next_month
        else:
            while start <= end:
                next_year = start.replace(month=1, day=1, year=start.year + 1)
                chunk_end = min(next_year - timedelta(days=1), end)
                yield start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
                start = next_year

    def build_dataframe(data: dict) -> pd.DataFrame:
        query = data.get("query", {}) or {}
        dim_names = [d.split(":")[-1] for d in query.get("dimensions", [])]
        met_names = [m.split(":")[-1] for m in query.get("metrics", [])]

        rows = []
        for row in data.get("data", []):
            record = {}
            for j, dim in enumerate(row.get("dimensions", [])):
                record[dim_names[j] if j < len(dim_names) else f"dimension_{j}"] = dim.get("name")
            metrics = row.get("metrics", [])
            if len(metrics) == 1 and isinstance(metrics[0], list):
                metrics = metrics[0]
            for j, val in enumerate(metrics):
                record[met_names[j] if j < len(met_names) else f"metric_{j}"] = val
            rows.append(record)

        return pd.DataFrame(rows, columns=dim_names + met_names)

    def fetch_page(url, params, headers, timeout):
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def fetch_chunk_all_pages(url, params, headers, timeout, batch_size, max_rows=None):
        all_rows, offset, query = [], 0, None
        while True:
            p = params.copy()
            p.update({"limit": batch_size, "offset": offset})
            payload = fetch_page(url, p, headers, timeout)
            if query is None:
                query = payload.get("query", {})
            data_rows = payload.get("data", []) or []
            all_rows.extend(data_rows)
            if max_rows and len(all_rows) >= max_rows:
                all_rows = all_rows[:max_rows]
                break
            if len(data_rows) < batch_size:
                break
            offset += batch_size
        return {"query": query, "data": all_rows}

    # --- Валидация ---
    if not arguments.get("ids"):
        raise ValueError("Не найден параметр 'ids'")
    if not arguments.get("date1") or not arguments.get("date2"):
        raise ValueError("Не заданы даты (date1/date2)")

    # --- Подготовка параметров ---
    metrics, dimensions, filters = arguments.get("metrics"), arguments.get("dimensions"), arguments.get("filters")
    preset, sort = arguments.get("preset"), arguments.get("sort")
    lang = arguments.get("lang", "en")
    token = arguments.get("token") or os.getenv("YANDEX_METRIKA_TOKEN")
    split = arguments.get("split", True)
    timeout = int(arguments.get("timeout", 60))
    batch_size = int(arguments.get("batch_size", 10000))
    max_rows = int(arguments.get("max_rows", 0)) or None
    output_format = arguments.get("output_format", "csv")

    use_manual_config = bool(metrics and dimensions)

    params = {"ids": arguments["ids"], "lang": lang, "date1": arguments["date1"], "date2": arguments["date2"]}
    if use_manual_config:
        params["metrics"], params["dimensions"] = metrics, dimensions
        if filters:
            params["filters"] = filters
    else:
        params["preset"] = preset or "traffic"
    if sort:
        params["sort"] = sort

    headers = {}
    if token:
        headers["Authorization"] = f"OAuth {token}"

    # --- Основная логика ---
    date_chunks = list(auto_date_chunks(params["date1"], params["date2"], split=split))
    all_rows, query = [], None
    for d1, d2 in date_chunks:
        chunk_params = params.copy()
        chunk_params.update({"date1": d1, "date2": d2})
        payload = fetch_chunk_all_pages(API_URL, chunk_params, headers, timeout, batch_size, max_rows)
        if query is None:
            query = payload.get("query", {})
        all_rows.extend(payload.get("data", []))
        if max_rows and len(all_rows) >= max_rows:
            all_rows = all_rows[:max_rows]
            break

    df = build_dataframe({"data": all_rows, "query": query})

    return df.to_csv(index=False) if output_format == "csv" else json.dumps(df.to_dict(orient="records"), ensure_ascii=False, indent=2)
