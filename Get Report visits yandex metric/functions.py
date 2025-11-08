"""
stream = arguments.get("stream", False)
file_path = arguments.get("file_path", f"report.{output_format}")

if stream:
    logging.info(f"Включён стриминговый режим, результат будет писаться в {file_path}")
    return fetch_chunk_all_pages_streaming(
        API_URL, params, headers,
        batch_size=batch_size,
        output_format=output_format,
        file_path=file_path
    )
else:
    # старый путь: собираем всё в память
    payload = fetch_chunk_all_pages(API_URL, params, headers, batch_size, max_rows)
    ...

"""


def fetch_chunk_all_pages_streaming(url, params, headers, batch_size, output_format="csv", file_path="report.csv"):
	"""
	Стриминговая выгрузка: постранично пишет результат в файл (CSV или NDJSON),
	не накапливая все строки в памяти.
	"""
	import pandas as pd

	offset = 1
	query = None
	sampled_global = False
	header_written = False

	with open(file_path, "w", encoding="utf-8") as f:
		while True:
			p = params.copy()
			p.update({"limit": batch_size, "offset": offset})
			payload = fetch_page(url, p, headers)

			if "data" not in payload:
				raise YandexMetrikaError(f"Некорректный ответ API при offset={offset}: {payload}")

			if query is None:
				query = payload.get("query", {})

			if payload.get("sampled"):
				sampled_global = True

			data_rows = payload.get("data", []) or []
			if not data_rows:
				break

			df = build_dataframe({"data": data_rows, "query": query})

			if output_format == "csv":
				df.to_csv(f, index=False, header=not header_written)
			else:
				# JSON построчно (NDJSON), чтобы не держать всё в памяти
				f.writelines(df.to_json(orient="records", lines=True, force_ascii=False) + "\n")

			header_written = True
			if len(data_rows) < batch_size:
				break
			offset += batch_size

	if sampled_global:
		logging.warning("⚠️ Данные усечены (sampled=True) — отчёт может быть неполным")

	return file_path

"""
    upload_external = arguments.get("upload_external", False)
    
        # В конце функции исправить:
    if upload_external and len(df) > row_limit:
        logging.warning(f"Размер отчёта {len(df)} строк превышает {row_limit}, выгружаем во внешний файл")
        try:
            return upload_to_tmpfiles(df, output_format=output_format)
        except YandexMetrikaError as e:
            logging.error(f"Не удалось выгрузить на внешний сервер: {e}")
            # Продолжаем с обычной выгрузкой
            logging.info("Возвращаем данные в обычном режиме")

    # обычный режим
    if output_format == "json":  # ✅ используем переменную
        return df.to_json(orient="records", force_ascii=False)
    else:
        return df.to_csv(index=False)
"""

def upload_to_tmpfiles(df: pd.DataFrame, output_format: str = "csv") -> str:
	"""
	Сохраняет DataFrame во временный файл и загружает его на tmpfiles.org
	"""
	import tempfile

	suffix = ".csv" if output_format == "csv" else ".json"

	# Создаем временный файл
	with tempfile.NamedTemporaryFile(suffix=suffix, delete=False, mode="w",
									 encoding="utf-8", errors="replace") as tmp_file:
		if output_format == "csv":
			df.to_csv(tmp_file, index=False)  # ✅ Записываем в файловый объект
		else:
			df.to_json(tmp_file, orient="records", force_ascii=False)
		tmp_path = tmp_file.name

	try:
		# Загружаем на внешний сервис
		with open(tmp_path, "rb") as f:
			response = requests.post(
				"https://tmpfiles.org/api/v1/upload",
				files={"file": f},
				timeout=30
			)
			response.raise_for_status()
			data = response.json()
			return data["data"]["url"]
	except Exception as e:
		raise YandexMetrikaError(f"Ошибка загрузки на внешний сервер: {e}")
	finally:
		# Всегда удаляем временный файл
		try:
			os.remove(tmp_path)
		except OSError:
			pass  # Игнорируем ошибки удаления
