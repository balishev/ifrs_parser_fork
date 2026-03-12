# IFRS Bank Debt Notes Parser

Проект для извлечения показателей по кредитам/займам из PDF отчетности МСФО.

## Что делает проект

- Принимает PDF отчетности МСФО.
- Находит в Примечаниях/Приложениях разделы про кредиты, займы и долговые обязательства.
- Извлекает строки по ключевым словам: `банк`, `займ`, `заем`, `облигаци`.
- Назначает приоритет показателя:
  - `1` если есть `займ`
  - `2` если есть `банк`
  - `3` если есть `заем`
  - `4` если есть `облигаци`
- Возвращает структурированный JSON и сводную Markdown-таблицу.
- Может записывать результат в отдельный лист Google Sheets.
- Может работать через Telegram-бота: PDF на вход, CSV на выход.

## Определение периода

- По умолчанию период определяется автоматически из отчетности (Q/H1/9M/FY).
- Можно задать явный фильтр: `rep_year=2024`.
- Если `rep_year` не указан, берется последний найденный отчетный период в документе.

## Установка

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Авторизация Google AI

Поддерживаются 2 режима:

- API key (`GOOGLE_API_KEY` или `GEMINI_API_KEY`)
- Vertex AI service account (`IFRS_VERTEX_CREDENTIALS_JSON` + `IFRS_VERTEX_PROJECT`)

Пример для Vertex:

```bash
export IFRS_VERTEX_CREDENTIALS_JSON=ifrs-parser-489510-ed0c01e3a0ca.json
export IFRS_VERTEX_PROJECT=ifrs-parser-489510
export GOOGLE_CLOUD_LOCATION=us-central1
```

## CLI: парсинг PDF долгов/кредитов

Базовый запуск:

```bash
ifrs-parser \
  --mode bank-debt-notes \
  --pdf /path/to/ifrs_report.pdf \
  --out output/bank_debt_notes.json
```

С указанием года:

```bash
ifrs-parser \
  --mode bank-debt-notes \
  --pdf /path/to/ifrs_report.pdf \
  --rep-year 2024 \
  --out output/bank_debt_notes_2024.json
```

Через Vertex:

```bash
ifrs-parser \
  --mode bank-debt-notes \
  --pdf /path/to/ifrs_report.pdf \
  --credentials-json ./ifrs-parser-489510-ed0c01e3a0ca.json \
  --project ifrs-parser-489510 \
  --location us-central1 \
  --out output/bank_debt_notes.json
```

OCR fallback (если есть заранее извлеченный текст):

```bash
ifrs-parser \
  --mode bank-debt-notes \
  --images-text-file /path/to/images_text.txt \
  --out output/bank_debt_notes.json
```

## Формат результата

В выходном JSON:

- `mode`: `bank_debt_notes`
- `rows`: массив строк
- `row_count`: количество строк
- `markdown_table`: итоговая таблица
- `detected_reporting_period`: найденный период
- `detected_reporting_period_end_date`: конец периода (ISO)
- `effective_rep_year`: фактически использованный год фильтра

Поля каждой строки `rows`:

- `company_name`
- `section_name`
- `indicator`
- `priority`
- `period`
- `period_end_date`
- `amount`
- `unit`

## Google Sheets (отдельный лист для долгов)

1. Скопируйте шаблон конфига:

```bash
cp config/sheets_export.example.json config/sheets_export.json
```

2. Заполните `config/sheets_export.json`:

- `credentials_json`
- `spreadsheet_id` (если уже есть таблица)
- `bank_debt_worksheet_name` (например: `Банк_долг_анализ`)

3. Инициализируйте таблицу:

```bash
ifrs-sheets-init --config config/sheets_export.json
```

4. Запуск с записью в Sheets:

```bash
ifrs-parser \
  --mode bank-debt-notes \
  --pdf /path/to/ifrs_report.pdf \
  --sheets-config config/sheets_export.json \
  --out output/bank_debt_notes.json
```

Подробно: `docs/google_sheets_setup.md`.

## Telegram-бот

Запуск:

```bash
cd "/Users/artm/Desktop/ВТБ/ifrs_parser" && \
PYTHONPATH=src \
IFRS_SHEETS_CONFIG_PATH=config/sheets_export.json \
IFRS_VERTEX_CREDENTIALS_JSON=ifrs-parser-489510-ed0c01e3a0ca.json \
IFRS_VERTEX_PROJECT=ifrs-parser-489510 \
python -m ifrs_parser.telegram_bot --token-file tg_token
```

Как использовать:

- Отправьте PDF документом.
- Бот отправит статус: `Принял, анализирую ...`.
- По завершении вернет CSV.
- Для явного года добавьте в подпись: `rep_year=2024`.
- Можно добавить `period_hint=Q2 2025`.

Команды:

- `/start` приветствие
- `/help` режим обратной связи

## CSV колонки бота (режим долгов)

- `Название компании`
- `Номер и название раздела (Примечания, Приложения)`
- `Показатель`
- `Приоритет`
- `Период`
- `Сумма`
- `Единица измерения`

## Устойчивость к ошибкам

- При `429 RESOURCE_EXHAUSTED` применяются автоматические повторы с backoff.
- При временных сетевых ошибках (`Server disconnected without sending a response`, timeout, 5xx) также выполняются автоповторы.
- Если после ретраев ошибка сохраняется, повторите отправку через 10-30 секунд.

## Ограничения

- В Vertex-режиме inline PDF ограничен размером около `19 MB`.
- Для больших PDF предпочтителен режим с API key (Files API).
