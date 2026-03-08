# IFRS PDF Parser (Google AI API)

This project extracts a defined set of IFRS financial metrics from a PDF report using Google Gemini API.

## What it does

- Input: IFRS PDF document.
- Output: Structured JSON with requested financial metrics.
- Engine: Google AI API (`google-genai` SDK).
- Metric set: default list in code, or custom list from JSON config.
- All numeric output values are converted to `RUB bn` (billions of rubles).
- Only latest reporting period is returned; prior/comparative period values are excluded.
- Additional calculated metrics are added to output based on parsed values.

Default metric set:

- Выручка (`revenue`)
- Финансовые расходы (`interest_expense_loans`), приоритет:
  1) процентные расходы по кредитам банков
  2) если нет, процентные расходы
  3) если нет, финансовые расходы
- Амортизация (`depreciation`)
- Денежные средства и эквиваленты (`cash_and_cash_equivalents`)
- Основные средства (`property_plant_and_equipment`)
- Операционная прибыль (`operating_profit`)
- Долгосрочные обязательства: кредиты + лизинг (`long_term_debt_and_lease`)
- Краткосрочные обязательства: кредиты + лизинг (`short_term_debt_and_lease`)

Calculated metrics in output:

- EBITDA = `operating_profit + depreciation`
- EBITDA margin, % = `EBITDA / revenue * 100`
- Total debt = `short_term_debt_and_lease + long_term_debt_and_lease`
- Net debt = `total_debt - cash_and_cash_equivalents`
- EBITDA / % expenses = `EBITDA / abs(interest_expense_loans)`
- Net debt / EBITDA LTM

## Requirements

- Python 3.10+
- One auth mode:
  - Google API key with Gemini Developer API access, or
  - Google service account JSON for Vertex AI

Environment variable:

```bash
export GOOGLE_API_KEY="your_api_key"
```

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Usage

```bash
ifrs-parser \
  --pdf /path/to/ifrs_report.pdf \
  --out output/ifrs_metrics.json
```

With service account JSON (Vertex AI mode):

```bash
ifrs-parser \
  --pdf /path/to/ifrs_report.pdf \
  --credentials-json ./ifrs-parser-489510-ed0c01e3a0ca.json \
  --project ifrs-parser-489510 \
  --location us-central1 \
  --out output/ifrs_metrics.json
```

With custom metrics config:

```bash
ifrs-parser \
  --pdf /path/to/ifrs_report.pdf \
  --metrics-config config/metrics.example.json \
  --out output/custom_metrics.json
```

Optional parameters:

- `--model` (default: `gemini-2.5-flash`)
- `--period-hint` (example: `FY2025`)
- `--timeout-sec` (default: `300`)
- `--keep-uploaded-file` (do not delete uploaded file from Google API)
- `--api-key` (if not using env var)
- `--credentials-json` (service account key path for Vertex mode)
- `--project` and `--location` (Vertex settings)

## HTTP API (async, with OpenAPI)

Run API server:

```bash
ifrs-api
```

or:

```bash
uvicorn ifrs_parser.api:app --host 0.0.0.0 --port 8000
```

API docs (OpenAPI/Swagger):

- `http://localhost:8000/docs`
- `http://localhost:8000/openapi.json`

Environment variables for auth/config:

- `GOOGLE_API_KEY` or `GEMINI_API_KEY` for Gemini API key mode.
- `IFRS_VERTEX_CREDENTIALS_JSON` + `IFRS_VERTEX_PROJECT` for Vertex mode.
- `GOOGLE_CLOUD_LOCATION` (optional, default `us-central1`).
- `IFRS_API_HOST` and `IFRS_API_PORT` for server bind settings.

Request example:

```bash
curl -X POST "http://localhost:8000/parse" \
  -F "file=@/path/to/ifrs_report.pdf" \
  -F "period_hint=Q2 2025" \
  -F "model=gemini-2.5-flash"
```

Response: same JSON structure as CLI output (`metrics`, `missing_metrics`, etc.).

## Telegram Bot (async)

Run bot:

```bash
export TELEGRAM_BOT_TOKEN="your_telegram_bot_token"
ifrs-telegram-bot
```

or:

```bash
ifrs-telegram-bot --token "your_telegram_bot_token"
```

or via token file `tg_token`:

```bash
echo 'TOKEN = your_telegram_bot_token' > tg_token
ifrs-telegram-bot
```

How it works:

- Send IFRS PDF as a document to the bot.
- Optional caption: `period_hint=Q2 2025`.
- Bot parses PDF and sends back CSV with extracted metrics.
- `/start` sends a welcome message with usage.
- `/help` asks user to leave feedback (`Ошибка`, `Изменение`, `Вопрос`) in the next text message.
- Feedback is forwarded to support chat (default id: `780684269`).

Bot uses the same Google auth env vars as CLI/API:

- `GOOGLE_API_KEY` or `GEMINI_API_KEY`, or
- `IFRS_VERTEX_CREDENTIALS_JSON` + `IFRS_VERTEX_PROJECT`
- Optional: `IFRS_FEEDBACK_CHAT_ID` to override default feedback chat id.

## Metrics config format

JSON array of metric definitions:

```json
[
  {
    "key": "revenue",
    "name": "Выручка",
    "description": "Revenue from IFRS statement of profit or loss."
  }
]
```

Rules:

- `key`: lowercase letters, digits, underscore only (for example `property_plant_and_equipment`).
- Keys must be unique.

## Output format

Output is a JSON object like:

```json
{
  "source_document": "report.pdf",
  "model": "gemini-2.5-flash",
  "company_name": "Example PLC",
  "reporting_period": "FY2025",
  "reporting_period_end_date": "2025-12-31",
  "reporting_currency": "RUB",
  "output_value_unit": "RUB bn",
  "notes": null,
  "metrics": [
    {
      "metric_key": "revenue",
      "metric_name": "Revenue",
      "found": true,
      "value": 123.456,
      "unit": "RUB bn",
      "scale_multiplier": 1.0,
      "period_label": "2025",
      "period_end_date": "2025-12-31",
      "selection_level": "bank_loan_interest",
      "statement": "Statement of profit or loss",
      "page": 45,
      "evidence": "Revenue 123,456",
      "confidence": 0.89,
      "notes": null
    }
  ],
  "missing_metrics": []
}
```

`missing_metrics` contains metric keys that were not found.

Business rule:

- If `depreciation` is not found, parser estimates it as `10%` of `property_plant_and_equipment`.

## Notes about PDF size

- API key mode uses Files API and supports larger PDFs.
- Vertex mode uses inline PDF payload in this implementation and has a size limit of `19 MB`.
