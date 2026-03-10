# Google Sheets Export Setup

## 1) Enable APIs in Google Cloud

In the same project as your service account:

1. Enable `Google Sheets API`.
2. Enable `Google Drive API`.

## 2) Prepare service account access

Use service account JSON key (the same style as your Vertex key file).

Find service account email in JSON (`client_email`) and share target Google Sheet with this email as `Editor`.

## 3) Create config file

Create `config/sheets_export.json` from example:

```bash
cp config/sheets_export.example.json config/sheets_export.json
```

Fill required fields:

- `credentials_json`: absolute path to service-account JSON.
- `spreadsheet_id`: keep empty to auto-create sheet on first init.
- `worksheet_name`: target tab name.
- `share_with`: emails that should get access to newly created sheet.
- `ubo_by_company` (optional): mapping `company_name -> фамилия UBO` for reliable заполнение столбца `UBO`.
- `ubo_unknown_value` (optional): fallback text if UBO is not found.

## 4) Initialize target sheet

```bash
ifrs-sheets-init --config config/sheets_export.json
```

The command creates sheet if needed, prepares worksheet headers in one-pager style, and writes `spreadsheet_id` back into config.

## 5) Enable auto-export after parsing

Any parser flow can append rows to Google Sheets:

- CLI: `ifrs-parser --pdf ... --sheets-config config/sheets_export.json`
- API: `POST /parse` with default `write_to_sheets=true`
- Telegram bot: set env `IFRS_SHEETS_CONFIG_PATH=config/sheets_export.json`

## Output format in sheet

Rows are written in the one-pager-like column format:

- `Отрасль`, `UBO`, `Компания`, `Type`, `Показатель`, `Сегмент`, `Сегмент`, `Источник`, `Ед.изм.`, `LTM`, `2025`, `2024`, `2023`, `2022`

Values are placed in:

- Year columns for annual (`12M/FY`) periods.
- `1Q/2Q/3Q/4Q` for quarter (`3M`) periods.
- `1H/2H` for half-year (`6M`) periods.
- `3Q` for `9M` periods.
- `LTM` for forced-LTM metrics (for example `Чистый долг/ EBITDA LTM`).

If parser returns both latest and comparative values, both are written into the same metric row:

- latest value goes to its own year/H/Q slot;
- comparative value goes to its own earlier slot (for balance metrics it can be prior year-end).

Column `UBO` is filled for every exported row of the company:

- source priority: parser field `ubo_surname` -> config mapping `ubo_by_company` -> `ubo_unknown_value`.
