from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any

from telegram import InputFile, Update
from telegram.constants import ChatAction
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from .metrics import load_metrics
from .parser import DEFAULT_LOCATION, DEFAULT_MODEL, GoogleIFRSPdfParser, IFRSParserConfig

logger = logging.getLogger(__name__)

_FILENAME_SAFE_RE = re.compile(r"[^A-Za-z0-9._-]+")
_TOKEN_LINE_RE = re.compile(r"^\s*TOKEN\s*=\s*(.+?)\s*$", re.IGNORECASE)
DEFAULT_FEEDBACK_CHAT_ID = 780684269
_AWAITING_FEEDBACK_KEY = "awaiting_feedback"
_CSV_COLUMNS = [
    "source_document",
    "company_name",
    "reporting_period",
    "reporting_period_end_date",
    "reporting_currency",
    "output_value_unit",
    "metric_key",
    "metric_name",
    "found",
    "value",
    "unit",
    "period_label",
    "period_end_date",
    "selection_level",
    "statement",
    "page",
    "confidence",
    "notes",
]


def _strip_wrapping_quotes(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1].strip()
    return text


def _load_token_from_file(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None

    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        return None

    for line in raw.splitlines():
        candidate = line.strip()
        if not candidate or candidate.startswith("#"):
            continue
        match = _TOKEN_LINE_RE.match(candidate)
        if match:
            token = _strip_wrapping_quotes(match.group(1))
            return token or None
        if "=" not in candidate:
            token = _strip_wrapping_quotes(candidate)
            return token or None

    return None


def _resolve_telegram_token(cli_token: str | None, token_file_path: str | None) -> str | None:
    if cli_token:
        return cli_token.strip() or None

    env_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if env_token and env_token.strip():
        return env_token.strip()

    default_file = token_file_path or os.getenv("TELEGRAM_BOT_TOKEN_FILE") or "tg_token"
    return _load_token_from_file(Path(default_file))


def _build_parser() -> GoogleIFRSPdfParser:
    config = IFRSParserConfig(
        model=os.getenv("IFRS_MODEL", DEFAULT_MODEL),
        location=os.getenv("GOOGLE_CLOUD_LOCATION", DEFAULT_LOCATION),
        timeout_sec=int(os.getenv("IFRS_TIMEOUT_SEC", "300")),
    )
    credentials_json = os.getenv("IFRS_VERTEX_CREDENTIALS_JSON")
    project = os.getenv("IFRS_VERTEX_PROJECT")
    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    return GoogleIFRSPdfParser(
        api_key=api_key,
        credentials_json=credentials_json,
        project=project,
        config=config,
    )


def _extract_period_hint(caption: str | None) -> str | None:
    if not caption:
        return None
    text = caption.strip()
    if not text:
        return None
    lowered = text.lower()
    for prefix in ("period_hint=", "period=", "period:", "hint:"):
        if lowered.startswith(prefix):
            value = text[len(prefix) :].strip()
            return value or None
    return text


def _safe_filename(filename: str | None) -> str:
    raw = (filename or "report.pdf").strip()
    if not raw:
        raw = "report.pdf"
    sanitized = _FILENAME_SAFE_RE.sub("_", raw).strip("._")
    if not sanitized:
        sanitized = "report.pdf"
    if not sanitized.lower().endswith(".pdf"):
        sanitized = f"{sanitized}.pdf"
    return sanitized


def _parse_pdf_sync(pdf_path: Path, period_hint: str | None) -> dict[str, Any]:
    parser = _build_parser()
    metrics = load_metrics()
    return parser.extract_metrics(
        pdf_path=pdf_path,
        metrics=metrics,
        period_hint=period_hint,
    )


def _result_to_csv_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    base = {
        "source_document": result.get("source_document"),
        "company_name": result.get("company_name"),
        "reporting_period": result.get("reporting_period"),
        "reporting_period_end_date": result.get("reporting_period_end_date"),
        "reporting_currency": result.get("reporting_currency"),
        "output_value_unit": result.get("output_value_unit"),
    }
    metrics = result.get("metrics")
    if not isinstance(metrics, list) or not metrics:
        return [
            {
                **base,
                "metric_key": None,
                "metric_name": None,
                "found": None,
                "value": None,
                "unit": None,
                "period_label": None,
                "period_end_date": None,
                "selection_level": None,
                "statement": None,
                "page": None,
                "confidence": None,
                "notes": result.get("notes"),
            }
        ]

    rows: list[dict[str, Any]] = []
    for metric in metrics:
        if not isinstance(metric, dict):
            continue
        rows.append(
            {
                **base,
                "metric_key": metric.get("metric_key"),
                "metric_name": metric.get("metric_name"),
                "found": metric.get("found"),
                "value": metric.get("value"),
                "unit": metric.get("unit"),
                "period_label": metric.get("period_label"),
                "period_end_date": metric.get("period_end_date"),
                "selection_level": metric.get("selection_level"),
                "statement": metric.get("statement"),
                "page": metric.get("page"),
                "confidence": metric.get("confidence"),
                "notes": metric.get("notes"),
            }
        )
    return rows


def _write_result_csv(result: dict[str, Any], csv_path: Path) -> None:
    rows = _result_to_csv_rows(result)
    with csv_path.open("w", encoding="utf-8-sig", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=_CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _build_done_caption(result: dict[str, Any]) -> str:
    company = result.get("company_name") or "Компания не определена"
    period = result.get("reporting_period_end_date") or result.get("reporting_period") or "Период не определен"
    return f"Готово: {company}, период {period}"


def _resolve_feedback_chat_id() -> int:
    raw_value = os.getenv("IFRS_FEEDBACK_CHAT_ID", str(DEFAULT_FEEDBACK_CHAT_ID))
    try:
        return int(raw_value)
    except ValueError as exc:
        raise ValueError(
            f"Invalid IFRS_FEEDBACK_CHAT_ID='{raw_value}'. It must be an integer chat id."
        ) from exc


def _classify_feedback_kind(text: str) -> str:
    normalized = text.strip().lower()
    if normalized.startswith(("ошибка", "баг", "error", "bug")):
        return "Ошибка"
    if normalized.startswith(("изменение", "предложение", "улучшение", "change", "feature")):
        return "Предложение изменения"
    if normalized.startswith(("вопрос", "question")):
        return "Вопрос"
    return "Обращение"


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None:
        return
    context.user_data[_AWAITING_FEEDBACK_KEY] = False
    await update.message.reply_text(
        "Привет! Я бот для парсинга МСФО PDF в CSV.\n"
        "Отправьте PDF-отчет документом, и я верну CSV с параметрами.\n"
        "Опционально в подписи укажите period_hint=Q2 2025.\n"
        "Если нужна помощь или хотите оставить обратную связь, используйте /help."
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None:
        return
    context.user_data[_AWAITING_FEEDBACK_KEY] = True
    await update.message.reply_text(
        "Как пользоваться:\n"
        "1) Отправьте PDF файлом.\n"
        "2) Можно добавить подпись: period_hint=Q2 2025.\n"
        "3) Я верну CSV с извлеченными метриками.\n\n"
        "Обратная связь:\n"
        "Отправьте следующим сообщением текст обращения в одном из форматов:\n"
        "- Ошибка: <описание>\n"
        "- Изменение: <предложение>\n"
        "- Вопрос: <ваш вопрос>"
    )


async def handle_pdf_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message is None or message.document is None:
        return

    document = message.document
    if not (document.file_name or "").lower().endswith(".pdf"):
        await message.reply_text("Нужен PDF-файл.")
        return

    temp_dir = Path(tempfile.mkdtemp(prefix="ifrs_tg_bot_"))
    display_filename = (document.file_name or "document.pdf").strip() or "document.pdf"
    status_message = await message.reply_text(
        f'Принял, анализирую "{display_filename}". Это может занять 1-3 минуты.'
    )
    try:
        pdf_path = temp_dir / _safe_filename(document.file_name)
        tg_file = await context.bot.get_file(document.file_id)
        await tg_file.download_to_drive(custom_path=str(pdf_path))

        period_hint = _extract_period_hint(message.caption)
        await context.bot.send_chat_action(chat_id=message.chat_id, action=ChatAction.TYPING)
        result = await asyncio.to_thread(_parse_pdf_sync, pdf_path, period_hint)

        csv_path = temp_dir / f"{pdf_path.stem}_metrics.csv"
        await asyncio.to_thread(_write_result_csv, result, csv_path)

        await context.bot.send_chat_action(chat_id=message.chat_id, action=ChatAction.UPLOAD_DOCUMENT)
        with csv_path.open("rb") as csv_file:
            await message.reply_document(
                document=InputFile(csv_file, filename=csv_path.name),
                caption=_build_done_caption(result),
            )
        await status_message.edit_text("Парсинг завершен.")
    except Exception as exc:
        logger.exception("Failed to process PDF from Telegram message")
        await status_message.edit_text(f"Ошибка парсинга: {exc}")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


async def handle_non_pdf_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if update.message is None:
        return
    await update.message.reply_text("Поддерживаются только PDF-документы.")


async def handle_feedback_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message is None:
        return

    is_awaiting = bool(context.user_data.get(_AWAITING_FEEDBACK_KEY))
    if not is_awaiting:
        return

    feedback_text = (message.text or "").strip()
    if not feedback_text:
        await message.reply_text("Пустое обращение. Отправьте текст с ошибкой, предложением или вопросом.")
        return

    kind = _classify_feedback_kind(feedback_text)
    user = message.from_user
    username = user.username if user and user.username else f"id{user.id if user else 'unknown'}"
    user_id = user.id if user else "unknown"
    feedback_header = f"ОБРАТНАЯ СВЯЗЬ ПО РАБОТЕ ОТ ПОЛЬЗОВАТЕЛЯ '@{username}'"
    feedback_payload = "\n".join(
        [
            feedback_header,
            f"Тип: {kind}",
            f"User ID: {user_id}",
            f"Chat ID: {message.chat_id}",
            "",
            feedback_text,
        ]
    )

    feedback_chat_id = int(context.bot_data.get("feedback_chat_id", DEFAULT_FEEDBACK_CHAT_ID))
    try:
        await context.bot.send_message(chat_id=feedback_chat_id, text=feedback_payload)
    except Exception as exc:
        logger.exception("Failed to deliver feedback message")
        await message.reply_text(f"Не удалось отправить обратную связь: {exc}")
        return

    context.user_data[_AWAITING_FEEDBACK_KEY] = False
    await message.reply_text("Спасибо! Обращение отправлено.")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ifrs-telegram-bot",
        description="Telegram bot that parses IFRS PDF and returns CSV metrics.",
    )
    parser.add_argument(
        "--token",
        help="Telegram bot token. If omitted, TELEGRAM_BOT_TOKEN env var or token file is used.",
    )
    parser.add_argument(
        "--token-file",
        default="tg_token",
        help="Path to token file (default: tg_token). Supports format: TOKEN = <token>.",
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    token = _resolve_telegram_token(args.token, args.token_file)
    if not token:
        raise ValueError(
            "Telegram token is missing. Set TELEGRAM_BOT_TOKEN, pass --token, "
            "or create token file (e.g. tg_token with TOKEN = <token>)."
        )

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        level=logging.INFO,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

    application = Application.builder().token(token).build()
    application.bot_data["feedback_chat_id"] = _resolve_feedback_chat_id()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(MessageHandler(filters.Document.PDF, handle_pdf_document))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_non_pdf_document))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_feedback_text))

    application.run_polling(drop_pending_updates=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
