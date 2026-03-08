from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, UploadFile

from .metrics import load_metrics
from .parser import DEFAULT_LOCATION, DEFAULT_MODEL, GoogleIFRSPdfParser, IFRSParserConfig

app = FastAPI(
    title="IFRS PDF Parser API",
    version="0.1.0",
    description="Upload IFRS PDF and receive extracted financial metrics JSON.",
)


def _build_parser(model: str, timeout_sec: int) -> GoogleIFRSPdfParser:
    config = IFRSParserConfig(
        model=model or DEFAULT_MODEL,
        location=os.getenv("GOOGLE_CLOUD_LOCATION", DEFAULT_LOCATION),
        timeout_sec=timeout_sec,
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


def _save_temp_pdf(pdf_bytes: bytes) -> Path:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(pdf_bytes)
        return Path(tmp.name)


def _parse_pdf_sync(
    pdf_path: Path,
    period_hint: str | None,
    model: str,
    timeout_sec: int,
) -> dict[str, Any]:
    parser = _build_parser(model=model, timeout_sec=timeout_sec)
    metrics = load_metrics()
    return parser.extract_metrics(
        pdf_path=pdf_path,
        metrics=metrics,
        period_hint=period_hint,
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/parse")
async def parse_ifrs_pdf(
    file: UploadFile = File(...),
    period_hint: str | None = Form(default=None),
    model: str = Form(default=DEFAULT_MODEL),
    timeout_sec: int = Form(default=300),
) -> dict[str, Any]:
    filename = (file.filename or "").strip()
    if not filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only .pdf files are supported.")

    pdf_bytes = await file.read()
    if not pdf_bytes:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    temp_pdf_path = await asyncio.to_thread(_save_temp_pdf, pdf_bytes)
    try:
        result = await asyncio.to_thread(
            _parse_pdf_sync,
            temp_pdf_path,
            period_hint,
            model,
            timeout_sec,
        )
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Parsing failed: {exc}") from exc
    finally:
        try:
            temp_pdf_path.unlink(missing_ok=True)
        except Exception:
            pass


def main() -> int:
    import uvicorn

    host = os.getenv("IFRS_API_HOST", "0.0.0.0")
    port = int(os.getenv("IFRS_API_PORT", "8000"))
    uvicorn.run("ifrs_parser.api:app", host=host, port=port, reload=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
