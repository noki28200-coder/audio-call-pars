"""
Главный REST API сервис системы записей звонков.

Эндпоинты:
  POST /calls/upload                  — ручная загрузка MP3
  GET  /calls                         — список звонков (с фильтрами)
  GET  /calls/{call_id}               — полная информация о звонке
  GET  /calls/{call_id}/audio         — стриминг аудиофайла
  GET  /calls/{call_id}/transcript    — транскрипция
  GET  /calls/{call_id}/analysis      — результаты анализа
  GET  /calls/{call_id}/status        — только статус (легковесный)
  GET  /admin/stats                   — статистика для дашборда
  GET  /admin/logs                    — журнал операций и ошибок
  GET  /health                        — health check (без авторизации)

Авторизация: заголовок X-API-Key: <ключ из .env API_KEY>
"""

import json
import logging
import mimetypes
import os
import re
import secrets
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import boto3
import psycopg2
import psycopg2.extras
from botocore.client import Config
from botocore.exceptions import ClientError
from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("api")

# ── Конфигурация ──────────────────────────────────────────────
POSTGRES_DSN = os.getenv("POSTGRES_DSN")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
BUCKET = os.getenv("S3_BUCKET", "call-audios")
API_KEY = os.getenv("API_KEY", "")

_s3 = None
_producer = None


# ── Инициализация клиентов ────────────────────────────────────

def _init_s3():
    client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    try:
        client.create_bucket(Bucket=BUCKET)
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("BucketAlreadyExists", "BucketAlreadyOwnedByYou"):
            raise
    return client


def _init_kafka(retries: int = 15, delay: int = 3) -> KafkaProducer:
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all",
                retries=3,
            )
            logger.info("Kafka подключена: %s", KAFKA_BOOTSTRAP)
            return producer
        except KafkaError as e:
            logger.warning("Kafka попытка %d/%d: %s", attempt + 1, retries, e)
            if attempt < retries - 1:
                time.sleep(delay)
    raise RuntimeError("Не удалось подключиться к Kafka")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _s3, _producer
    _s3 = _init_s3()
    _producer = _init_kafka()
    yield
    if _producer:
        _producer.flush()
        _producer.close()


app = FastAPI(
    title="Call Records API",
    description="API для доступа к записям звонков, транскрипциям и аналитике.",
    version="1.0.0",
    lifespan=lifespan,
)


# ── DB helper ────────────────────────────────────────────────

def _db():
    conn = psycopg2.connect(POSTGRES_DSN, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = False
    try:
        yield conn
    finally:
        conn.close()


def _log_db(conn, call_id: str, level: str, stage: str, message: str, details: dict = None):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO call_logs (call_id, level, stage, message, details) VALUES (%s,%s,%s,%s,%s)",
                (call_id, level, stage, message, psycopg2.extras.Json(details) if details else None),
            )
        conn.commit()
    except Exception as exc:
        logger.error("Не удалось записать лог: %s", exc)


# ── Авторизация ──────────────────────────────────────────────

def require_auth(
    x_api_key: str = Header(default=""),
    api_key: str = Query(default="", include_in_schema=False),
):
    """
    Проверка API-ключа. Если API_KEY не задан — аутентификация отключена (dev-режим).

    Ключ берётся из заголовка X-API-Key, а для ссылок, которые браузер открывает
    сам (<audio src>, скачивание файла), — из параметра ?api_key=.
    """
    provided = x_api_key or api_key
    if API_KEY and not secrets.compare_digest(provided.encode(), API_KEY.encode()):
        raise HTTPException(status_code=403, detail="Неверный API ключ")
    return provided


# ── Поиск звонка по ID ──────────────────────────────────────

def _find_call(conn, call_id: str) -> dict:
    """Найти звонок по call_id или mango_recording_id."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM calls WHERE call_id = %s OR mango_recording_id = %s LIMIT 1",
            (call_id, call_id),
        )
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Звонок '{call_id}' не найден")
    return dict(row)


# ════════════════════════════════════════════════════════════
# ЭНДПОИНТЫ
# ════════════════════════════════════════════════════════════

@app.get("/health", tags=["System"])
def health():
    """Health check — без авторизации."""
    return {"status": "ok", "service": "api"}


# ── Загрузка звонка вручную ──────────────────────────────────

@app.post("/calls/upload", tags=["Calls"], summary="Загрузить MP3 файл вручную")
def upload_call(
    file: UploadFile = File(...),
    employee_id: str = Form(default=""),
    employee_name: str = Form(default=""),
    phone_from: str = Form(default=""),
    phone_to: str = Form(default=""),
    direction: str = Form(default=""),
    conn=Depends(_db),
    _=Depends(require_auth),
):
    """
    Ручная загрузка аудиозаписи звонка.

    Параметры формы (опциональны):
    - **employee_id** — ID сотрудника
    - **employee_name** — Имя сотрудника
    - **phone_from** — Номер звонящего
    - **phone_to** — Номер принимающего
    - **direction** — Направление: inbound | outbound
    """
    AUDIO_EXTENSIONS = {".mp3", ".wav", ".m4a", ".ogg", ".mp4", ".aac", ".flac", ".webm"}
    fname = (file.filename or "").lower()
    ext = "." + fname.rsplit(".", 1)[-1] if "." in fname else ""
    is_audio_ct = file.content_type and ("audio" in file.content_type or "video" in file.content_type)
    if ext not in AUDIO_EXTENSIONS and not is_audio_ct:
        raise HTTPException(status_code=400, detail=f"Принимаются аудиофайлы: {', '.join(sorted(AUDIO_EXTENSIONS))}")

    contents = file.file.read()
    if not contents:
        raise HTTPException(status_code=400, detail="Файл пустой")

    call_id = str(uuid.uuid4())
    today = datetime.utcnow().strftime("%Y-%m-%d")
    safe_ext = ext if ext in AUDIO_EXTENSIONS else ".mp3"
    object_key = f"calls/{today}/{call_id}{safe_ext}"

    content_type = file.content_type or "audio/mpeg"
    # Сохраняем в S3
    _s3.put_object(Bucket=BUCKET, Key=object_key, Body=contents, ContentType=content_type)

    # Пишем в PostgreSQL
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO calls
               (call_id, source, employee_id, employee_name, phone_from, phone_to,
                direction, file_path, file_size_bytes, status)
               VALUES (%s,'manual',%s,%s,%s,%s,%s,%s,%s,'stored')""",
            (call_id, employee_id or None, employee_name or None,
             phone_from or None, phone_to or None, direction or None,
             object_key, len(contents)),
        )
    conn.commit()
    _log_db(conn, call_id, "info", "upload", "Файл загружен вручную", {"size": len(contents)})

    # Отправляем в Kafka. Данные звонка транскрибатор берёт из БД, в событии только call_id.
    # Если Kafka недоступна, звонок остаётся в 'stored' и транскрибатор подберёт его сам.
    try:
        _producer.send("calls-ready", value={"call_id": call_id}).get(timeout=10)
    except Exception as exc:
        logger.warning("[%s] Не удалось отправить событие в Kafka: %s", call_id, exc)

    logger.info("[%s] Загружен вручную (%d байт)", call_id, len(contents))
    return {"status": "uploaded", "call_id": call_id, "file_path": object_key}


# ── Список звонков ────────────────────────────────────────────

@app.get("/calls", tags=["Calls"], summary="Список звонков с фильтрами")
def list_calls(
    status: Optional[str] = Query(None, description="Фильтр по статусу: pending|stored|transcribing|transcribed|analyzing|analyzed|failed_*"),
    source: Optional[str] = Query(None, description="Источник: mango|manual"),
    employee_id: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None, description="От даты (ISO 8601): 2024-01-01"),
    date_to: Optional[str] = Query(None, description="До даты (ISO 8601): 2024-12-31"),
    q: Optional[str] = Query(None, description="Полнотекстовый поиск: транскрипт, сотрудник, телефон"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    conn=Depends(_db),
    _=Depends(require_auth),
):
    """Постраничный список звонков с опциональными фильтрами."""
    conditions = []
    params = []

    if status:
        conditions.append("status = %s")
        params.append(status)
    if source:
        conditions.append("source = %s")
        params.append(source)
    if employee_id:
        conditions.append("employee_id = %s")
        params.append(employee_id)
    if date_from:
        conditions.append("created_at >= %s")
        params.append(date_from)
    if date_to:
        # Дата без времени (2024-12-31) включает весь день
        if len(date_to) == 10:
            conditions.append("created_at < %s::date + 1")
        else:
            conditions.append("created_at <= %s")
        params.append(date_to)
    if q:
        pq = f"%{q}%"
        conditions.append(
            "(transcript_text ILIKE %s OR analysis_summary ILIKE %s"
            " OR employee_name ILIKE %s OR phone_from LIKE %s OR phone_to LIKE %s)"
        )
        params.extend([pq, pq, pq, pq, pq])

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS cnt FROM calls {where}", params)
        total = cur.fetchone()["cnt"]

        extra_col = ", transcript_text, analysis_summary" if q else ""
        cur.execute(
            f"""SELECT call_id, mango_recording_id, source, employee_id, employee_name,
                       phone_from, phone_to, direction, duration_seconds, start_time,
                       file_size_bytes, status, transcribed_at, analyzed_at,
                       analysis_sentiment, analysis_score, analysis_call_outcome,
                       created_at{extra_col}
                FROM calls {where}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s""",
            params + [limit, offset],
        )
        rows = [dict(r) for r in cur.fetchall()]

    if q:
        ql = q.lower()
        for row in rows:
            snippet = None
            for field in ("transcript_text", "analysis_summary"):
                text = row.pop(field, None) or ""
                if snippet is None and ql in text.lower():
                    pos = text.lower().find(ql)
                    start = max(0, pos - 60)
                    end = min(len(text), pos + len(q) + 60)
                    snippet = ("…" if start > 0 else "") + text[start:end] + ("…" if end < len(text) else "")
            row["transcript_snippet"] = snippet

    return {"total": total, "limit": limit, "offset": offset, "calls": rows}


# ── Полная информация о звонке ────────────────────────────────

@app.get("/calls/{call_id}", tags=["Calls"], summary="Полная информация о звонке")
def get_call(call_id: str, conn=Depends(_db), _=Depends(require_auth)):
    """
    Возвращает все данные о звонке включая метаданные, статус, транскрипцию и анализ.

    Параметр `call_id` принимает как внутренний ID, так и ID записи МангоОфис.
    """
    return _find_call(conn, call_id)


# ── Статус звонка (лёгкий) ───────────────────────────────────

@app.get("/calls/{call_id}/status", tags=["Calls"], summary="Статус обработки звонка")
def get_status(call_id: str, conn=Depends(_db), _=Depends(require_auth)):
    """Только статус и временные метки — без тяжёлых данных транскрипции."""
    call = _find_call(conn, call_id)
    return {
        "call_id": call["call_id"],
        "mango_recording_id": call.get("mango_recording_id"),
        "status": call["status"],
        "created_at": call["created_at"],
        "transcribed_at": call.get("transcribed_at"),
        "analyzed_at": call.get("analyzed_at"),
    }


# ── Аудиофайл ─────────────────────────────────────────────────

_RANGE_RE = re.compile(r"^bytes=(\d*)-(\d*)$")


def _parse_range(header: str, size: int) -> Optional[tuple]:
    """Разобрать один диапазон 'bytes=start-end'. None — заголовок не поддерживается, отдаём файл целиком."""
    m = _RANGE_RE.match(header.strip())
    if not m or (not m.group(1) and not m.group(2)):
        return None
    if m.group(1):
        start = int(m.group(1))
        end = min(int(m.group(2)), size - 1) if m.group(2) else size - 1
    else:  # bytes=-500 — последние 500 байт
        start = max(0, size - int(m.group(2)))
        end = size - 1
    if start > end or start >= size:
        raise HTTPException(status_code=416, detail="Неверный диапазон", headers={"Content-Range": f"bytes */{size}"})
    return start, end


@app.get("/calls/{call_id}/audio", tags=["Calls"], summary="Скачать аудиозапись")
def get_audio(
    call_id: str,
    range_header: Optional[str] = Header(default=None, alias="Range"),
    conn=Depends(_db),
    _=Depends(require_auth),
):
    """
    Стриминг аудиофайла звонка напрямую из S3.

    Поддерживает Range-запросы (206 Partial Content) — нужны плееру для перемотки.
    """
    call = _find_call(conn, call_id)
    if not call.get("file_path"):
        raise HTTPException(status_code=404, detail="Аудиофайл ещё не загружен")
    key = call["file_path"]

    try:
        size = _s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
        byte_range = _parse_range(range_header, size) if range_header and size else None
        if byte_range:
            obj = _s3.get_object(Bucket=BUCKET, Key=key, Range=f"bytes={byte_range[0]}-{byte_range[1]}")
        else:
            obj = _s3.get_object(Bucket=BUCKET, Key=key)
    except ClientError:
        raise HTTPException(status_code=404, detail="Файл не найден в хранилище")

    def _stream():
        while True:
            chunk = obj["Body"].read(65536)
            if not chunk:
                break
            yield chunk

    ext = os.path.splitext(key)[1] or ".mp3"
    media_type = obj.get("ContentType") or mimetypes.guess_type(key)[0] or "audio/mpeg"
    headers = {
        "Content-Disposition": f'attachment; filename="{call_id}{ext}"',
        "Content-Length": str(obj["ContentLength"]),
        "Accept-Ranges": "bytes",
    }
    if byte_range:
        headers["Content-Range"] = f"bytes {byte_range[0]}-{byte_range[1]}/{size}"
        return StreamingResponse(_stream(), status_code=206, media_type=media_type, headers=headers)
    return StreamingResponse(_stream(), media_type=media_type, headers=headers)


# ── Транскрипция ──────────────────────────────────────────────

@app.get("/calls/{call_id}/transcript", tags=["Calls"], summary="Транскрипция звонка")
def get_transcript(call_id: str, conn=Depends(_db), _=Depends(require_auth)):
    """Текст транскрипции и сегменты с временными метками."""
    call = _find_call(conn, call_id)
    if not call.get("transcript_text"):
        status = call["status"]
        raise HTTPException(
            status_code=404,
            detail=f"Транскрипция недоступна. Текущий статус: {status}",
        )
    return {
        "call_id": call["call_id"],
        "language": call.get("transcript_language"),
        "language_probability": call.get("transcript_language_probability"),
        "full_text": call["transcript_text"],
        "segments": call.get("transcript_segments") or [],
        "transcribed_at": call.get("transcribed_at"),
    }


# ── Анализ ────────────────────────────────────────────────────

@app.get("/calls/{call_id}/analysis", tags=["Calls"], summary="Результаты AI-анализа")
def get_analysis(call_id: str, conn=Depends(_db), _=Depends(require_auth)):
    """
    Результаты анализа звонка от Ollama/Gemma:
    резюме, тональность, соответствие скрипту, итог, оценка.
    """
    call = _find_call(conn, call_id)
    if not call.get("analysis_summary"):
        status = call["status"]
        raise HTTPException(
            status_code=404,
            detail=f"Анализ недоступен. Текущий статус: {status}",
        )
    return {
        "call_id": call["call_id"],
        "summary": call.get("analysis_summary"),
        "sentiment": call.get("analysis_sentiment"),
        "script_compliance": call.get("analysis_script_compliance"),
        "script_violations": call.get("analysis_script_violations") or [],
        "action_items": call.get("analysis_action_items") or [],
        "score": call.get("analysis_score"),
        "topics": call.get("analysis_topics") or [],
        "call_outcome": call.get("analysis_call_outcome"),
        "analyzed_at": call.get("analyzed_at"),
        "metadata": {
            "employee_id": call.get("employee_id"),
            "employee_name": call.get("employee_name"),
            "duration_seconds": call.get("duration_seconds"),
            "direction": call.get("direction"),
        },
    }


# ── Административная статистика ──────────────────────────────

@app.get("/admin/stats", tags=["Admin"], summary="Статистика для дашборда")
def get_stats(conn=Depends(_db), _=Depends(require_auth)):
    """
    Агрегированная статистика по всем звонкам и операциям.
    Используется фронтендом для отображения дашборда.
    """
    with conn.cursor() as cur:
        # Всего звонков
        cur.execute("SELECT COUNT(*) AS cnt FROM calls")
        total = cur.fetchone()["cnt"]

        # По статусам
        cur.execute("SELECT status, COUNT(*) AS cnt FROM calls GROUP BY status")
        by_status = {r["status"]: r["cnt"] for r in cur.fetchall()}

        # За последние 24 часа
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM calls WHERE created_at >= NOW() - INTERVAL '24 hours'"
        )
        last_24h_downloaded = cur.fetchone()["cnt"]

        cur.execute(
            "SELECT COUNT(*) AS cnt FROM calls WHERE transcribed_at >= NOW() - INTERVAL '24 hours'"
        )
        last_24h_transcribed = cur.fetchone()["cnt"]

        cur.execute(
            "SELECT COUNT(*) AS cnt FROM calls WHERE analyzed_at >= NOW() - INTERVAL '24 hours'"
        )
        last_24h_analyzed = cur.fetchone()["cnt"]

        # Ошибки
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM call_logs WHERE level='error' AND created_at >= NOW() - INTERVAL '24 hours'"
        )
        errors_24h = cur.fetchone()["cnt"]

        cur.execute(
            "SELECT COUNT(*) AS cnt FROM call_logs WHERE level='error' AND created_at >= NOW() - INTERVAL '7 days'"
        )
        errors_7d = cur.fetchone()["cnt"]

        # Хранилище
        cur.execute("SELECT COALESCE(SUM(file_size_bytes), 0) AS total FROM calls WHERE file_size_bytes IS NOT NULL")
        total_bytes = cur.fetchone()["total"]

        # Последняя активность
        cur.execute("SELECT MAX(created_at) AS t FROM calls")
        last_download = cur.fetchone()["t"]

        cur.execute("SELECT MAX(transcribed_at) AS t FROM calls WHERE transcribed_at IS NOT NULL")
        last_transcription = cur.fetchone()["t"]

        cur.execute("SELECT MAX(analyzed_at) AS t FROM calls WHERE analyzed_at IS NOT NULL")
        last_analysis = cur.fetchone()["t"]

        cur.execute(
            "SELECT MAX(created_at) AS t FROM call_logs WHERE level = 'error'"
        )
        last_error = cur.fetchone()["t"]

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "calls": {
            "total": total,
            "by_status": by_status,
            "last_24h": {
                "downloaded": last_24h_downloaded,
                "transcribed": last_24h_transcribed,
                "analyzed": last_24h_analyzed,
            },
        },
        "errors": {
            "last_24h": errors_24h,
            "last_7d": errors_7d,
        },
        "storage": {
            "total_size_bytes": int(total_bytes),
            "total_size_mb": round(int(total_bytes) / 1024 / 1024, 2),
        },
        "last_activity": {
            "last_download": last_download,
            "last_transcription": last_transcription,
            "last_analysis": last_analysis,
            "last_error": last_error,
        },
    }


# ── Журнал операций ──────────────────────────────────────────

@app.get("/admin/logs", tags=["Admin"], summary="Журнал операций и ошибок")
def get_logs(
    level: Optional[str] = Query(None, description="Уровень: info|warning|error"),
    stage: Optional[str] = Query(None, description="Этап: download|upload|transcription|analysis"),
    call_id: Optional[str] = Query(None, description="Фильтр по конкретному звонку"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    conn=Depends(_db),
    _=Depends(require_auth),
):
    """
    Журнал всех операций — загрузки, транскрипции, анализа — и ошибок.
    Используется для диагностики и мониторинга системы.
    """
    conditions, params = [], []
    if level:
        conditions.append("level = %s")
        params.append(level)
    if stage:
        conditions.append("stage = %s")
        params.append(stage)
    if call_id:
        conditions.append("call_id = %s")
        params.append(call_id)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS cnt FROM call_logs {where}", params)
        total = cur.fetchone()["cnt"]

        cur.execute(
            f"SELECT * FROM call_logs {where} ORDER BY created_at DESC LIMIT %s OFFSET %s",
            params + [limit, offset],
        )
        rows = [dict(r) for r in cur.fetchall()]

    return {"total": total, "limit": limit, "offset": offset, "logs": rows}
