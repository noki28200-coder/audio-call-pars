# Call Parser — автоматический анализ звонков

[English](README.md) | [Deutsch](README.de.md) | **Русский**

Система автоматически обрабатывает записи телефонных звонков:

1. **Сбор** — новые записи из МангоОфис или ручная загрузка
2. **Хранение** — аудиофайлы в S3-хранилище (SeaweedFS)
3. **Расшифровка** — речь в текст через Whisper
4. **Анализ** — локальная языковая модель (Ollama, `qwen3.5:9b`): резюме, оценка 1–10, тон, нарушения правил, следующие шаги
5. **Выдача** — результаты через REST API, админку и (по желанию) вебхук в CRM

Все данные остаются на вашем сервере — внешние ИИ-сервисы не используются.

## Архитектура

```
МангоОфис ──► collector ─┐
                         ├─► S3 + PostgreSQL ──► Kafka ──► transcriber ──► Whisper
Загрузка (API) ──────────┘                                     │
                                                               ▼
                                  Вебхук CRM ◄── analyzer ──► Ollama (qwen3.5:9b)

Браузер / CRM ──► nginx :8080 ──► api (Go) ──► PostgreSQL, S3
```

- Все сервисы, кроме Whisper, написаны на **Go** (`pipeline/`).
- События Kafka содержат только `call_id`, данные лежат в PostgreSQL — потерянное или повторное событие ничего не ломает.
- Упавшие шаги повторяются автоматически (по умолчанию 3 попытки с паузой 5 минут). Зависшие звонки находятся и возвращаются в очередь.

## Быстрый старт

**Нужно:** Docker с Compose v2, 16 ГБ RAM, ~20 ГБ на диске, [Ollama](https://ollama.com) на машине.

```bash
git clone https://github.com/noki28200-coder/audio-call-pars.git
cd audio-call-pars

ollama pull qwen3.5:9b        # ИИ-модель, ~6,6 ГБ
cp .env.example .env          # вписать пароли и ключи
docker compose up -d          # первый запуск: 10–40 мин (образы + модель Whisper)
docker compose ps             # все сервисы должны быть "healthy"
```

| Адрес | Что там |
|---|---|
| http://localhost:8080/admin.html | Админка (API-ключ вводится вверху страницы) |
| http://localhost:8080/analytics.html | Аналитика |
| http://localhost:8080/docs | Интерактивная документация API (Swagger) |
| http://localhost:8888/buckets/call-audios/ | Просмотр файлов в S3 |

Без Ollama на машине: `docker compose --profile docker-ollama up -d` и `OLLAMA_HOST=http://ollama:11434` в `.env` (работает только на CPU).

## Настройки (`.env`)

| Переменная | Назначение | По умолчанию |
|---|---|---|
| `API_KEY` | Ключ для API и админки; пусто = без авторизации (только для разработки) | `dev-insecure-key` |
| `POSTGRES_PASSWORD` | Пароль базы данных | – |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY` | Доступ к S3-хранилищу | – |
| `MANGO_API_KEY`, `MANGO_API_SALT` | Доступ к МангоОфис (только для автоимпорта) | – |
| `POLL_INTERVAL_SECONDS` | Как часто опрашивать МангоОфис | `300` |
| `WHISPER_MODEL` | `tiny` … `large-v3`; больше — точнее, но медленнее | `medium` |
| `WHISPER_LANGUAGE` | Язык звонков | `ru` |
| `OLLAMA_HOST` | Адрес Ollama | `http://host.docker.internal:11434` |
| `OLLAMA_MODEL` | ИИ-модель; на слабой машине — `qwen3.5:4b` | `qwen3.5:9b` |
| `MAX_RETRIES` | Повторов на каждый шаг обработки | `3` |
| `CRM_WEBHOOK_URL` | Дополнительно отправлять результаты анализа на этот адрес | пусто |

Сгенерировать надёжный ключ: `python3 -c "import secrets; print(secrets.token_urlsafe(32))"`

## API коротко

Авторизация — заголовок `X-API-Key` (для ссылок на аудио — `?api_key=`). Полный справочник — `/docs`, заметки для фронтенда — [FRONTEND_INTEGRATION.md](FRONTEND_INTEGRATION.md).

| Метод | Путь | Зачем |
|---|---|---|
| `POST` | `/calls/upload` | Загрузить аудиофайл (до 200 МБ) |
| `GET` | `/calls` | Список с фильтрами и поиском по тексту |
| `GET` | `/calls/{id}` | Все данные звонка |
| `GET` | `/calls/{id}/status` | Только статус (для опроса) |
| `GET` | `/calls/{id}/audio` | Прослушать запись (с перемоткой) |
| `GET` | `/calls/{id}/transcript` | Расшифровка с таймкодами |
| `GET` | `/calls/{id}/analysis` | ИИ-анализ |
| `GET` | `/admin/stats`, `/admin/logs` | Статистика и журнал |

```bash
curl -H "X-API-Key: $API_KEY" -F file=@call.mp3 http://localhost:8080/calls/upload
```

**Статусы звонка:** `pending` → `stored` → `transcribing` → `transcribed` → `analyzing` → `analyzed` (при ошибках `failed_download` / `failed_transcription` / `failed_analysis`).

## Эксплуатация

```bash
docker compose logs -f analyzer                 # логи одного сервиса
docker compose up -d --build api                # пересобрать после изменений кода
docker compose exec postgres psql -U callsuser -d callsdb   # консоль базы

# Бэкап
docker compose exec postgres pg_dump -U callsuser callsdb > backup.sql
docker run --rm -v audio-call-parser-go_s3_data:/data -v "$PWD":/b alpine tar czf /b/s3.tar.gz /data
```

**Частые проблемы**

- *Расшифровка медленная или не хватает памяти* → модель Whisper поменьше (`WHISPER_MODEL=small`) или дать Docker больше CPU/RAM.
- *Анализ падает* → проверить `ollama list`, скачать модель: `ollama pull qwen3.5:9b`.
- *403 Forbidden* → `X-API-Key` не совпадает с `API_KEY` в `.env`.

## Структура проекта

```
db/init.sql            схема БД (применяется при каждом запуске, идемпотентно)
docker-compose.yml     все сервисы
frontend/              статические страницы (админка, аналитика)
nginx/                 обратный прокси
whisperx-service/      распознавание речи (Python, faster-whisper)
examples/              пример методики разговора для ИИ-оценки
pipeline/              Go-модуль
  cmd/api              REST API
  cmd/collector        импорт из МангоОфис
  cmd/transcriber      расшифровка
  cmd/analyzer         ИИ-анализ
  internal/            общие пакеты (настройки, S3, Kafka, БД, воркер)
```

## Разработка

```bash
cd pipeline && go test ./...

# Интеграционные тесты на временной БД (она будет очищена!)
docker run --rm -d --name pg-test -p 55432:5432 -e POSTGRES_PASSWORD=test postgres:16-alpine
TEST_POSTGRES_DSN=postgres://postgres:test@localhost:55432/postgres go test -p 1 ./...
docker stop pg-test
```

## Лицензия

Все права защищены — см. [LICENSE](LICENSE).
