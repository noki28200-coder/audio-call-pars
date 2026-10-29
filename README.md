# Call Parser – automatic call analysis

**English** | [Deutsch](README.de.md) | [Русский](README.ru.md)

The system processes phone call recordings automatically:

1. **Collect** – new recordings from MangoOffice or via upload
2. **Store** – audio files in S3 storage (SeaweedFS)
3. **Transcribe** – speech to text with Whisper
4. **Analyze** – local language model (Ollama, `qwen3.5:9b`): summary, score 1–10, sentiment, rule violations, next steps
5. **Deliver** – results via REST API, admin panel and an optional CRM webhook

All data stays on your own server – no external AI services are used.

## Architecture

```
MangoOffice ──► collector ─┐
                           ├─► S3 + PostgreSQL ──► Kafka ──► transcriber ──► Whisper
Upload (API) ──────────────┘                                     │
                                                                 ▼
                                    CRM webhook ◄── analyzer ──► Ollama (qwen3.5:9b)

Browser / CRM ──► nginx :8080 ──► api (Go) ──► PostgreSQL, S3
```

- All services except Whisper are written in **Go** (`pipeline/`).
- Kafka events carry only the `call_id`; the data lives in PostgreSQL, so a lost or duplicated event does no harm.
- Failed steps are retried automatically (default: 3 attempts, 5-minute pause). Stuck calls are detected and re-queued.

## Quick start

**Requirements:** Docker with Compose v2, 16 GB RAM, ~20 GB free disk, [Ollama](https://ollama.com) on the host.

```bash
git clone https://github.com/noki28200-coder/audio-call-pars.git
cd audio-call-pars

ollama pull qwen3.5:9b        # AI model, ~6.6 GB
cp .env.example .env          # set passwords and keys
docker compose up -d          # first start: 10–40 min (images + Whisper model)
docker compose ps             # all services should be "healthy"
```

| URL | What |
|---|---|
| http://localhost:8080/admin.html | Admin panel (enter the API key at the top) |
| http://localhost:8080/analytics.html | Analytics |
| http://localhost:8080/docs | Interactive API docs (Swagger) |
| http://localhost:8888/buckets/call-audios/ | S3 file browser |

Without Ollama on the host: `docker compose --profile docker-ollama up -d` and set `OLLAMA_HOST=http://ollama:11434` in `.env` (CPU only).

## Configuration (`.env`)

| Variable | Meaning | Default |
|---|---|---|
| `API_KEY` | Key for the API and admin panel; empty = no auth (development only) | `dev-insecure-key` |
| `POSTGRES_PASSWORD` | Database password | – |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY` | S3 storage credentials | – |
| `MANGO_API_KEY`, `MANGO_API_SALT` | MangoOffice access (only for automatic import) | – |
| `POLL_INTERVAL_SECONDS` | MangoOffice polling interval | `300` |
| `WHISPER_MODEL` | `tiny` … `large-v3`; larger = more accurate but slower | `medium` |
| `WHISPER_LANGUAGE` | Language of the calls | `ru` |
| `OLLAMA_HOST` | Ollama address | `http://host.docker.internal:11434` |
| `OLLAMA_MODEL` | AI model; weaker machines: `qwen3.5:4b` | `qwen3.5:9b` |
| `MAX_RETRIES` | Retries per processing step | `3` |
| `CRM_WEBHOOK_URL` | Also send analysis results to this URL | empty |

Generate a secure key: `python3 -c "import secrets; print(secrets.token_urlsafe(32))"`

## API at a glance

Authentication via the `X-API-Key` header (or `?api_key=` for audio links). Full reference at `/docs`, frontend notes in [FRONTEND_INTEGRATION.md](FRONTEND_INTEGRATION.md) (Russian).

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/calls/upload` | Upload an audio file (up to 200 MB) |
| `GET` | `/calls` | List with filters and full-text search |
| `GET` | `/calls/{id}` | All data of a call |
| `GET` | `/calls/{id}/status` | Status only (for polling) |
| `GET` | `/calls/{id}/audio` | Play the recording (seeking supported) |
| `GET` | `/calls/{id}/transcript` | Transcript with timestamps |
| `GET` | `/calls/{id}/analysis` | AI analysis |
| `GET` | `/admin/stats`, `/admin/logs` | Statistics and log |

```bash
curl -H "X-API-Key: $API_KEY" -F file=@call.mp3 http://localhost:8080/calls/upload
```

**Call status:** `pending` → `stored` → `transcribing` → `transcribed` → `analyzing` → `analyzed` (on errors `failed_download` / `failed_transcription` / `failed_analysis`).

## Operations

```bash
docker compose logs -f analyzer                 # logs of one service
docker compose up -d --build api                # rebuild after code changes
docker compose exec postgres psql -U callsuser -d callsdb   # database shell

# Backup
docker compose exec postgres pg_dump -U callsuser callsdb > backup.sql
docker run --rm -v audio-call-parser-go_s3_data:/data -v "$PWD":/b alpine tar czf /b/s3.tar.gz /data
```

**Common issues**

- *Transcription too slow or out of memory* → smaller `WHISPER_MODEL` (e.g. `small`) or give Docker more CPU/RAM.
- *Analysis fails* → check `ollama list`, pull the model with `ollama pull qwen3.5:9b`.
- *403 Forbidden* → `X-API-Key` does not match `API_KEY` in `.env`.

## Project structure

```
db/init.sql            database schema (applied idempotently on every start)
docker-compose.yml     all services
frontend/              static pages (admin, analytics)
nginx/                 reverse proxy
whisperx-service/      speech recognition (Python, faster-whisper)
examples/              sample conversation methodology for AI scoring
pipeline/              Go module
  cmd/api              REST API
  cmd/collector        MangoOffice import
  cmd/transcriber      transcription
  cmd/analyzer         AI analysis
  internal/            shared packages (config, S3, Kafka, database, worker)
```

## Development

```bash
cd pipeline && go test ./...

# Integration tests with a temporary database (it will be wiped!)
docker run --rm -d --name pg-test -p 55432:5432 -e POSTGRES_PASSWORD=test postgres:16-alpine
TEST_POSTGRES_DSN=postgres://postgres:test@localhost:55432/postgres go test -p 1 ./...
docker stop pg-test
```

## License

All rights reserved – see [LICENSE](LICENSE).
