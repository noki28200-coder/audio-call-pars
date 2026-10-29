# Call Parser – automatische Gesprächsanalyse

Das System verarbeitet Aufzeichnungen von Telefonaten automatisch:

1. **Abholen** – neue Aufnahmen aus MangoOffice oder per Upload
2. **Speichern** – Audiodateien im S3-Speicher (SeaweedFS)
3. **Transkribieren** – Sprache zu Text mit Whisper
4. **Analysieren** – lokales Sprachmodell (Ollama, `qwen3.5:9b`): Zusammenfassung, Bewertung 1–10, Stimmung, Regelverstöße, nächste Schritte
5. **Bereitstellen** – Ergebnisse über REST-API, Admin-Oberfläche und optionalen CRM-Webhook

Alle Daten bleiben auf dem eigenen Server – es werden keine externen KI-Dienste genutzt.

## Architektur

```
MangoOffice ──► collector ─┐
                           ├─► S3 + PostgreSQL ──► Kafka ──► transcriber ──► Whisper
Upload (API) ──────────────┘                                     │
                                                                 ▼
                                    CRM-Webhook ◄── analyzer ──► Ollama (qwen3.5:9b)

Browser / CRM ──► nginx :8080 ──► api (Go) ──► PostgreSQL, S3
```

- Alle Dienste außer Whisper sind in **Go** geschrieben (`pipeline/`).
- Kafka-Events enthalten nur die `call_id`; die Daten liegen in PostgreSQL. Ein verlorenes oder doppeltes Event schadet daher nicht.
- Fehlgeschlagene Schritte werden automatisch wiederholt (Standard: 3 Versuche, 5 Minuten Pause). Hängende Anrufe werden erkannt und neu eingereiht.

## Schnellstart

**Voraussetzungen:** Docker mit Compose v2, 16 GB RAM, ca. 20 GB freier Speicher, [Ollama](https://ollama.com) auf dem Host.

```bash
git clone https://github.com/noki28200-coder/audio-call-pars.git
cd audio-call-pars

ollama pull qwen3.5:9b        # KI-Modell, ca. 6,6 GB
cp .env.example .env          # Passwörter und Schlüssel eintragen
docker compose up -d          # erster Start: 10–40 Min. (Images + Whisper-Modell)
docker compose ps             # alle Dienste sollten "healthy" sein
```

| Adresse | Inhalt |
|---|---|
| http://localhost:8080/admin.html | Admin-Oberfläche (API-Schlüssel oben eingeben) |
| http://localhost:8080/analytics.html | Auswertungen |
| http://localhost:8080/docs | Interaktive API-Dokumentation (Swagger) |
| http://localhost:8888/buckets/call-audios/ | Dateiansicht des S3-Speichers |

Ohne Ollama auf dem Host: `docker compose --profile docker-ollama up -d` und in `.env` `OLLAMA_HOST=http://ollama:11434` setzen (läuft dann nur auf der CPU).

## Konfiguration (`.env`)

| Variable | Bedeutung | Standard |
|---|---|---|
| `API_KEY` | Schlüssel für API und Oberfläche; leer = keine Authentifizierung (nur Entwicklung) | `dev-insecure-key` |
| `POSTGRES_PASSWORD` | Datenbank-Passwort | – |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY` | Zugangsdaten für den S3-Speicher | – |
| `MANGO_API_KEY`, `MANGO_API_SALT` | Zugang zu MangoOffice (nur für automatischen Import) | – |
| `POLL_INTERVAL_SECONDS` | Abfrageintervall MangoOffice | `300` |
| `WHISPER_MODEL` | `tiny` … `large-v3`, größer = genauer, aber langsamer | `medium` |
| `WHISPER_LANGUAGE` | Sprache der Gespräche | `ru` |
| `OLLAMA_HOST` | Adresse von Ollama | `http://host.docker.internal:11434` |
| `OLLAMA_MODEL` | KI-Modell; schwächere Rechner: `qwen3.5:4b` | `qwen3.5:9b` |
| `MAX_RETRIES` | Wiederholungen pro Verarbeitungsschritt | `3` |
| `CRM_WEBHOOK_URL` | Analyseergebnisse zusätzlich an diese URL senden | leer |

Einen sicheren Schlüssel erzeugen: `python3 -c "import secrets; print(secrets.token_urlsafe(32))"`

## API in Kürze

Authentifizierung über den Header `X-API-Key` (für Audio-Links auch `?api_key=`). Vollständige Beschreibung unter `/docs`, Hinweise für das Frontend in [FRONTEND_INTEGRATION.md](FRONTEND_INTEGRATION.md).

| Methode | Pfad | Zweck |
|---|---|---|
| `POST` | `/calls/upload` | Audiodatei hochladen (bis 200 MB) |
| `GET` | `/calls` | Liste mit Filtern und Volltextsuche |
| `GET` | `/calls/{id}` | Alle Daten eines Anrufs |
| `GET` | `/calls/{id}/status` | Nur den Status (für Polling) |
| `GET` | `/calls/{id}/audio` | Aufnahme abspielen (mit Spulen) |
| `GET` | `/calls/{id}/transcript` | Transkript mit Zeitstempeln |
| `GET` | `/calls/{id}/analysis` | KI-Analyse |
| `GET` | `/admin/stats`, `/admin/logs` | Statistik und Protokoll |

```bash
curl -H "X-API-Key: $API_KEY" -F file=@anruf.mp3 http://localhost:8080/calls/upload
```

**Status eines Anrufs:** `pending` → `stored` → `transcribing` → `transcribed` → `analyzing` → `analyzed` (bei Fehlern `failed_download` / `failed_transcription` / `failed_analysis`).

## Betrieb

```bash
docker compose logs -f analyzer                 # Logs eines Dienstes
docker compose up -d --build api                # nach Codeänderungen neu bauen
docker compose exec postgres psql -U callsuser -d callsdb   # Datenbank

# Backup
docker compose exec postgres pg_dump -U callsuser callsdb > backup.sql
docker run --rm -v audio-call-parser-go_s3_data:/data -v "$PWD":/b alpine tar czf /b/s3.tar.gz /data
```

**Typische Probleme**

- *Transkription zu langsam oder Speicher voll* → kleineres `WHISPER_MODEL` (z. B. `small`) oder Docker mehr CPU/RAM geben.
- *Analyse schlägt fehl* → `ollama list` prüfen, Modell mit `ollama pull qwen3.5:9b` laden.
- *403 Forbidden* → `X-API-Key` stimmt nicht mit `API_KEY` in `.env` überein.

## Projektstruktur

```
db/init.sql            Datenbankschema (wird bei jedem Start idempotent angewendet)
docker-compose.yml     alle Dienste
frontend/              statische Seiten (Admin, Analytics)
nginx/                 Reverse Proxy
whisperx-service/      Spracherkennung (Python, faster-whisper)
examples/              Beispiel einer Gesprächsmethodik für die KI-Bewertung
pipeline/              Go-Modul
  cmd/api              REST-API
  cmd/collector        Import aus MangoOffice
  cmd/transcriber      Transkription
  cmd/analyzer         KI-Analyse
  internal/            gemeinsame Pakete (Konfiguration, S3, Kafka, Datenbank, Worker)
```

## Entwicklung

```bash
cd pipeline && go test ./...

# Integrationstests mit temporärer Datenbank (wird geleert!)
docker run --rm -d --name pg-test -p 55432:5432 -e POSTGRES_PASSWORD=test postgres:16-alpine
TEST_POSTGRES_DSN=postgres://postgres:test@localhost:55432/postgres go test -p 1 ./...
docker stop pg-test
```

## Lizenz

Alle Rechte vorbehalten – siehe [LICENSE](LICENSE).
