// Сервис транскрибации звонков.
//
// Pipeline:
//
//	Kafka 'calls-ready' {call_id}
//	  → взять file_path из PostgreSQL, скачать аудио из MinIO
//	  → POST аудио в whisperx-service
//	  → обновить PostgreSQL (transcript_*, status='transcribed')
//	  → отправить в Kafka 'transcriptions-done' {call_id}
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
	"github.com/twmb/franz-go/pkg/kgo"

	"callparser/internal/app"
	"callparser/internal/objstore"
	"callparser/internal/queue"
	"callparser/internal/store"
	"callparser/internal/worker"
)

// whisperTimeout — сколько ждём ответа WhisperX на один файл.
const whisperTimeout = 15 * time.Minute

type transcriber struct {
	db       *store.Store
	mc       *minio.Client
	bucket   string
	kc       *kgo.Client
	whisper  *whisperClient
	language string
}

func (t *transcriber) process(ctx context.Context, callID string) error {
	var filePath *string
	err := t.db.DB.QueryRow(ctx, `SELECT file_path FROM calls WHERE call_id = $1`, callID).Scan(&filePath)
	if errors.Is(err, pgx.ErrNoRows) {
		return worker.Permanent(fmt.Errorf("звонок не найден"))
	}
	if err != nil {
		return fmt.Errorf("db select: %w", err)
	}
	if filePath == nil || *filePath == "" {
		return worker.Permanent(fmt.Errorf("у звонка нет аудиофайла"))
	}

	obj, err := t.mc.GetObject(ctx, t.bucket, *filePath, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("minio get: %w", err)
	}
	defer obj.Close()

	transcript, err := t.whisper.transcribe(ctx, obj, path.Base(*filePath), t.language)
	if err != nil {
		return fmt.Errorf("whisperx: %w", err)
	}

	segmentsJSON, err := json.Marshal(transcript.Segments)
	if err != nil {
		return fmt.Errorf("marshal segments: %w", err)
	}

	_, err = t.db.DB.Exec(ctx,
		`UPDATE calls SET
		     status = $1,
		     transcript_text = $2,
		     transcript_language = $3,
		     transcript_language_probability = $4,
		     transcript_segments = $5,
		     transcribed_at = NOW()
		 WHERE call_id = $6`,
		store.StatusTranscribed,
		transcript.FullText,
		transcript.Language,
		transcript.LanguageProbability,
		segmentsJSON,
		callID,
	)
	if err != nil {
		return fmt.Errorf("db update: %w", err)
	}
	t.db.Log(ctx, callID, "info", "transcription",
		fmt.Sprintf("Транскрибировано: %d сегментов", len(transcript.Segments)))

	// Если событие не ушло, звонок останется в 'transcribed' и анализатор
	// подберёт его сам (sweeper), поэтому это не ошибка этапа.
	if err := queue.Publish(ctx, t.kc, queue.TopicTranscriptionsDone, queue.CallEvent{CallID: callID}); err != nil {
		slog.Warn("kafka produce transcriptions-done", "call_id", callID, "err", err)
	}

	slog.Info("транскрибировано", "call_id", callID,
		"segments", len(transcript.Segments),
		"language", transcript.Language,
		"probability", transcript.LanguageProbability,
	)
	return nil
}

func main() {
	ctx, stop := app.Init()
	defer stop()

	minioCfg := objstore.ConfigFromEnv()
	postgresDSN := app.MustEnv("POSTGRES_DSN")
	whisperURL := app.Getenv("WHISPERX_URL", "http://whisperx-service:8001")

	db, err := store.New(ctx, postgresDSN)
	if err != nil {
		app.Fatal("postgres connect", err)
	}
	defer db.Close()

	mc, err := objstore.New(minioCfg)
	if err != nil {
		app.Fatal("minio connect", err)
	}

	kc, err := queue.NewConsumer(queue.Bootstrap(), "transcriber-group", queue.TopicCallsReady)
	if err != nil {
		app.Fatal("kafka connect", err)
	}
	defer kc.Close()
	if err := queue.WaitReady(ctx, kc); err != nil {
		app.Fatal("kafka", err)
	}

	if !app.WaitHTTP(ctx, "whisperx-service", whisperURL+"/health", 60, 5*time.Second) {
		if ctx.Err() != nil {
			return
		}
		slog.Warn("whisperx-service не ответил, стартую всё равно: ошибки уйдут в повтор")
	}

	t := &transcriber{
		db:       db,
		mc:       mc,
		bucket:   minioCfg.Bucket,
		kc:       kc,
		whisper:  &whisperClient{baseURL: whisperURL, http: &http.Client{Timeout: whisperTimeout}},
		language: app.Getenv("WHISPER_LANGUAGE", "ru"),
	}

	worker.Run(ctx, worker.Stage{
		Name:    "transcription",
		Topic:   queue.TopicCallsReady,
		Group:   "transcriber-group",
		Ready:   store.StatusStored,
		Working: store.StatusTranscribing,
		Failed:  store.StatusFailedTranscription,
		// Запас над таймаутом WhisperX, чтобы не пометить живую обработку зависшей.
		StuckAfter: 2 * whisperTimeout,
		// На CPU очередь может стоять часами: дубликаты событий
		// безопасны (Claim их отсеет), но слать их часто незачем.
		IdleAfter: time.Hour,
		Process:   t.process,
	}, db, kc)
	slog.Info("остановлен")
}
