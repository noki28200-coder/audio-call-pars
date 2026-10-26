// Сервис скачивания записей звонков из МангоОфис API.
//
// Pipeline каждые POLL_INTERVAL_SECONDS:
//  1. Повторить скачивание для failed_download и зависших pending
//  2. Получить список новых записей из МангоОфис API
//  3. Для каждой новой записи:
//     - Создать запись в PostgreSQL (status=pending)
//     - Скачать MP3 из МангоОфис и загрузить в MinIO
//     - Обновить PostgreSQL (status=stored)
//     - Отправить событие в Kafka: calls-ready {call_id}
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
	"github.com/twmb/franz-go/pkg/kgo"

	"callparser/internal/app"
	"callparser/internal/objstore"
	"callparser/internal/queue"
	"callparser/internal/store"
)

type collector struct {
	db           *store.Store
	mc           *minio.Client
	bucket       string
	kc           *kgo.Client
	mango        *mangoClient
	pollInterval time.Duration
	lookback     time.Duration
	maxRetries   int
}

// download скачивает запись, кладёт в MinIO и переводит звонок в stored.
func (c *collector) download(ctx context.Context, callID, recordingID string) error {
	audio, err := c.mango.downloadRecording(ctx, recordingID)
	if err != nil {
		return fmt.Errorf("скачивание: %w", err)
	}
	if len(audio) == 0 {
		return errors.New("скачивание: пустой файл")
	}

	objectKey := fmt.Sprintf("calls/%s/%s.mp3", time.Now().UTC().Format("2006-01-02"), callID)
	_, err = c.mc.PutObject(ctx, c.bucket, objectKey,
		bytes.NewReader(audio), int64(len(audio)),
		minio.PutObjectOptions{ContentType: "audio/mpeg"},
	)
	if err != nil {
		return fmt.Errorf("minio upload: %w", err)
	}

	_, err = c.db.DB.Exec(ctx,
		`UPDATE calls SET status = $1, file_path = $2, file_size_bytes = $3 WHERE call_id = $4`,
		store.StatusStored, objectKey, len(audio), callID,
	)
	if err != nil {
		return fmt.Errorf("db update: %w", err)
	}
	c.db.Log(ctx, callID, "info", "download",
		fmt.Sprintf("Запись скачана и загружена (%d байт)", len(audio)))

	// Файл уже сохранён: если событие не ушло, транскрибатор подберёт
	// звонок в статусе stored сам, перекачивать запись не нужно.
	if err := queue.Publish(ctx, c.kc, queue.TopicCallsReady, queue.CallEvent{CallID: callID}); err != nil {
		slog.Warn("kafka produce calls-ready", "call_id", callID, "err", err)
	}
	slog.Info("загружено", "call_id", callID, "bytes", len(audio))
	return nil
}

func (c *collector) downloadOrFail(ctx context.Context, callID, recordingID string) bool {
	err := c.download(ctx, callID, recordingID)
	if err == nil {
		return true
	}
	if ctx.Err() != nil {
		// Остановка: звонок останется в pending/failed и будет повторён при следующем старте.
		return false
	}
	slog.Error("ошибка загрузки", "call_id", callID, "err", err)
	if mfErr := c.db.MarkFailed(ctx, callID, store.StatusFailedDownload, false, c.maxRetries); mfErr != nil {
		slog.Error("не удалось сохранить статус ошибки", "call_id", callID, "err", mfErr)
	}
	c.db.Log(ctx, callID, "error", "download", "Ошибка загрузки: "+err.Error())
	return false
}

// retryFailed повторяет скачивание для failed_download и для pending,
// которые зависли (сервис упал между созданием записи и скачиванием).
func (c *collector) retryFailed(ctx context.Context) {
	rows, err := c.db.DB.Query(ctx,
		`SELECT call_id, mango_recording_id FROM calls
		 WHERE status IN ($1, $2)
		   AND mango_recording_id IS NOT NULL
		   AND retry_count < $3
		   AND updated_at < NOW() - INTERVAL '5 minutes'
		 ORDER BY updated_at
		 LIMIT 10`,
		store.StatusFailedDownload, store.StatusPending, c.maxRetries,
	)
	if err != nil {
		slog.Warn("retry query", "err", err)
		return
	}
	type retryRow struct{ CallID, RecordingID string }
	toRetry, err := pgx.CollectRows(rows, pgx.RowToStructByPos[retryRow])
	if err != nil {
		slog.Warn("retry scan", "err", err)
		return
	}

	for _, r := range toRetry {
		if ctx.Err() != nil {
			return
		}
		slog.Info("повторная попытка скачивания", "call_id", r.CallID)
		c.downloadOrFail(ctx, r.CallID, r.RecordingID)
	}
}

func (c *collector) pollOnce(ctx context.Context) {
	c.retryFailed(ctx)

	// Окно с запасом в интервал опроса, чтобы не пропустить записи между опросами.
	now := time.Now().UTC()
	records, err := c.mango.listRecords(ctx, now.Add(-(c.lookback + c.pollInterval + time.Minute)), now)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("MangoOffice API ошибка", "err", err)
			c.db.Log(ctx, "", "error", "download", "MangoOffice API error: "+err.Error())
		}
		return
	}

	newCount := 0
	for _, rec := range records {
		if ctx.Err() != nil {
			return
		}
		callID := "mango_" + rec.RecordingID

		// ON CONFLICT без цели покрывает и call_id, и mango_recording_id:
		// пустой результат значит, что запись уже есть.
		var inserted string
		err := c.db.DB.QueryRow(ctx,
			`INSERT INTO calls
			 (call_id, mango_recording_id, source,
			  employee_id, employee_name, phone_from, phone_to,
			  direction, duration_seconds, start_time, status)
			 VALUES ($1, $2, 'mango', $3, $4, $5, $6, $7, $8, $9, $10)
			 ON CONFLICT DO NOTHING
			 RETURNING call_id`,
			callID, rec.RecordingID,
			nullIfEmpty(rec.EmployeeID), nullIfEmpty(rec.EmployeeName),
			nullIfEmpty(rec.PhoneFrom), nullIfEmpty(rec.PhoneTo),
			rec.Direction, rec.Duration, rec.StartTime, store.StatusPending,
		).Scan(&inserted)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			slog.Error("не удалось создать запись в БД", "call_id", callID, "err", err)
			continue
		}

		if c.downloadOrFail(ctx, callID, rec.RecordingID) {
			newCount++
		}
	}

	if newCount > 0 {
		slog.Info("обработано новых записей", "count", newCount)
	} else {
		slog.Info("новых записей нет")
	}
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func main() {
	ctx, stop := app.Init()
	defer stop()

	minioCfg := objstore.ConfigFromEnv()
	postgresDSN := app.MustEnv("POSTGRES_DSN")
	mango := &mangoClient{
		baseURL: app.Getenv("MANGO_API_URL", "https://app.mango-office.ru/vpbx"),
		apiKey:  app.MustEnv("MANGO_API_KEY"),
		apiSalt: app.MustEnv("MANGO_API_SALT"),
		http:    &http.Client{Timeout: 120 * time.Second},
	}

	db, err := store.New(ctx, postgresDSN)
	if err != nil {
		app.Fatal("postgres connect", err)
	}
	defer db.Close()

	mc, err := objstore.New(minioCfg)
	if err != nil {
		app.Fatal("minio connect", err)
	}

	kc, err := queue.NewProducer(queue.Bootstrap())
	if err != nil {
		app.Fatal("kafka connect", err)
	}
	defer kc.Close()
	if err := queue.WaitReady(ctx, kc); err != nil {
		app.Fatal("kafka", err)
	}

	c := &collector{
		db:           db,
		mc:           mc,
		bucket:       minioCfg.Bucket,
		kc:           kc,
		mango:        mango,
		pollInterval: time.Duration(app.GetInt("POLL_INTERVAL_SECONDS", 300)) * time.Second,
		lookback:     time.Duration(app.GetInt("LOOKBACK_MINUTES", 10)) * time.Minute,
		maxRetries:   app.GetInt("MAX_RETRIES", 3),
	}

	slog.Info("старт", "poll_interval", c.pollInterval, "lookback", c.lookback)
	for {
		c.pollOnce(ctx)
		if !app.Sleep(ctx, c.pollInterval) {
			break
		}
	}
	slog.Info("остановлен")
}
