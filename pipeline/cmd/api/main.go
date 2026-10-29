// REST API системы записей звонков (замена прежнего Python/FastAPI).
//
// Эндпоинты:
//
//	POST /calls/upload                  — ручная загрузка аудио
//	GET  /calls                         — список звонков (с фильтрами)
//	GET  /calls/{call_id}               — полная информация о звонке
//	GET  /calls/{call_id}/audio         — стриминг аудиофайла (Range)
//	GET  /calls/{call_id}/transcript    — транскрипция
//	GET  /calls/{call_id}/analysis      — результаты анализа
//	GET  /calls/{call_id}/status        — только статус (легковесный)
//	GET  /admin/stats                   — статистика для дашборда
//	GET  /admin/logs                    — журнал операций и ошибок
//	*    /admin/methodologies...         — методики анализа (см. methodology.go)
//	GET  /health                        — health check (без авторизации)
//
// Авторизация: заголовок X-API-Key или параметр ?api_key= (ключ из API_KEY).
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/twmb/franz-go/pkg/kgo"

	"callparser/internal/app"
	"callparser/internal/llm"
	"callparser/internal/methodology"
	"callparser/internal/objstore"
	"callparser/internal/queue"
	"callparser/internal/store"
)

type server struct {
	db     *store.Store
	s3     *minio.Client
	bucket string
	kc     *kgo.Client
	apiKey string
	// generate составляет методику из текста админа (в тестах — заглушка)
	generate func(ctx context.Context, sourceText string) (*methodology.Methodology, error)
}

// generateTimeout — сколько ждём, пока модель составит методику.
const generateTimeout = 10 * time.Minute

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	auth := func(h http.HandlerFunc) http.HandlerFunc { return requireAuth(s.apiKey, h) }

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "api"})
	})
	mux.HandleFunc("GET /openapi.json", serveOpenAPI)
	mux.HandleFunc("GET /docs", serveDocs)

	mux.HandleFunc("POST /calls/upload", auth(s.uploadCall))
	mux.HandleFunc("GET /calls", auth(s.listCalls))
	mux.HandleFunc("GET /calls/{call_id}", auth(s.getCall))
	mux.HandleFunc("GET /calls/{call_id}/status", auth(s.getStatus))
	mux.HandleFunc("GET /calls/{call_id}/audio", auth(s.getAudio))
	mux.HandleFunc("GET /calls/{call_id}/transcript", auth(s.getTranscript))
	mux.HandleFunc("GET /calls/{call_id}/analysis", auth(s.getAnalysis))

	mux.HandleFunc("GET /admin/stats", auth(s.getStats))
	mux.HandleFunc("GET /admin/logs", auth(s.getLogs))

	mux.HandleFunc("GET /admin/methodologies", auth(s.listMethodologies))
	mux.HandleFunc("POST /admin/methodologies", auth(s.createMethodology))
	mux.HandleFunc("POST /admin/methodologies/extract-text", auth(s.extractMethodologyText))
	mux.HandleFunc("POST /admin/methodologies/generate", auth(s.generateMethodology))
	mux.HandleFunc("GET /admin/methodologies/{id}", auth(s.getMethodology))
	mux.HandleFunc("PUT /admin/methodologies/{id}", auth(s.updateMethodology))
	mux.HandleFunc("DELETE /admin/methodologies/{id}", auth(s.deleteMethodology))
	mux.HandleFunc("POST /admin/methodologies/{id}/activate", auth(s.activateMethodology))

	return logRequests(mux)
}

func main() {
	ctx, stop := app.Init()
	defer stop()

	s3Cfg := objstore.ConfigFromEnv()
	apiKey := app.Getenv("API_KEY", "")
	if apiKey == "" {
		slog.Warn("API_KEY не задан — авторизация отключена (dev-режим)")
	}

	db, err := store.New(ctx, app.MustEnv("POSTGRES_DSN"))
	if err != nil {
		app.Fatal("postgres connect", err)
	}
	defer db.Close()

	s3, err := objstore.New(s3Cfg)
	if err != nil {
		app.Fatal("s3 connect", err)
	}
	if err := objstore.EnsureBucket(ctx, s3, s3Cfg.Bucket); err != nil {
		app.Fatal("s3 bucket", err)
	}

	kc, err := queue.NewProducer(queue.Bootstrap())
	if err != nil {
		app.Fatal("kafka connect", err)
	}
	defer kc.Close()
	if err := queue.WaitReady(ctx, kc); err != nil {
		app.Fatal("kafka", err)
	}

	// Модель нужна только для составления методик: API работает и без неё
	model := llm.New(
		app.Getenv("OLLAMA_HOST", "http://host.docker.internal:11434"),
		app.Getenv("OLLAMA_MODEL", "qwen3.5:9b"),
		generateTimeout,
	)
	generate := func(ctx context.Context, text string) (*methodology.Methodology, error) {
		return methodology.Generate(ctx, model, text)
	}

	s := &server{db: db, s3: s3, bucket: s3Cfg.Bucket, kc: kc, apiKey: apiKey, generate: generate}
	srv := &http.Server{
		Addr:              ":" + app.Getenv("PORT", "8000"),
		Handler:           s.routes(),
		ReadHeaderTimeout: 10 * time.Second,
		// Без WriteTimeout: загрузка больших файлов и стриминг аудио идут долго
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Warn("shutdown", "err", err)
		}
	}()

	slog.Info("API слушает", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		app.Fatal("http server", err)
	}
	slog.Info("остановлен")
}
