// Сервис анализа транскрипций через Ollama/Gemma.
//
// Pipeline:
//
//	Kafka 'transcriptions-done' {call_id}
//	  → взять транскрипцию и метаданные из PostgreSQL
//	  → вызвать Ollama API (Gemma)
//	  → обновить PostgreSQL (analysis_*, status='analyzed')
//	  → отправить в Kafka 'analysis-done' (полные данные звонка)
//	  → опционально: POST вебхук в CRM
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"

	"callparser/internal/app"
	"callparser/internal/queue"
	"callparser/internal/store"
	"callparser/internal/worker"
)

// ollamaTimeout — сколько ждём ответа модели на одну транскрипцию.
const ollamaTimeout = 5 * time.Minute

// callRow — данные звонка, нужные для анализа и исходящего события.
type callRow struct {
	CallID       string
	FilePath     string
	Source       string
	EmployeeID   string
	EmployeeName string
	PhoneFrom    string
	PhoneTo      string
	Direction    string
	Duration     int
	Transcript   string
	Language     string
	LangProb     float64
	Segments     json.RawMessage
}

func loadCall(ctx context.Context, db *store.Store, callID string) (*callRow, error) {
	c := &callRow{CallID: callID}
	err := db.DB.QueryRow(ctx,
		`SELECT COALESCE(file_path, ''), source,
		        COALESCE(employee_id, ''), COALESCE(employee_name, ''),
		        COALESCE(phone_from, ''), COALESCE(phone_to, ''),
		        COALESCE(direction, ''), COALESCE(duration_seconds, 0),
		        COALESCE(transcript_text, ''), COALESCE(transcript_language, ''),
		        COALESCE(transcript_language_probability, 0),
		        COALESCE(transcript_segments, '[]'::jsonb)
		 FROM calls WHERE call_id = $1`, callID,
	).Scan(&c.FilePath, &c.Source,
		&c.EmployeeID, &c.EmployeeName,
		&c.PhoneFrom, &c.PhoneTo,
		&c.Direction, &c.Duration,
		&c.Transcript, &c.Language,
		&c.LangProb,
		&c.Segments,
	)
	return c, err
}

type analyzer struct {
	db         *store.Store
	kc         *kgo.Client
	ollama     *ollamaClient
	crmWebhook string
}

func (a *analyzer) process(ctx context.Context, callID string) error {
	call, err := loadCall(ctx, a.db, callID)
	if errors.Is(err, pgx.ErrNoRows) {
		return worker.Permanent(fmt.Errorf("звонок не найден"))
	}
	if err != nil {
		return fmt.Errorf("db select: %w", err)
	}
	if strings.TrimSpace(call.Transcript) == "" {
		return worker.Permanent(fmt.Errorf("пустая транскрипция: речь не распознана"))
	}

	prompt := buildPrompt(call.EmployeeName, call.EmployeeID, call.Direction, call.Duration, call.Transcript)
	analysis, err := a.ollama.analyze(ctx, prompt)
	if err != nil {
		return fmt.Errorf("ollama: %w", err)
	}

	violations, _ := json.Marshal(analysis.ScriptViolations)
	actions, _ := json.Marshal(analysis.ActionItems)
	topics, _ := json.Marshal(analysis.Topics)
	_, err = a.db.DB.Exec(ctx,
		`UPDATE calls SET
		     status = $1,
		     analysis_summary = $2,
		     analysis_sentiment = $3,
		     analysis_script_compliance = $4,
		     analysis_script_violations = $5,
		     analysis_action_items = $6,
		     analysis_score = $7,
		     analysis_topics = $8,
		     analysis_call_outcome = $9,
		     analyzed_at = NOW()
		 WHERE call_id = $10`,
		store.StatusAnalyzed,
		analysis.Summary,
		analysis.Sentiment,
		analysis.ScriptCompliance,
		violations,
		actions,
		analysis.Score,
		topics,
		analysis.CallOutcome,
		callID,
	)
	if err != nil {
		return fmt.Errorf("db update: %w", err)
	}
	a.db.Log(ctx, callID, "info", "analysis",
		fmt.Sprintf("Анализ завершён: score=%d, sentiment=%s", analysis.Score, analysis.Sentiment))

	// Анализ уже сохранён: сбои уведомлений ниже не делают этап неудачным.
	a.notify(ctx, call, analysis)

	slog.Info("проанализировано", "call_id", callID,
		"score", analysis.Score,
		"sentiment", analysis.Sentiment,
		"outcome", analysis.CallOutcome,
	)
	return nil
}

// notify отправляет результат во внешние системы: Kafka 'analysis-done' и вебхук CRM.
func (a *analyzer) notify(ctx context.Context, call *callRow, analysis *AnalysisResult) {
	event := map[string]any{
		"call_id":       call.CallID,
		"file_path":     call.FilePath,
		"source":        call.Source,
		"employee_id":   call.EmployeeID,
		"employee_name": call.EmployeeName,
		"phone_from":    call.PhoneFrom,
		"phone_to":      call.PhoneTo,
		"direction":     call.Direction,
		"duration":      call.Duration,
		"transcript": map[string]any{
			"full_text":            call.Transcript,
			"language":             call.Language,
			"language_probability": call.LangProb,
			"segments":             call.Segments,
		},
		"analysis":    analysis,
		"analyzed_at": time.Now().UTC().Format(time.RFC3339),
	}
	if err := queue.Publish(ctx, a.kc, queue.TopicAnalysisDone, event); err != nil {
		slog.Warn("kafka produce analysis-done", "call_id", call.CallID, "err", err)
	}

	if a.crmWebhook == "" {
		return
	}
	body, _ := json.Marshal(map[string]any{
		"call_id":       call.CallID,
		"employee_id":   call.EmployeeID,
		"employee_name": call.EmployeeName,
		"phone_from":    call.PhoneFrom,
		"phone_to":      call.PhoneTo,
		"duration":      call.Duration,
		"analysis":      analysis,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.crmWebhook, bytes.NewReader(body))
	if err != nil {
		slog.Warn("webhook create request", "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		slog.Warn("webhook send", "call_id", call.CallID, "err", err)
		a.db.Log(ctx, call.CallID, "warning", "analysis", "Вебхук CRM не отправлен: "+err.Error())
		return
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		slog.Warn("webhook ответил ошибкой", "call_id", call.CallID, "status", resp.StatusCode)
		a.db.Log(ctx, call.CallID, "warning", "analysis", fmt.Sprintf("Вебхук CRM ответил %d", resp.StatusCode))
		return
	}
	slog.Info("вебхук отправлен", "call_id", call.CallID)
}

func main() {
	ctx, stop := app.Init()
	defer stop()

	postgresDSN := app.MustEnv("POSTGRES_DSN")
	ollama := &ollamaClient{
		host:  app.Getenv("OLLAMA_HOST", "http://ollama:11434"),
		model: app.Getenv("OLLAMA_MODEL", "gemma3:4b"),
		http:  &http.Client{Timeout: ollamaTimeout},
	}

	db, err := store.New(ctx, postgresDSN)
	if err != nil {
		app.Fatal("postgres connect", err)
	}
	defer db.Close()

	kc, err := queue.NewConsumer(queue.Bootstrap(), "analyzer-group", queue.TopicTranscriptionsDone)
	if err != nil {
		app.Fatal("kafka connect", err)
	}
	defer kc.Close()
	if err := queue.WaitReady(ctx, kc); err != nil {
		app.Fatal("kafka", err)
	}
	// До скачивания модели: оно может идти десятки минут, сервис при этом жив.
	app.Heartbeat(ctx, db.DB.Ping, kc.Ping)

	if !app.WaitHTTP(ctx, "ollama", ollama.host+"/api/tags", 20, 5*time.Second) && ctx.Err() != nil {
		return
	}
	// Без модели анализ невозможен: падаем, Docker перезапустит сервис.
	if err := ollama.ensureModel(ctx); err != nil {
		app.Fatal("модель Ollama недоступна", err)
	}

	a := &analyzer{
		db:         db,
		kc:         kc,
		ollama:     ollama,
		crmWebhook: app.Getenv("CRM_WEBHOOK_URL", ""),
	}

	worker.Run(ctx, worker.Stage{
		Name:       "analysis",
		Topic:      queue.TopicTranscriptionsDone,
		Group:      "analyzer-group",
		Ready:      store.StatusTranscribed,
		Working:    store.StatusAnalyzing,
		Failed:     store.StatusFailedAnalysis,
		StuckAfter: 3 * ollamaTimeout,
		Process:    a.process,
	}, db, kc)
	slog.Info("остановлен")
}
