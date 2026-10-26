// Пакет worker — общий цикл этапа конвейера (транскрибация, анализ).
//
// Для каждого события из входного топика:
//
//	Claim (ready|failed → working) → Process → коммит оффсета
//
// Process сам выставляет итоговый статус этапа. При ошибке звонок уходит
// в failed с увеличением retry_count.
//
// Параллельно работает sweeper:
//   - звонки, зависшие в working (процесс упал), → failed;
//   - звонки в ready без движения и failed с оставшимися попытками
//     снова отправляются во входной топик.
//
// Поэтому потерянное событие или временный сбой Ollama/WhisperX
// не оставляют звонок необработанным навсегда.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"callparser/internal/app"
	"callparser/internal/queue"
	"callparser/internal/store"
)

// Stage описывает один этап конвейера.
type Stage struct {
	Name  string // имя этапа для журнала: transcription | analysis
	Topic string // входной топик
	Group string // consumer group

	Ready   string // статус «готов к этапу»
	Working string // статус «этап выполняется»
	Failed  string // статус ошибки этапа

	MaxRetries   int
	StuckAfter   time.Duration // сколько можно висеть в Working
	IdleAfter    time.Duration // сколько можно висеть в Ready без обработки
	RetryBackoff time.Duration // пауза перед повтором после ошибки
	SweepEvery   time.Duration

	// Process выполняет этап и выставляет итоговый статус.
	Process func(ctx context.Context, callID string) error
}

// WithDefaults заполняет незаданные интервалы.
func (s Stage) WithDefaults() Stage {
	if s.MaxRetries == 0 {
		s.MaxRetries = app.GetInt("MAX_RETRIES", 3)
	}
	if s.StuckAfter == 0 {
		s.StuckAfter = 30 * time.Minute
	}
	if s.IdleAfter == 0 {
		s.IdleAfter = 30 * time.Minute
	}
	if s.RetryBackoff == 0 {
		s.RetryBackoff = 5 * time.Minute
	}
	if s.SweepEvery == 0 {
		s.SweepEvery = time.Minute
	}
	return s
}

// ── постоянные ошибки ───────────────────────────────────────────

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent помечает ошибку как неисправимую повтором.
func Permanent(err error) error { return permanentError{err} }

func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

// ── запуск ──────────────────────────────────────────────────────

// Run читает топик и обрабатывает события до отмены ctx.
func Run(ctx context.Context, st Stage, db *store.Store, kc *kgo.Client) {
	st = st.WithDefaults()
	go sweep(ctx, st, db, kc)

	slog.Info("слушаю топик", "topic", st.Topic, "max_retries", st.MaxRetries)
	for {
		fetches := kc.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				slog.Error("kafka fetch error", "topic", e.Topic, "err", e.Err)
			}
			app.Sleep(ctx, 2*time.Second)
			continue
		}

		stopped := false
		fetches.EachRecord(func(record *kgo.Record) {
			if stopped {
				return
			}
			if !handle(ctx, st, db, kc, record) {
				// Остановка посреди обработки: оффсет не коммитим,
				// событие придёт снова после перезапуска.
				stopped = true
				return
			}
			commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			if err := kc.CommitRecords(commitCtx, record); err != nil {
				slog.Warn("kafka commit", "err", err)
			}
			cancel()
		})
		if stopped {
			return
		}
	}
}

// handle обрабатывает одно событие. false — сервис останавливается
// и событие надо оставить незакоммиченным.
func handle(ctx context.Context, st Stage, db *store.Store, kc *kgo.Client, record *kgo.Record) bool {
	var ev queue.CallEvent
	if err := json.Unmarshal(record.Value, &ev); err != nil || ev.CallID == "" {
		slog.Error("некорректное событие, пропускаю", "topic", record.Topic, "err", err, "value", string(record.Value))
		return true
	}
	callID := ev.CallID

	claimed, err := db.Claim(ctx, callID, st.Ready, st.Failed, st.Working, st.MaxRetries)
	if err != nil {
		// Звонок остался в Ready/Failed — его подберёт sweeper.
		slog.Error("не удалось захватить звонок", "call_id", callID, "err", err)
		return ctx.Err() == nil
	}
	if !claimed {
		slog.Info("звонок уже обработан или обрабатывается, пропускаю", "call_id", callID)
		return true
	}

	started := time.Now()
	err = st.Process(ctx, callID)
	if err == nil {
		slog.Info("этап завершён", "stage", st.Name, "call_id", callID, "took", time.Since(started).Round(time.Second))
		return true
	}

	bg := context.WithoutCancel(ctx)
	if ctx.Err() != nil {
		slog.Warn("остановка во время обработки, возвращаю звонок в очередь", "call_id", callID)
		if relErr := db.Release(bg, callID, st.Working, st.Ready); relErr != nil {
			slog.Error("release", "call_id", callID, "err", relErr)
		}
		return false
	}

	permanent := IsPermanent(err)
	slog.Error("ОШИБКА обработки", "stage", st.Name, "call_id", callID, "permanent", permanent, "err", err)
	if mfErr := db.MarkFailed(bg, callID, st.Failed, permanent, st.MaxRetries); mfErr != nil {
		slog.Error("не удалось сохранить статус ошибки", "call_id", callID, "err", mfErr)
	}
	db.Log(bg, callID, "error", st.Name, "Ошибка: "+err.Error())
	if pubErr := queue.Publish(bg, kc, queue.TopicErrors, queue.ErrorEvent{
		CallID: callID, Stage: st.Name, Error: err.Error(), Permanent: permanent,
	}); pubErr != nil {
		slog.Warn("kafka produce calls-errors", "err", pubErr)
	}
	return true
}

// ── sweeper ─────────────────────────────────────────────────────

func sweep(ctx context.Context, st Stage, db *store.Store, kc *kgo.Client) {
	for app.Sleep(ctx, st.SweepEvery) {
		stuck, err := db.RecoverStuck(ctx, st.Working, st.Failed, st.StuckAfter)
		if err != nil {
			slog.Warn("sweeper: recover stuck", "err", err)
		}
		for _, id := range stuck {
			slog.Warn("звонок завис в обработке, помечаю ошибкой", "call_id", id, "status", st.Working)
			db.Log(ctx, id, "error", st.Name, "Превышено время обработки (процесс был прерван)")
		}

		due, err := db.DueForRetry(ctx, st.Ready, st.Failed, st.IdleAfter, st.RetryBackoff, st.MaxRetries, 100)
		if err != nil {
			slog.Warn("sweeper: due for retry", "err", err)
			continue
		}
		for _, id := range due {
			if err := queue.Publish(ctx, kc, st.Topic, queue.CallEvent{CallID: id}); err != nil {
				slog.Warn("sweeper: не удалось поставить в очередь", "call_id", id, "err", err)
				continue
			}
			slog.Info("sweeper: звонок снова поставлен в очередь", "call_id", id, "topic", st.Topic)
		}
	}
}
