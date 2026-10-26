// Пакет store — работа с таблицами calls и call_logs.
//
// Жизненный цикл статуса звонка:
//
//	pending → stored → transcribing → transcribed → analyzing → analyzed
//
// Ошибки: failed_download | failed_transcription | failed_analysis.
// Звонок в failed_* повторяется, пока retry_count < MaxRetries.
package store

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	StatusPending             = "pending"
	StatusStored              = "stored"
	StatusTranscribing        = "transcribing"
	StatusTranscribed         = "transcribed"
	StatusAnalyzing           = "analyzing"
	StatusAnalyzed            = "analyzed"
	StatusFailedDownload      = "failed_download"
	StatusFailedTranscription = "failed_transcription"
	StatusFailedAnalysis      = "failed_analysis"
)

type Store struct {
	DB *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{DB: pool}, nil
}

func (s *Store) Close() { s.DB.Close() }

// Log пишет запись в журнал операций. Ошибки журнала не фатальны.
func (s *Store) Log(ctx context.Context, callID, level, stage, message string) {
	_, err := s.DB.Exec(ctx,
		`INSERT INTO call_logs (call_id, level, stage, message) VALUES ($1, $2, $3, $4)`,
		nullIfEmpty(callID), level, stage, message,
	)
	if err != nil {
		slog.Warn("не удалось записать лог в БД", "err", err)
	}
}

// Claim атомарно переводит звонок из ready (или failed с оставшимися попытками)
// в working. false — звонок уже обработан, обрабатывается или исчерпал попытки:
// такое событие — дубликат, его надо пропустить.
func (s *Store) Claim(ctx context.Context, callID, ready, failed, working string, maxRetries int) (bool, error) {
	tag, err := s.DB.Exec(ctx,
		`UPDATE calls SET status = $4
		 WHERE call_id = $1
		   AND (status = $2 OR (status = $3 AND retry_count < $5))`,
		callID, ready, failed, working, maxRetries,
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// MarkFailed переводит звонок в failed и увеличивает счётчик попыток.
// permanent — повторять бессмысленно (битый файл, пустая транскрипция):
// счётчик сразу выставляется в maxRetries.
func (s *Store) MarkFailed(ctx context.Context, callID, failed string, permanent bool, maxRetries int) error {
	_, err := s.DB.Exec(ctx,
		`UPDATE calls SET status = $2,
		     retry_count = CASE WHEN $3 THEN GREATEST(retry_count, $4) ELSE retry_count + 1 END
		 WHERE call_id = $1`,
		callID, failed, permanent, maxRetries,
	)
	return err
}

// Release возвращает звонок из working в ready без траты попытки
// (используется при остановке сервиса посреди обработки).
func (s *Store) Release(ctx context.Context, callID, working, ready string) error {
	_, err := s.DB.Exec(ctx,
		`UPDATE calls SET status = $3 WHERE call_id = $1 AND status = $2`,
		callID, working, ready,
	)
	return err
}

// RecoverStuck переводит в failed звонки, которые висят в working дольше after
// (процесс упал посреди обработки). Попытка засчитывается.
func (s *Store) RecoverStuck(ctx context.Context, working, failed string, after time.Duration) ([]string, error) {
	rows, err := s.DB.Query(ctx,
		`UPDATE calls SET status = $2, retry_count = retry_count + 1
		 WHERE status = $1 AND updated_at < NOW() - make_interval(secs => $3)
		 RETURNING call_id`,
		working, failed, after.Seconds(),
	)
	if err != nil {
		return nil, err
	}
	return collectIDs(rows)
}

// DueForRetry возвращает звонки, которые надо снова поставить в очередь:
//   - в ready дольше idle (событие потерялось, например Kafka была недоступна);
//   - в failed с оставшимися попытками, дольше backoff с последней ошибки.
//
// updated_at выбранных строк обновляется, поэтому один и тот же звонок
// попадёт сюда снова не раньше чем через idle/backoff.
func (s *Store) DueForRetry(ctx context.Context, ready, failed string, idle, backoff time.Duration, maxRetries, limit int) ([]string, error) {
	rows, err := s.DB.Query(ctx,
		`UPDATE calls SET updated_at = NOW()
		 WHERE call_id IN (
		     SELECT call_id FROM calls
		     WHERE (status = $1 AND updated_at < NOW() - make_interval(secs => $3))
		        OR (status = $2 AND retry_count < $5 AND updated_at < NOW() - make_interval(secs => $4))
		     ORDER BY updated_at
		     LIMIT $6
		     FOR UPDATE SKIP LOCKED)
		 RETURNING call_id`,
		ready, failed, idle.Seconds(), backoff.Seconds(), maxRetries, limit,
	)
	if err != nil {
		return nil, err
	}
	return collectIDs(rows)
}

func collectIDs(rows pgx.Rows) ([]string, error) {
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
