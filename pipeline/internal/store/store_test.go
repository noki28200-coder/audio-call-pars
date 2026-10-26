package store

// Интеграционный тест на настоящем PostgreSQL. Запускается, только если задан
// TEST_POSTGRES_DSN (БД будет очищена!):
//
//	docker run --rm -d -p 55432:5432 -e POSTGRES_PASSWORD=test postgres:16-alpine
//	TEST_POSTGRES_DSN=postgres://postgres:test@localhost:55432/postgres go test ./internal/store/

import (
	"context"
	"os"
	"slices"
	"testing"
	"time"
)

func setup(t *testing.T) (*Store, context.Context) {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN не задан")
	}
	ctx := context.Background()
	s, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)

	schema, err := os.ReadFile("../../../db/init.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec(ctx, `DROP TABLE IF EXISTS calls, call_logs`); err != nil {
		t.Fatal(err)
	}
	// Дважды: скрипт должен быть идемпотентным.
	for range 2 {
		if _, err := s.DB.Exec(ctx, string(schema)); err != nil {
			t.Fatal("init.sql:", err)
		}
	}
	return s, ctx
}

func insertCall(t *testing.T, s *Store, ctx context.Context, id, status string, retries int, age time.Duration) {
	t.Helper()
	_, err := s.DB.Exec(ctx, `INSERT INTO calls (call_id, status, retry_count) VALUES ($1, $2, $3)`, id, status, retries)
	if err != nil {
		t.Fatal(err)
	}
	backdate(t, s, ctx, id, age)
}

// backdate сдвигает updated_at в прошлое (триггер на UPDATE ставит NOW(), поэтому отключаем его).
func backdate(t *testing.T, s *Store, ctx context.Context, id string, age time.Duration) {
	t.Helper()
	steps := []struct {
		sql  string
		args []any
	}{
		{`ALTER TABLE calls DISABLE TRIGGER trg_calls_updated_at`, nil},
		{`UPDATE calls SET updated_at = NOW() - make_interval(secs => $1) WHERE call_id = $2`, []any{age.Seconds(), id}},
		{`ALTER TABLE calls ENABLE TRIGGER trg_calls_updated_at`, nil},
	}
	for _, st := range steps {
		if _, err := s.DB.Exec(ctx, st.sql, st.args...); err != nil {
			t.Fatal(err)
		}
	}
}

func status(t *testing.T, s *Store, ctx context.Context, id string) (string, int) {
	t.Helper()
	var st string
	var rc int
	if err := s.DB.QueryRow(ctx, `SELECT status, retry_count FROM calls WHERE call_id = $1`, id).Scan(&st, &rc); err != nil {
		t.Fatal(err)
	}
	return st, rc
}

func TestClaim(t *testing.T) {
	s, ctx := setup(t)
	insertCall(t, s, ctx, "ready", StatusStored, 0, 0)
	insertCall(t, s, ctx, "failed-ok", StatusFailedTranscription, 2, 0)
	insertCall(t, s, ctx, "failed-max", StatusFailedTranscription, 3, 0)
	insertCall(t, s, ctx, "done", StatusTranscribed, 0, 0)

	cases := map[string]bool{"ready": true, "failed-ok": true, "failed-max": false, "done": false, "missing": false}
	for id, want := range cases {
		got, err := s.Claim(ctx, id, StatusStored, StatusFailedTranscription, StatusTranscribing, 3)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("Claim(%s) = %v, want %v", id, got, want)
		}
	}
	// Повторный Claim того же звонка — дубликат события.
	if got, _ := s.Claim(ctx, "ready", StatusStored, StatusFailedTranscription, StatusTranscribing, 3); got {
		t.Error("повторный Claim должен вернуть false")
	}
	if st, _ := status(t, s, ctx, "ready"); st != StatusTranscribing {
		t.Errorf("status = %s", st)
	}
}

func TestMarkFailedAndRelease(t *testing.T) {
	s, ctx := setup(t)
	insertCall(t, s, ctx, "a", StatusTranscribing, 0, 0)
	insertCall(t, s, ctx, "b", StatusTranscribing, 1, 0)

	if err := s.MarkFailed(ctx, "a", StatusFailedTranscription, false, 3); err != nil {
		t.Fatal(err)
	}
	if st, rc := status(t, s, ctx, "a"); st != StatusFailedTranscription || rc != 1 {
		t.Errorf("transient: %s %d", st, rc)
	}
	if err := s.MarkFailed(ctx, "b", StatusFailedTranscription, true, 3); err != nil {
		t.Fatal(err)
	}
	if st, rc := status(t, s, ctx, "b"); st != StatusFailedTranscription || rc != 3 {
		t.Errorf("permanent: %s %d", st, rc)
	}

	insertCall(t, s, ctx, "c", StatusTranscribing, 1, 0)
	if err := s.Release(ctx, "c", StatusTranscribing, StatusStored); err != nil {
		t.Fatal(err)
	}
	if st, rc := status(t, s, ctx, "c"); st != StatusStored || rc != 1 {
		t.Errorf("release: %s %d", st, rc)
	}
}

func TestRecoverStuck(t *testing.T) {
	s, ctx := setup(t)
	insertCall(t, s, ctx, "stuck", StatusTranscribing, 0, 2*time.Hour)
	insertCall(t, s, ctx, "fresh", StatusTranscribing, 0, time.Minute)

	ids, err := s.RecoverStuck(ctx, StatusTranscribing, StatusFailedTranscription, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(ids, []string{"stuck"}) {
		t.Fatalf("ids = %v", ids)
	}
	if st, rc := status(t, s, ctx, "stuck"); st != StatusFailedTranscription || rc != 1 {
		t.Errorf("stuck: %s %d", st, rc)
	}
}

func TestDueForRetry(t *testing.T) {
	s, ctx := setup(t)
	insertCall(t, s, ctx, "lost-event", StatusStored, 0, 2*time.Hour)
	insertCall(t, s, ctx, "queued", StatusStored, 0, time.Minute)
	insertCall(t, s, ctx, "failed-due", StatusFailedTranscription, 1, 10*time.Minute)
	insertCall(t, s, ctx, "failed-recent", StatusFailedTranscription, 1, time.Minute)
	insertCall(t, s, ctx, "failed-exhausted", StatusFailedTranscription, 3, 10*time.Minute)
	insertCall(t, s, ctx, "other-stage", StatusTranscribed, 0, 2*time.Hour)

	due := func() []string {
		ids, err := s.DueForRetry(ctx, StatusStored, StatusFailedTranscription, time.Hour, 5*time.Minute, 3, 100)
		if err != nil {
			t.Fatal(err)
		}
		slices.Sort(ids)
		return ids
	}
	if got := due(); !slices.Equal(got, []string{"failed-due", "lost-event"}) {
		t.Fatalf("due = %v", got)
	}
	// updated_at обновился — сразу второй раз те же звонки не выбираются.
	if got := due(); len(got) != 0 {
		t.Fatalf("повторная выборка должна быть пустой, got %v", got)
	}
}
