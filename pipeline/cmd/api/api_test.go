package main

// Интеграционные тесты запускаются, только если задан TEST_POSTGRES_DSN (БД будет очищена!):
//
//	docker run --rm -d -p 55432:5432 -e POSTGRES_PASSWORD=test postgres:16-alpine
//	TEST_POSTGRES_DSN=postgres://postgres:test@localhost:55432/postgres go test ./cmd/api/

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"callparser/internal/store"
)

// ── юнит-тесты ──────────────────────────────────────────────────

func TestSnippet(t *testing.T) {
	text := strings.Repeat("а", 100) + "Тариф Премиум" + strings.Repeat("б", 100)
	got, ok := snippet(text, "тариф", 5)
	if !ok || got != "…ааааа"+"Тариф"+" Прем…" {
		t.Fatalf("got %q ok=%v", got, ok)
	}
	if got, ok := snippet("короткий тариф", "ТАРИФ", 60); !ok || got != "короткий тариф" {
		t.Fatalf("без многоточий: got %q", got)
	}
	if _, ok := snippet("ничего нет", "тариф", 60); ok {
		t.Fatal("не должно находить")
	}
}

func TestParseRange(t *testing.T) {
	cases := []struct {
		header             string
		start, end         int64
		partial, wantError bool
	}{
		{"bytes=4-8", 4, 8, true, false},
		{"bytes=4-", 4, 19, true, false},
		{"bytes=10-999", 10, 19, true, false}, // конец обрезается по размеру
		{"bytes=-4", 16, 19, true, false},     // последние 4 байта
		{"bytes=-100", 0, 19, true, false},
		{"bytes=500-", 0, 0, false, true}, // за пределами файла → 416
		{"bytes=8-4", 0, 0, false, true},
		{"bytes=0-1,5-6", 0, 0, false, false}, // несколько диапазонов не поддерживаем — весь файл
		{"items=0-1", 0, 0, false, false},
		{"", 0, 0, false, false},
	}
	for _, c := range cases {
		start, end, partial, err := parseRange(c.header, 20)
		if (err != nil) != c.wantError || partial != c.partial || (partial && (start != c.start || end != c.end)) {
			t.Errorf("%q: got %d-%d partial=%v err=%v", c.header, start, end, partial, err)
		}
	}
}

func TestOpenAPISpecCoversRoutes(t *testing.T) {
	var spec struct {
		Paths map[string]map[string]any `json:"paths"`
	}
	if err := json.Unmarshal(openapiSpec, &spec); err != nil {
		t.Fatal("openapi.json невалидный:", err)
	}
	for _, route := range []string{
		"post /calls/upload", "get /calls", "get /calls/{call_id}", "get /calls/{call_id}/status",
		"get /calls/{call_id}/audio", "get /calls/{call_id}/transcript", "get /calls/{call_id}/analysis",
		"get /admin/stats", "get /admin/logs", "get /health",
	} {
		method, path, _ := strings.Cut(route, " ")
		if spec.Paths[path][method] == nil {
			t.Errorf("в openapi.json нет %s", route)
		}
	}
}

func TestRequireAuth(t *testing.T) {
	h := requireAuth("secret", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	cases := []struct {
		name   string
		header string
		query  string
		want   int
	}{
		{"без ключа", "", "", http.StatusForbidden},
		{"неверный", "wrong", "", http.StatusForbidden},
		{"заголовок", "secret", "", http.StatusOK},
		{"query", "", "?api_key=secret", http.StatusOK},
	}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodGet, "/calls"+c.query, nil)
		if c.header != "" {
			r.Header.Set("X-API-Key", c.header)
		}
		w := httptest.NewRecorder()
		h(w, r)
		if w.Code != c.want {
			t.Errorf("%s: code %d, want %d", c.name, w.Code, c.want)
		}
	}

	// Пустой ключ — dev-режим без авторизации
	w := httptest.NewRecorder()
	requireAuth("", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusOK {
		t.Errorf("dev-режим: code %d", w.Code)
	}
}

// ── интеграционные тесты ────────────────────────────────────────

func setupServer(t *testing.T) (*server, http.Handler) {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN не задан")
	}
	ctx := context.Background()
	db, err := store.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	schema, err := os.ReadFile("../../../db/init.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB.Exec(ctx, `DROP TABLE IF EXISTS calls, call_logs`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB.Exec(ctx, string(schema)); err != nil {
		t.Fatal(err)
	}
	_, err = db.DB.Exec(ctx, `
		INSERT INTO calls (call_id, mango_recording_id, source, employee_name, phone_from, status,
		                   transcript_text, transcript_segments, analysis_summary, analysis_score,
		                   analysis_topics, file_size_bytes, created_at)
		VALUES
		  ('c1', 'rec-1', 'mango', 'Иван', '79990001122', 'analyzed',
		   'Здравствуйте, хочу купить тариф Премиум', '[{"start":0,"end":1,"text":"Здравствуйте"}]',
		   'Клиент купил тариф', 8, '["тариф"]', 2097152, NOW()),
		  ('c2', NULL, 'manual', 'Пётр', '74950000000', 'stored',
		   NULL, NULL, NULL, NULL, NULL, 1048576, NOW() - INTERVAL '3 days');
		INSERT INTO call_logs (call_id, level, stage, message) VALUES
		  ('c1', 'info', 'analysis', 'Анализ завершён'),
		  ('c2', 'error', 'transcription', 'Ошибка');`)
	if err != nil {
		t.Fatal(err)
	}
	s := &server{db: db, apiKey: "k"}
	return s, s.routes()
}

func get(t *testing.T, h http.Handler, path string) (int, map[string]any) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, path, nil)
	r.Header.Set("X-API-Key", "k")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("%s: невалидный JSON: %s", path, w.Body.String())
	}
	return w.Code, body
}

func TestListCalls(t *testing.T) {
	_, h := setupServer(t)

	code, body := get(t, h, "/calls")
	if code != 200 || body["total"].(float64) != 2 {
		t.Fatalf("code=%d body=%v", code, body)
	}
	first := body["calls"].([]any)[0].(map[string]any)
	if first["call_id"] != "c1" {
		t.Errorf("сортировка по created_at DESC: первым должен быть c1, got %v", first["call_id"])
	}
	if _, has := first["transcript_snippet"]; has {
		t.Error("transcript_snippet только при поиске")
	}

	_, body = get(t, h, "/calls?status=stored")
	if body["total"].(float64) != 1 {
		t.Errorf("фильтр status: %v", body["total"])
	}

	_, body = get(t, h, "/calls?q=премиум")
	calls := body["calls"].([]any)
	if len(calls) != 1 || !strings.Contains(calls[0].(map[string]any)["transcript_snippet"].(string), "Премиум") {
		t.Errorf("поиск: %v", calls)
	}

	// date_to без времени включает весь сегодняшний день
	today := strings.Split(first["created_at"].(string), "T")[0]
	_, body = get(t, h, "/calls?date_to="+today)
	if body["total"].(float64) != 2 {
		t.Errorf("date_to=%s: total %v", today, body["total"])
	}

	if code, _ := get(t, h, "/calls?date_from=не-дата"); code != 422 {
		t.Errorf("неверная дата: code %d", code)
	}
	if code, _ := get(t, h, "/calls?limit=0"); code != 422 {
		t.Errorf("limit=0: code %d", code)
	}
}

func TestCallEndpoints(t *testing.T) {
	_, h := setupServer(t)

	// Поиск и по call_id, и по mango_recording_id
	for _, id := range []string{"c1", "rec-1"} {
		code, body := get(t, h, "/calls/"+id)
		if code != 200 || body["call_id"] != "c1" {
			t.Fatalf("/calls/%s: %d %v", id, code, body)
		}
		if _, ok := body["id"].(string); !ok {
			t.Errorf("uuid id должен сериализоваться строкой: %#v", body["id"])
		}
	}

	if code, body := get(t, h, "/calls/nope"); code != 404 || body["detail"] == nil {
		t.Errorf("404: %d %v", code, body)
	}

	_, body := get(t, h, "/calls/c1/transcript")
	if len(body["segments"].([]any)) != 1 || body["full_text"] == "" {
		t.Errorf("transcript: %v", body)
	}
	_, body = get(t, h, "/calls/c1/analysis")
	if body["score"].(float64) != 8 || len(body["topics"].([]any)) != 1 || len(body["action_items"].([]any)) != 0 {
		t.Errorf("analysis: %v", body)
	}
	if code, body := get(t, h, "/calls/c2/analysis"); code != 404 || !strings.Contains(body["detail"].(string), "stored") {
		t.Errorf("анализ ещё не готов: %d %v", code, body)
	}
	if _, body := get(t, h, "/calls/c2/status"); body["status"] != "stored" {
		t.Errorf("status: %v", body)
	}
}

func TestAdmin(t *testing.T) {
	_, h := setupServer(t)

	_, body := get(t, h, "/admin/stats")
	calls := body["calls"].(map[string]any)
	if calls["total"].(float64) != 2 || calls["by_status"].(map[string]any)["analyzed"].(float64) != 1 {
		t.Errorf("stats calls: %v", calls)
	}
	if calls["last_24h"].(map[string]any)["downloaded"].(float64) != 1 {
		t.Errorf("last_24h: %v", calls["last_24h"])
	}
	if body["storage"].(map[string]any)["total_size_mb"].(float64) != 3 {
		t.Errorf("storage: %v", body["storage"])
	}
	if body["errors"].(map[string]any)["last_24h"].(float64) != 1 {
		t.Errorf("errors: %v", body["errors"])
	}

	_, body = get(t, h, "/admin/logs?level=error")
	if body["total"].(float64) != 1 || body["logs"].([]any)[0].(map[string]any)["stage"] != "transcription" {
		t.Errorf("logs: %v", body)
	}
}
