package main

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ── ответы ──────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Warn("write json", "err", err)
	}
}

// writeError отвечает в формате FastAPI {"detail": "..."}: фронтенд читает поле detail.
func writeError(w http.ResponseWriter, status int, detail string) {
	writeJSON(w, status, map[string]string{"detail": detail})
}

func internalError(w http.ResponseWriter, r *http.Request, err error) {
	slog.Error("ошибка обработки запроса", "method", r.Method, "path", r.URL.Path, "err", err)
	writeError(w, http.StatusInternalServerError, "Внутренняя ошибка сервера")
}

// ── авторизация ─────────────────────────────────────────────────

// requireAuth проверяет API-ключ из заголовка X-API-Key или параметра ?api_key=
// (для ссылок, которые браузер открывает сам: <audio src>, скачивание).
// Пустой apiKey отключает проверку (dev-режим).
func requireAuth(apiKey string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if apiKey != "" {
			provided := r.Header.Get("X-API-Key")
			if provided == "" {
				provided = r.URL.Query().Get("api_key")
			}
			if subtle.ConstantTimeCompare([]byte(provided), []byte(apiKey)) != 1 {
				writeError(w, http.StatusForbidden, "Неверный API ключ")
				return
			}
		}
		next(w, r)
	}
}

// ── журнал запросов ─────────────────────────────────────────────

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Unwrap нужен http.ResponseController (например, для стриминга аудио).
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		if r.URL.Path == "/health" {
			return
		}
		slog.Info("запрос", "method", r.Method, "path", r.URL.Path, "status", rec.status,
			"took", time.Since(start).Round(time.Millisecond))
	})
}

// ── параметры запроса ───────────────────────────────────────────

// intParam читает целое из query с границами. ok=false — ответ об ошибке уже отправлен.
func intParam(w http.ResponseWriter, r *http.Request, name string, def, min, max int) (int, bool) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def, true
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < min || n > max {
		writeError(w, http.StatusUnprocessableEntity,
			"Параметр "+name+" должен быть целым числом от "+strconv.Itoa(min)+" до "+strconv.Itoa(max))
		return 0, false
	}
	return n, true
}

// ── строки БД → JSON ────────────────────────────────────────────

// rowsToMaps превращает строки в map[колонка]значение, как RealDictCursor в Python-версии.
func rowsToMaps(rows pgx.Rows) ([]map[string]any, error) {
	defer rows.Close()
	fields := rows.FieldDescriptions()
	out := []map[string]any{}
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		m := make(map[string]any, len(fields))
		for i, f := range fields {
			m[f.Name] = jsonValue(vals[i])
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func jsonValue(v any) any {
	if b, ok := v.([16]byte); ok { // uuid
		return uuid.UUID(b).String()
	}
	return v
}

// isBadInput — ошибка Postgres из-за некорректного значения от клиента (например, даты).
func isBadInput(err error) bool {
	var pgErr interface{ SQLState() string }
	if errors.As(err, &pgErr) {
		switch pgErr.SQLState() {
		case "22007", "22008", "22P02": // неверный формат даты/времени, неверный текст значения
			return true
		}
	}
	return false
}
