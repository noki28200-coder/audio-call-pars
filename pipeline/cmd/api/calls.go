package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"unicode/utf8"
)

var errCallNotFound = errors.New("звонок не найден")

// findCall ищет звонок по call_id или mango_recording_id.
func (s *server) findCall(ctx context.Context, callID string) (map[string]any, error) {
	rows, err := s.db.DB.Query(ctx,
		`SELECT * FROM calls WHERE call_id = $1 OR mango_recording_id = $1 LIMIT 1`, callID)
	if err != nil {
		return nil, err
	}
	calls, err := rowsToMaps(rows)
	if err != nil {
		return nil, err
	}
	if len(calls) == 0 {
		return nil, errCallNotFound
	}
	return calls[0], nil
}

// callOr404 — findCall с ответом об ошибке. ok=false — ответ уже отправлен.
func (s *server) callOr404(w http.ResponseWriter, r *http.Request) (map[string]any, bool) {
	callID := r.PathValue("call_id")
	call, err := s.findCall(r.Context(), callID)
	if errors.Is(err, errCallNotFound) {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Звонок '%s' не найден", callID))
		return nil, false
	}
	if err != nil {
		internalError(w, r, err)
		return nil, false
	}
	return call, true
}

// orEmpty заменяет NULL из JSONB-колонки на пустой массив.
func orEmpty(v any) any {
	if v == nil {
		return []any{}
	}
	return v
}

// GET /calls/{call_id}
func (s *server) getCall(w http.ResponseWriter, r *http.Request) {
	if call, ok := s.callOr404(w, r); ok {
		writeJSON(w, http.StatusOK, call)
	}
}

// GET /calls/{call_id}/status
func (s *server) getStatus(w http.ResponseWriter, r *http.Request) {
	call, ok := s.callOr404(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"call_id":            call["call_id"],
		"mango_recording_id": call["mango_recording_id"],
		"status":             call["status"],
		"created_at":         call["created_at"],
		"transcribed_at":     call["transcribed_at"],
		"analyzed_at":        call["analyzed_at"],
	})
}

// GET /calls/{call_id}/transcript
func (s *server) getTranscript(w http.ResponseWriter, r *http.Request) {
	call, ok := s.callOr404(w, r)
	if !ok {
		return
	}
	if text, _ := call["transcript_text"].(string); text == "" {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Транскрипция недоступна. Текущий статус: %v", call["status"]))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"call_id":              call["call_id"],
		"language":             call["transcript_language"],
		"language_probability": call["transcript_language_probability"],
		"full_text":            call["transcript_text"],
		"segments":             orEmpty(call["transcript_segments"]),
		"transcribed_at":       call["transcribed_at"],
	})
}

// GET /calls/{call_id}/analysis
func (s *server) getAnalysis(w http.ResponseWriter, r *http.Request) {
	call, ok := s.callOr404(w, r)
	if !ok {
		return
	}
	if summary, _ := call["analysis_summary"].(string); summary == "" {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Анализ недоступен. Текущий статус: %v", call["status"]))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"call_id":           call["call_id"],
		"summary":           call["analysis_summary"],
		"sentiment":         call["analysis_sentiment"],
		"script_compliance": call["analysis_script_compliance"],
		"script_violations": orEmpty(call["analysis_script_violations"]),
		"action_items":      orEmpty(call["analysis_action_items"]),
		"score":             call["analysis_score"],
		"topics":            orEmpty(call["analysis_topics"]),
		"call_outcome":      call["analysis_call_outcome"],
		"analyzed_at":       call["analyzed_at"],
		"metadata": map[string]any{
			"employee_id":      call["employee_id"],
			"employee_name":    call["employee_name"],
			"duration_seconds": call["duration_seconds"],
			"direction":        call["direction"],
		},
	})
}

// ── список звонков ──────────────────────────────────────────────

// filter собирает WHERE из условий с плейсхолдерами $N.
type filter struct {
	conds []string
	args  []any
}

// add добавляет условие; "?" в cond заменяются на очередные $N.
func (f *filter) add(cond string, args ...any) {
	for _, a := range args {
		f.args = append(f.args, a)
		cond = strings.Replace(cond, "?", fmt.Sprintf("$%d", len(f.args)), 1)
	}
	f.conds = append(f.conds, cond)
}

func (f *filter) where() string {
	if len(f.conds) == 0 {
		return ""
	}
	return "WHERE " + strings.Join(f.conds, " AND ")
}

// next — номер следующего плейсхолдера (для LIMIT/OFFSET).
func (f *filter) next(n int) string { return fmt.Sprintf("$%d", len(f.args)+n) }

// GET /calls
func (s *server) listCalls(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, ok := intParam(w, r, "limit", 50, 1, 500)
	if !ok {
		return
	}
	offset, ok := intParam(w, r, "offset", 0, 0, 1<<31-1)
	if !ok {
		return
	}

	var f filter
	for _, col := range []string{"status", "source", "employee_id"} {
		if v := q.Get(col); v != "" {
			f.add(col+" = ?", v)
		}
	}
	if v := q.Get("date_from"); v != "" {
		f.add("created_at >= ?::text::timestamptz", v)
	}
	if v := q.Get("date_to"); v != "" {
		if len(v) == len("2006-01-02") {
			// Дата без времени включает весь день
			f.add("created_at < ?::text::date + 1", v)
		} else {
			f.add("created_at <= ?::text::timestamptz", v)
		}
	}
	search := q.Get("q")
	if search != "" {
		p := "%" + search + "%"
		f.add("(transcript_text ILIKE ? OR analysis_summary ILIKE ? OR employee_name ILIKE ? OR phone_from LIKE ? OR phone_to LIKE ?)",
			p, p, p, p, p)
	}

	ctx := r.Context()
	var total int
	if err := s.db.DB.QueryRow(ctx, "SELECT COUNT(*) FROM calls "+f.where(), f.args...).Scan(&total); err != nil {
		if isBadInput(err) {
			writeError(w, http.StatusUnprocessableEntity, "Неверный формат даты: используйте ISO 8601, например 2024-01-31")
			return
		}
		internalError(w, r, err)
		return
	}

	extra := ""
	if search != "" {
		extra = ", transcript_text, analysis_summary"
	}
	rows, err := s.db.DB.Query(ctx,
		`SELECT call_id, mango_recording_id, source, employee_id, employee_name,
		        phone_from, phone_to, direction, duration_seconds, start_time,
		        file_size_bytes, status, transcribed_at, analyzed_at,
		        analysis_sentiment, analysis_score, analysis_call_outcome,
		        created_at`+extra+`
		 FROM calls `+f.where()+`
		 ORDER BY created_at DESC
		 LIMIT `+f.next(1)+` OFFSET `+f.next(2),
		append(f.args, limit, offset)...,
	)
	if err != nil {
		internalError(w, r, err)
		return
	}
	calls, err := rowsToMaps(rows)
	if err != nil {
		internalError(w, r, err)
		return
	}

	if search != "" {
		for _, c := range calls {
			var snip any
			for _, field := range []string{"transcript_text", "analysis_summary"} {
				text, _ := c[field].(string)
				delete(c, field)
				if snip == nil {
					if sn, ok := snippet(text, search, 60); ok {
						snip = sn
					}
				}
			}
			c["transcript_snippet"] = snip
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"total": total, "limit": limit, "offset": offset, "calls": calls,
	})
}

// snippet возвращает фрагмент text вокруг первого вхождения query (без учёта регистра)
// с context символами по бокам и многоточиями, если текст обрезан.
func snippet(text, query string, context int) (string, bool) {
	runes := []rune(text)
	lower := []rune(strings.ToLower(text))
	q := []rune(strings.ToLower(query))
	if len(lower) != len(runes) || len(q) == 0 {
		return "", false // смена регистра изменила длину — не рискуем сдвигом позиций
	}
	pos := indexRunes(lower, q)
	if pos < 0 {
		return "", false
	}
	start := max(0, pos-context)
	end := min(len(runes), pos+utf8.RuneCountInString(query)+context)
	out := string(runes[start:end])
	if start > 0 {
		out = "…" + out
	}
	if end < len(runes) {
		out += "…"
	}
	return out, true
}

func indexRunes(s, sub []rune) int {
outer:
	for i := 0; i+len(sub) <= len(s); i++ {
		for j := range sub {
			if s[i+j] != sub[j] {
				continue outer
			}
		}
		return i
	}
	return -1
}
