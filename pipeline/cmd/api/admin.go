package main

import (
	"math"
	"net/http"
	"time"
)

// GET /admin/stats — агрегированная статистика для дашборда.
func (s *server) getStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var (
		total, downloaded24h, transcribed24h, analyzed24h int64
		totalBytes                                        int64
		lastDownload, lastTranscription, lastAnalysis     *time.Time
	)
	err := s.db.DB.QueryRow(ctx,
		`SELECT COUNT(*),
		        COUNT(*) FILTER (WHERE created_at     >= NOW() - INTERVAL '24 hours'),
		        COUNT(*) FILTER (WHERE transcribed_at >= NOW() - INTERVAL '24 hours'),
		        COUNT(*) FILTER (WHERE analyzed_at    >= NOW() - INTERVAL '24 hours'),
		        COALESCE(SUM(file_size_bytes), 0)::bigint,
		        MAX(created_at), MAX(transcribed_at), MAX(analyzed_at)
		 FROM calls`,
	).Scan(&total, &downloaded24h, &transcribed24h, &analyzed24h,
		&totalBytes, &lastDownload, &lastTranscription, &lastAnalysis)
	if err != nil {
		internalError(w, r, err)
		return
	}

	byStatus := map[string]int64{}
	rows, err := s.db.DB.Query(ctx, `SELECT status, COUNT(*) FROM calls GROUP BY status`)
	if err != nil {
		internalError(w, r, err)
		return
	}
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			rows.Close()
			internalError(w, r, err)
			return
		}
		byStatus[status] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		internalError(w, r, err)
		return
	}

	var errors24h, errors7d int64
	var lastError *time.Time
	err = s.db.DB.QueryRow(ctx,
		`SELECT COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours'),
		        COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days'),
		        MAX(created_at)
		 FROM call_logs WHERE level = 'error'`,
	).Scan(&errors24h, &errors7d, &lastError)
	if err != nil {
		internalError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": time.Now().UTC().Format("2006-01-02T15:04:05.000000Z"),
		"calls": map[string]any{
			"total":     total,
			"by_status": byStatus,
			"last_24h": map[string]any{
				"downloaded":  downloaded24h,
				"transcribed": transcribed24h,
				"analyzed":    analyzed24h,
			},
		},
		"errors": map[string]any{
			"last_24h": errors24h,
			"last_7d":  errors7d,
		},
		"storage": map[string]any{
			"total_size_bytes": totalBytes,
			"total_size_mb":    math.Round(float64(totalBytes)/1024/1024*100) / 100,
		},
		"last_activity": map[string]any{
			"last_download":      lastDownload,
			"last_transcription": lastTranscription,
			"last_analysis":      lastAnalysis,
			"last_error":         lastError,
		},
	})
}

// GET /admin/logs — журнал операций и ошибок.
func (s *server) getLogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, ok := intParam(w, r, "limit", 100, 1, 1000)
	if !ok {
		return
	}
	offset, ok := intParam(w, r, "offset", 0, 0, 1<<31-1)
	if !ok {
		return
	}

	var f filter
	for _, col := range []string{"level", "stage", "call_id"} {
		if v := q.Get(col); v != "" {
			f.add(col+" = ?", v)
		}
	}

	ctx := r.Context()
	var total int64
	if err := s.db.DB.QueryRow(ctx, "SELECT COUNT(*) FROM call_logs "+f.where(), f.args...).Scan(&total); err != nil {
		internalError(w, r, err)
		return
	}
	rows, err := s.db.DB.Query(ctx,
		"SELECT * FROM call_logs "+f.where()+" ORDER BY created_at DESC LIMIT "+f.next(1)+" OFFSET "+f.next(2),
		append(f.args, limit, offset)...,
	)
	if err != nil {
		internalError(w, r, err)
		return
	}
	logs, err := rowsToMaps(rows)
	if err != nil {
		internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total": total, "limit": limit, "offset": offset, "logs": logs,
	})
}
