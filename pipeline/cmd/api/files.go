package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"

	"callparser/internal/queue"
	"callparser/internal/store"
)

// maxUploadBytes совпадает с client_max_body_size в nginx.
const maxUploadBytes = 200 << 20

var audioExtensions = map[string]bool{
	".mp3": true, ".wav": true, ".m4a": true, ".ogg": true,
	".mp4": true, ".aac": true, ".flac": true, ".webm": true,
}

func allowedExtensions() string {
	exts := make([]string, 0, len(audioExtensions))
	for e := range audioExtensions {
		exts = append(exts, e)
	}
	sort.Strings(exts)
	return strings.Join(exts, ", ")
}

// POST /calls/upload — ручная загрузка аудиозаписи (multipart: file + метаданные).
func (s *server) uploadCall(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadBytes+1<<20)
	// Файл больше 32 МБ уходит во временный файл на диске, а не в память
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "Файл больше 200 МБ")
			return
		}
		writeError(w, http.StatusUnprocessableEntity, "Ожидается multipart/form-data с полем file")
		return
	}
	defer r.MultipartForm.RemoveAll()

	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, "Не передан файл (поле file)")
		return
	}
	defer file.Close()

	ext := strings.ToLower(path.Ext(header.Filename))
	contentType := header.Header.Get("Content-Type")
	isAudioCT := strings.Contains(contentType, "audio") || strings.Contains(contentType, "video")
	if !audioExtensions[ext] && !isAudioCT {
		writeError(w, http.StatusBadRequest, "Принимаются аудиофайлы: "+allowedExtensions())
		return
	}
	if header.Size == 0 {
		writeError(w, http.StatusBadRequest, "Файл пустой")
		return
	}
	if !audioExtensions[ext] {
		ext = ".mp3"
	}
	if contentType == "" {
		contentType = "audio/mpeg"
	}

	form := func(name string) any {
		if v := strings.TrimSpace(r.FormValue(name)); v != "" {
			return v
		}
		return nil
	}

	ctx := r.Context()
	callID := uuid.NewString()
	objectKey := fmt.Sprintf("calls/%s/%s%s", time.Now().UTC().Format("2006-01-02"), callID, ext)

	if _, err := s.s3.PutObject(ctx, s.bucket, objectKey, file, header.Size,
		minio.PutObjectOptions{ContentType: contentType}); err != nil {
		internalError(w, r, fmt.Errorf("s3 upload: %w", err))
		return
	}

	_, err = s.db.DB.Exec(ctx,
		`INSERT INTO calls
		 (call_id, source, employee_id, employee_name, phone_from, phone_to,
		  direction, file_path, file_size_bytes, status)
		 VALUES ($1, 'manual', $2, $3, $4, $5, $6, $7, $8, $9)`,
		callID, form("employee_id"), form("employee_name"), form("phone_from"), form("phone_to"),
		form("direction"), objectKey, header.Size, store.StatusStored,
	)
	if err != nil {
		// Без записи в БД файл никто не найдёт — удаляем, чтобы не копить мусор
		if rmErr := s.s3.RemoveObject(context.WithoutCancel(ctx), s.bucket, objectKey, minio.RemoveObjectOptions{}); rmErr != nil {
			slog.Warn("не удалось удалить файл после ошибки БД", "key", objectKey, "err", rmErr)
		}
		internalError(w, r, fmt.Errorf("db insert: %w", err))
		return
	}

	details, _ := json.Marshal(map[string]any{"size": header.Size})
	if _, err := s.db.DB.Exec(ctx,
		`INSERT INTO call_logs (call_id, level, stage, message, details) VALUES ($1, 'info', 'upload', 'Файл загружен вручную', $2)`,
		callID, details); err != nil {
		slog.Warn("не удалось записать лог в БД", "err", err)
	}

	// Если Kafka недоступна, звонок остаётся в 'stored' и транскрибатор подберёт его сам.
	if err := queue.Publish(ctx, s.kc, queue.TopicCallsReady, queue.CallEvent{CallID: callID}); err != nil {
		slog.Warn("не удалось отправить событие в Kafka", "call_id", callID, "err", err)
	}

	slog.Info("загружен вручную", "call_id", callID, "bytes", header.Size)
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "uploaded", "call_id": callID, "file_path": objectKey,
	})
}

// ── аудио ───────────────────────────────────────────────────────

var rangeRe = regexp.MustCompile(`^bytes=(\d*)-(\d*)$`)

// parseRange разбирает один диапазон "bytes=start-end" для файла размера size.
// ok=false — заголовок не поддерживается, файл отдаётся целиком;
// errRange — диапазон за пределами файла (ответ 416).
func parseRange(header string, size int64) (start, end int64, ok bool, err error) {
	m := rangeRe.FindStringSubmatch(strings.TrimSpace(header))
	if m == nil || (m[1] == "" && m[2] == "") || size <= 0 {
		return 0, 0, false, nil
	}
	if m[1] != "" {
		start, _ = strconv.ParseInt(m[1], 10, 64)
		end = size - 1
		if m[2] != "" {
			e, _ := strconv.ParseInt(m[2], 10, 64)
			end = min(e, size-1)
		}
	} else { // bytes=-500 — последние 500 байт
		n, _ := strconv.ParseInt(m[2], 10, 64)
		start, end = max(0, size-n), size-1
	}
	if start > end || start >= size {
		return 0, 0, false, errRange
	}
	return start, end, true, nil
}

var errRange = errors.New("неверный диапазон")

// GET /calls/{call_id}/audio — стриминг аудио из S3 с поддержкой Range (перемотка в плеере).
func (s *server) getAudio(w http.ResponseWriter, r *http.Request) {
	call, ok := s.callOr404(w, r)
	if !ok {
		return
	}
	key, _ := call["file_path"].(string)
	if key == "" {
		writeError(w, http.StatusNotFound, "Аудиофайл ещё не загружен")
		return
	}

	ctx := r.Context()
	stat, err := s.s3.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		writeError(w, http.StatusNotFound, "Файл не найден в хранилище")
		return
	}

	opts := minio.GetObjectOptions{}
	start, end, partial, err := parseRange(r.Header.Get("Range"), stat.Size)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", stat.Size))
		writeError(w, http.StatusRequestedRangeNotSatisfiable, "Неверный диапазон")
		return
	}
	length := stat.Size
	if partial {
		if err := opts.SetRange(start, end); err != nil {
			internalError(w, r, err)
			return
		}
		length = end - start + 1
	}

	obj, err := s.s3.GetObject(ctx, s.bucket, key, opts)
	if err != nil {
		writeError(w, http.StatusNotFound, "Файл не найден в хранилище")
		return
	}
	defer obj.Close()

	ext := path.Ext(key)
	if ext == "" {
		ext = ".mp3"
	}
	contentType := stat.ContentType
	if contentType == "" || contentType == "application/octet-stream" {
		contentType = mime.TypeByExtension(ext)
	}
	if contentType == "" {
		contentType = "audio/mpeg"
	}

	h := w.Header()
	h.Set("Content-Type", contentType)
	h.Set("Content-Length", strconv.FormatInt(length, 10))
	h.Set("Accept-Ranges", "bytes")
	h.Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s%s"`, r.PathValue("call_id"), ext))
	status := http.StatusOK
	if partial {
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, stat.Size))
		status = http.StatusPartialContent
	}
	w.WriteHeader(status)
	if _, err := io.Copy(w, obj); err != nil {
		// Клиент часто обрывает соединение при перемотке — это не ошибка сервера
		slog.Debug("стриминг аудио прерван", "key", key, "err", err)
	}
}
