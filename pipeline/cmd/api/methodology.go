package main

// Методики анализа: админ загружает правила разговора, локальная LLM делает
// из них промпт и критерии (черновик), админ правит и применяет. Применённая
// версия используется анализатором для новых звонков; откат — применить
// архивную версию.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"callparser/internal/methodology"
)

const org = methodology.DefaultOrg

// methodologyError переводит ошибки пакета methodology в HTTP-ответ.
func methodologyError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, methodology.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, methodology.ErrNotDraft):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, methodology.ErrInvalid):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	default:
		internalError(w, r, err)
	}
}

func methodologyID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusNotFound, methodology.ErrNotFound.Error())
		return 0, false
	}
	return id, true
}

// readJSON читает тело запроса (не больше 1 МБ) в v.
func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(v); err != nil {
		writeError(w, http.StatusUnprocessableEntity, "Неверный JSON: "+err.Error())
		return false
	}
	return true
}

// POST /admin/methodologies/extract-text — текст из загруженного файла (txt, md, docx, pdf).
func (s *server) extractMethodologyText(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxDocBytes+1<<20)
	file, header, err := r.FormFile("file")
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "Файл больше 20 МБ")
			return
		}
		writeError(w, http.StatusUnprocessableEntity, "Не передан файл (поле file)")
		return
	}
	defer file.Close()
	if r.MultipartForm != nil {
		defer r.MultipartForm.RemoveAll()
	}

	data, err := io.ReadAll(file)
	if err != nil {
		internalError(w, r, err)
		return
	}
	text, err := extractText(header.Filename, data)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"filename": header.Filename, "text": text})
}

// POST /admin/methodologies/generate {source_text} — ИИ составляет черновик.
// Ответ модели занимает от десятков секунд до нескольких минут.
func (s *server) generateMethodology(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SourceText string `json:"source_text"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	if s.generate == nil {
		writeError(w, http.StatusServiceUnavailable, "ИИ-модель не настроена")
		return
	}

	slog.Info("генерирую методику", "chars", len([]rune(req.SourceText)))
	draft, err := s.generate(r.Context(), req.SourceText)
	if err != nil {
		if errors.Is(err, methodology.ErrInvalid) {
			writeError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		slog.Error("генерация методики", "err", err)
		writeError(w, http.StatusBadGateway, "ИИ не смог составить методику: "+err.Error())
		return
	}
	// Запрос мог идти минуты: сохраняем, даже если клиент уже отключился
	saved, err := methodology.CreateDraft(context.WithoutCancel(r.Context()), s.db.DB, org, draft)
	if err != nil {
		methodologyError(w, r, err)
		return
	}
	s.db.Log(r.Context(), "", "info", "methodology",
		fmt.Sprintf("ИИ составил черновик методики v%d: %d критериев", saved.Version, len(saved.Criteria)))
	writeJSON(w, http.StatusOK, saved)
}

// POST /admin/methodologies — черновик вручную (например, копия активной для правки).
func (s *server) createMethodology(w http.ResponseWriter, r *http.Request) {
	var req methodology.Methodology
	if !readJSON(w, r, &req) {
		return
	}
	saved, err := methodology.CreateDraft(r.Context(), s.db.DB, org, &req)
	if err != nil {
		methodologyError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, saved)
}

// GET /admin/methodologies — все версии, новые сверху.
func (s *server) listMethodologies(w http.ResponseWriter, r *http.Request) {
	list, err := methodology.List(r.Context(), s.db.DB, org)
	if err != nil {
		internalError(w, r, err)
		return
	}
	var activeID *int64
	for _, m := range list {
		if m.Status == methodology.StatusActive {
			activeID = &m.ID
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"active_id": activeID, "methodologies": list})
}

// GET /admin/methodologies/{id}
func (s *server) getMethodology(w http.ResponseWriter, r *http.Request) {
	id, ok := methodologyID(w, r)
	if !ok {
		return
	}
	m, err := methodology.Get(r.Context(), s.db.DB, org, id)
	if err != nil {
		methodologyError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, m)
}

// PUT /admin/methodologies/{id} {title, prompt, criteria} — правка черновика.
func (s *server) updateMethodology(w http.ResponseWriter, r *http.Request) {
	id, ok := methodologyID(w, r)
	if !ok {
		return
	}
	var req methodology.Methodology
	if !readJSON(w, r, &req) {
		return
	}
	m, err := methodology.UpdateDraft(r.Context(), s.db.DB, org, id, req.Title, req.Prompt, req.Criteria)
	if err != nil {
		methodologyError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, m)
}

// POST /admin/methodologies/{id}/activate — применить (или откатиться на архивную).
func (s *server) activateMethodology(w http.ResponseWriter, r *http.Request) {
	id, ok := methodologyID(w, r)
	if !ok {
		return
	}
	m, err := methodology.Activate(r.Context(), s.db.DB, org, id)
	if err != nil {
		methodologyError(w, r, err)
		return
	}
	s.db.Log(r.Context(), "", "info", "methodology",
		fmt.Sprintf("Применена методика v%d «%s»", m.Version, m.Title))
	slog.Info("методика применена", "id", m.ID, "version", m.Version)
	writeJSON(w, http.StatusOK, m)
}

// DELETE /admin/methodologies/{id} — удалить черновик.
func (s *server) deleteMethodology(w http.ResponseWriter, r *http.Request) {
	id, ok := methodologyID(w, r)
	if !ok {
		return
	}
	if err := methodology.DeleteDraft(r.Context(), s.db.DB, org, id); err != nil {
		methodologyError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": id})
}
