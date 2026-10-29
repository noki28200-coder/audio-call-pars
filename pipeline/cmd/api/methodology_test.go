package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"golang.org/x/text/encoding/charmap"

	"callparser/internal/methodology"
)

// ── извлечение текста ───────────────────────────────────────────

func makeDocx(t *testing.T, documentXML string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	f, _ := zw.Create("word/document.xml")
	f.Write([]byte(documentXML))
	zw.Close()
	return buf.Bytes()
}

func TestExtractText(t *testing.T) {
	docx := makeDocx(t, `<?xml version="1.0"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body>
<w:p><w:r><w:t>1. Поздороваться</w:t></w:r></w:p>
<w:p><w:r><w:t xml:space="preserve">2. Назвать </w:t></w:r><w:r><w:t>компанию</w:t></w:r></w:p>
</w:body></w:document>`)
	if got, err := extractText("Методика.DOCX", docx); err != nil || got != "1. Поздороваться\n2. Назвать компанию" {
		t.Fatalf("docx: %q %v", got, err)
	}

	cp1251, _ := charmap.Windows1251.NewEncoder().Bytes([]byte("Не перебивать клиента"))
	if got, _ := extractText("rules.txt", cp1251); got != "Не перебивать клиента" {
		t.Fatalf("txt cp1251: %q", got)
	}
	if got, _ := extractText("rules.md", []byte("\xef\xbb\xbfПравила\r\n\r\n\r\n\r\nКонец  \n")); got != "Правила\n\nКонец" {
		t.Fatalf("md: %q", got)
	}

	if _, err := extractText("rules.xlsx", []byte("x")); err == nil {
		t.Error("xlsx не поддерживается")
	}
	if _, err := extractText("broken.pdf", []byte("не pdf")); err == nil {
		t.Error("битый pdf должен давать ошибку, а не панику")
	}
	if _, err := extractText("empty.txt", []byte("   \n")); err == nil {
		t.Error("пустой файл — ошибка")
	}
}

func TestExtractEndpoint(t *testing.T) {
	s := &server{apiKey: "k"}
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, _ := mw.CreateFormFile("file", "rules.txt")
	fw.Write([]byte("Правило 1"))
	mw.Close()

	r := httptest.NewRequest(http.MethodPost, "/admin/methodologies/extract-text", &body)
	r.Header.Set("Content-Type", mw.FormDataContentType())
	r.Header.Set("X-API-Key", "k")
	w := httptest.NewRecorder()
	s.routes().ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "Правило 1") {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
}

// ── жизненный цикл методики ─────────────────────────────────────

func send(t *testing.T, h http.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var rd *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	} else {
		rd = bytes.NewReader(nil)
	}
	r := httptest.NewRequest(method, path, rd)
	r.Header.Set("X-API-Key", "k")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("%s %s: невалидный JSON: %s", method, path, w.Body.String())
	}
	return w.Code, out
}

func idPath(m map[string]any, suffix string) string {
	return fmt.Sprintf("/admin/methodologies/%d%s", int64(m["id"].(float64)), suffix)
}

func TestMethodologyLifecycle(t *testing.T) {
	s, _ := setupServer(t)
	s.generate = func(ctx context.Context, text string) (*methodology.Methodology, error) {
		return &methodology.Methodology{
			Title: "Продажи", SourceText: text, Prompt: "Ты — аналитик",
			Criteria: []methodology.Criterion{{Name: "Представился", Description: "имя и компания", Weight: 3}},
		}, nil
	}
	h := s.routes()

	_, list := send(t, h, "GET", "/admin/methodologies", nil)
	if list["active_id"] != nil || len(list["methodologies"].([]any)) != 0 {
		t.Fatalf("сначала методик нет: %v", list)
	}

	code, v1 := send(t, h, "POST", "/admin/methodologies/generate", map[string]string{"source_text": "Правила"})
	if code != 200 || v1["status"] != "draft" || v1["version"].(float64) != 1 || v1["source_text"] != "Правила" {
		t.Fatalf("generate: %d %v", code, v1)
	}

	// Правка черновика: id критериев проставляются заново
	code, v1 = send(t, h, "PUT", idPath(v1, ""), map[string]any{
		"title": "Продажи v1", "prompt": "Ты — строгий аналитик",
		"criteria": []map[string]any{{"name": "Представился", "weight": 3}, {"name": "Не перебивал", "weight": 2}},
	})
	if code != 200 || v1["title"] != "Продажи v1" || len(v1["criteria"].([]any)) != 2 {
		t.Fatalf("update: %d %v", code, v1)
	}
	if code, _ := send(t, h, "PUT", idPath(v1, ""), map[string]any{"prompt": "", "criteria": []any{}}); code != 422 {
		t.Errorf("пустой промпт: %d", code)
	}

	code, v1 = send(t, h, "POST", idPath(v1, "/activate"), nil)
	if code != 200 || v1["status"] != "active" || v1["activated_at"] == nil {
		t.Fatalf("activate: %d %v", code, v1)
	}
	if code, _ := send(t, h, "PUT", idPath(v1, ""), map[string]any{"prompt": "x", "criteria": []map[string]any{{"name": "a"}}}); code != 409 {
		t.Errorf("активную править нельзя: %d", code)
	}
	if code, _ := send(t, h, "DELETE", idPath(v1, ""), nil); code != 409 {
		t.Errorf("активную удалять нельзя: %d", code)
	}

	// Вторая версия вручную → применяем → v1 в архиве
	_, v2 := send(t, h, "POST", "/admin/methodologies", map[string]any{
		"title": "Продажи v2", "source_text": "Правила 2", "prompt": "Ты — аналитик",
		"criteria": []map[string]any{{"name": "Назначил шаг", "weight": 2}},
	})
	if v2["version"].(float64) != 2 {
		t.Fatalf("v2: %v", v2)
	}
	send(t, h, "POST", idPath(v2, "/activate"), nil)
	_, list = send(t, h, "GET", "/admin/methodologies", nil)
	items := list["methodologies"].([]any)
	if list["active_id"] != v2["id"] || items[0].(map[string]any)["status"] != "active" || items[1].(map[string]any)["status"] != "archived" {
		t.Fatalf("после применения v2: %v", list)
	}

	// Откат на v1
	send(t, h, "POST", idPath(v1, "/activate"), nil)
	if active, _ := methodology.Active(context.Background(), s.db.DB, org); active == nil || active.Version != 1 {
		t.Fatalf("откат: %+v", active)
	}

	// Черновик можно удалить
	_, v3 := send(t, h, "POST", "/admin/methodologies/generate", map[string]string{"source_text": "Ещё"})
	if code, _ := send(t, h, "DELETE", idPath(v3, ""), nil); code != 200 {
		t.Errorf("удаление черновика: %d", code)
	}
	if code, _ := send(t, h, "GET", idPath(v3, ""), nil); code != 404 {
		t.Errorf("удалённый: %d", code)
	}
	if code, _ := send(t, h, "GET", "/admin/methodologies/abc", nil); code != 404 {
		t.Errorf("нечисловой id: %d", code)
	}
}

func TestAnalysisWithCriteria(t *testing.T) {
	s, h := setupServer(t)
	ctx := context.Background()
	m, err := methodology.CreateDraft(ctx, s.db.DB, org, &methodology.Methodology{
		Title: "Продажи", SourceText: "x", Prompt: "p",
		Criteria: []methodology.Criterion{{Name: "Представился", Weight: 3}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.DB.Exec(ctx,
		`UPDATE calls SET analysis_methodology_id = $1,
		        analysis_criteria = '[{"id":"c1","name":"Представился","weight":3,"result":"passed","quote":"Анна","comment":""}]'
		 WHERE call_id = 'c1'`, m.ID); err != nil {
		t.Fatal(err)
	}
	_, body := get(t, h, "/calls/c1/analysis")
	ref := body["methodology"].(map[string]any)
	if ref["version"].(float64) != 1 || ref["title"] != "Продажи" || len(body["criteria"].([]any)) != 1 {
		t.Fatalf("analysis: %v", body)
	}
}
