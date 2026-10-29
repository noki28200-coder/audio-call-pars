// Пакет llm — клиент локальной языковой модели (Ollama) с ответом по JSON-схеме.
package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// ContextTokens — размер контекста, который просим у Ollama (по умолчанию
// она выделяет меньше и молча обрезает начало промпта).
const ContextTokens = 16384

type Client struct {
	Host  string
	Model string
	HTTP  *http.Client
}

func New(host, model string, timeout time.Duration) *Client {
	return &Client{Host: strings.TrimRight(host, "/"), Model: model, HTTP: &http.Client{Timeout: timeout}}
}

// jsonExtractRe находит блок {...} в ответе (модель может обернуть его в ```json).
var jsonExtractRe = regexp.MustCompile(`(?s)\{.*\}`)

// ExtractJSON вырезает JSON-объект из ответа модели.
func ExtractJSON(raw string) string {
	if match := jsonExtractRe.FindString(raw); match != "" {
		return match
	}
	return raw
}

// GenerateJSON отправляет system- и user-сообщения и разбирает ответ в out.
// schema — JSON Schema ответа: Ollama ограничивает генерацию этой схемой,
// поэтому модель не может вернуть лишний текст или пропустить поле.
func (c *Client) GenerateJSON(ctx context.Context, system, prompt string, schema any, temperature float64, out any) error {
	messages := []map[string]string{}
	if system != "" {
		messages = append(messages, map[string]string{"role": "system", "content": system})
	}
	messages = append(messages, map[string]string{"role": "user", "content": prompt})

	reqBody, _ := json.Marshal(map[string]any{
		"model":    c.Model,
		"messages": messages,
		"stream":   false,
		"format":   schema,
		// Режим «размышлений» (qwen3.5 и др.) только замедляет ответ и мешает JSON
		"think":   false,
		"options": map[string]any{"num_ctx": ContextTokens, "temperature": temperature},
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Host+"/api/chat", bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read ollama response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("ollama %d: %s", resp.StatusCode, body)
	}

	var chat struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal(body, &chat); err != nil {
		return fmt.Errorf("parse ollama response: %w", err)
	}
	raw := ExtractJSON(chat.Message.Content)
	if err := json.Unmarshal([]byte(raw), out); err != nil {
		return fmt.Errorf("parse model json: %w (raw: %.200s)", err, raw)
	}
	return nil
}

// normalizeModel дописывает тег :latest, как это делает сам Ollama.
func normalizeModel(name string) string {
	if !strings.Contains(name, ":") {
		return name + ":latest"
	}
	return name
}

// hasModel проверяет, что именно эта модель (с тегом) уже скачана.
func hasModel(installed []string, model string) bool {
	want := normalizeModel(model)
	for _, name := range installed {
		if normalizeModel(name) == want {
			return true
		}
	}
	return false
}

// EnsureModel скачивает модель, если её нет в /api/tags.
func (c *Client) EnsureModel(ctx context.Context) error {
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(checkCtx, http.MethodGet, c.Host+"/api/tags", nil)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("ollama tags: %w", err)
	}
	var tags struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	err = json.NewDecoder(resp.Body).Decode(&tags)
	resp.Body.Close()
	if err != nil {
		return fmt.Errorf("parse ollama tags: %w", err)
	}
	installed := make([]string, 0, len(tags.Models))
	for _, m := range tags.Models {
		installed = append(installed, m.Name)
	}
	if hasModel(installed, c.Model) {
		slog.Info("модель уже загружена", "model", c.Model)
		return nil
	}

	slog.Info("скачиваю модель (может занять несколько минут)...", "model", c.Model)
	pullBody, _ := json.Marshal(map[string]any{"name": c.Model, "stream": true})
	pullReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.Host+"/api/pull", bytes.NewReader(pullBody))
	pullReq.Header.Set("Content-Type", "application/json")

	pullResp, err := (&http.Client{Timeout: time.Hour}).Do(pullReq)
	if err != nil {
		return fmt.Errorf("ollama pull: %w", err)
	}
	defer pullResp.Body.Close()
	if pullResp.StatusCode >= 400 {
		body, _ := io.ReadAll(pullResp.Body)
		return fmt.Errorf("ollama pull %d: %s", pullResp.StatusCode, body)
	}

	dec := json.NewDecoder(pullResp.Body)
	lastStatus := ""
	for {
		var line struct {
			Status string `json:"status"`
			Error  string `json:"error"`
		}
		if err := dec.Decode(&line); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("ollama pull stream: %w", err)
		}
		if line.Error != "" {
			return fmt.Errorf("ollama pull: %s", line.Error)
		}
		// Прогресс скачивания приходит сотнями строк с одним статусом — пишем только смену.
		if line.Status != lastStatus {
			slog.Info("ollama pull", "status", line.Status)
			lastStatus = line.Status
		}
	}
	if lastStatus != "success" {
		return fmt.Errorf("ollama pull оборвался на статусе %q", lastStatus)
	}
	slog.Info("скачивание модели завершено", "model", c.Model)
	return nil
}
