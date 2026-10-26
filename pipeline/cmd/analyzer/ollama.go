package main

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

// ── промпт для анализа ──────────────────────────────────────────

const analysisPromptTpl = `Ты — специалист по анализу качества звонков в колл-центре.
Проанализируй транскрипцию телефонного разговора между сотрудником и клиентом.

ИНФОРМАЦИЯ О ЗВОНКЕ:
- Сотрудник: %s (ID: %s)
- Направление: %s
- Длительность: %d сек

ТРАНСКРИПЦИЯ:
%s

Ответь СТРОГО в формате JSON без markdown и лишнего текста:
{
  "summary": "краткое резюме звонка в 2-3 предложения",
  "sentiment": "positive или neutral или negative",
  "script_compliance": true,
  "script_violations": ["список нарушений, пустой массив если нарушений нет"],
  "action_items": ["список действий после звонка"],
  "score": 8,
  "topics": ["ключевые темы разговора"],
  "call_outcome": "sold или callback или rejected или escalated или consultation или other"
}`

// maxTranscriptRunes ограничивает длину транскрипции, чтобы не выйти
// за контекст модели (~6000 символов ≈ ~1500 токенов).
const maxTranscriptRunes = 6000

func buildPrompt(empName, empID, direction string, duration int, transcript string) string {
	if empName == "" {
		empName = "неизвестен"
	}
	if direction == "" {
		direction = "не указано"
	}
	if text, cut := truncateRunes(transcript, maxTranscriptRunes); cut {
		transcript = text + "\n...[текст обрезан]"
	}
	return fmt.Sprintf(analysisPromptTpl, empName, empID, direction, duration, transcript)
}

// truncateRunes обрезает по символам, а не байтам, чтобы не разрезать кириллицу.
func truncateRunes(s string, n int) (string, bool) {
	r := []rune(s)
	if len(r) <= n {
		return s, false
	}
	return string(r[:n]), true
}

// ── результат анализа ───────────────────────────────────────────

type AnalysisResult struct {
	Summary          string   `json:"summary"`
	Sentiment        string   `json:"sentiment"`
	ScriptCompliance bool     `json:"script_compliance"`
	ScriptViolations []string `json:"script_violations"`
	ActionItems      []string `json:"action_items"`
	Score            int      `json:"score"`
	Topics           []string `json:"topics"`
	CallOutcome      string   `json:"call_outcome"`
}

var validOutcomes = map[string]bool{
	"sold": true, "callback": true, "rejected": true,
	"escalated": true, "consultation": true, "other": true,
}

// jsonExtractRe находит блок {...} в ответе (модель может обернуть его в ```json).
var jsonExtractRe = regexp.MustCompile(`(?s)\{.*\}`)

// parseAnalysis разбирает ответ модели и приводит поля к допустимым значениям.
func parseAnalysis(raw string) (*AnalysisResult, error) {
	if match := jsonExtractRe.FindString(raw); match != "" {
		raw = match
	}

	var result AnalysisResult
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return nil, fmt.Errorf("parse analysis json: %w (raw: %.200s)", err, raw)
	}

	switch result.Sentiment {
	case "positive", "neutral", "negative":
	default:
		result.Sentiment = "neutral"
	}
	if result.Score < 1 || result.Score > 10 {
		result.Score = 5
	}
	if !validOutcomes[result.CallOutcome] {
		result.CallOutcome = "other"
	}
	if result.Summary == "" {
		result.Summary = "Анализ выполнен"
	}
	if result.ScriptViolations == nil {
		result.ScriptViolations = []string{}
	}
	if result.ActionItems == nil {
		result.ActionItems = []string{}
	}
	if result.Topics == nil {
		result.Topics = []string{}
	}
	return &result, nil
}

// ── клиент Ollama ───────────────────────────────────────────────

type ollamaClient struct {
	host  string
	model string
	http  *http.Client
}

func (c *ollamaClient) analyze(ctx context.Context, prompt string) (*AnalysisResult, error) {
	reqBody, _ := json.Marshal(map[string]any{
		"model":  c.model,
		"prompt": prompt,
		"stream": false,
		"format": "json",
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.host+"/api/generate", bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read ollama response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("ollama %d: %s", resp.StatusCode, body)
	}

	var ollamaResp struct {
		Response string `json:"response"`
	}
	if err := json.Unmarshal(body, &ollamaResp); err != nil {
		return nil, fmt.Errorf("parse ollama response: %w", err)
	}
	return parseAnalysis(ollamaResp.Response)
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

// ensureModel скачивает модель, если её нет в /api/tags.
func (c *ollamaClient) ensureModel(ctx context.Context) error {
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(checkCtx, http.MethodGet, c.host+"/api/tags", nil)
	resp, err := c.http.Do(req)
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
	if hasModel(installed, c.model) {
		slog.Info("модель уже загружена", "model", c.model)
		return nil
	}

	slog.Info("скачиваю модель (может занять несколько минут)...", "model", c.model)
	pullBody, _ := json.Marshal(map[string]any{"name": c.model, "stream": true})
	pullReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.host+"/api/pull", bytes.NewReader(pullBody))
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
	slog.Info("скачивание модели завершено", "model", c.model)
	return nil
}
