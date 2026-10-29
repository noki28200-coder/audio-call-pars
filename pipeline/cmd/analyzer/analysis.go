package main

import (
	"context"
	"fmt"
	"strings"

	"callparser/internal/llm"
	"callparser/internal/methodology"
)

// maxTranscriptRunes ограничивает длину транскрипции, чтобы не выйти за
// контекст модели: ~24000 символов русского текста ≈ 8–10 тыс. токенов
// (примерно 30 минут разговора), остальное место — промпт и ответ.
const maxTranscriptRunes = 24000

// truncateRunes обрезает по символам, а не байтам, чтобы не разрезать кириллицу.
func truncateRunes(s string, n int) (string, bool) {
	r := []rune(s)
	if len(r) <= n {
		return s, false
	}
	return string(r[:n]), true
}

// AnalysisResult — итог анализа звонка (поля calls.analysis_*).
type AnalysisResult struct {
	Summary          string   `json:"summary"`
	Sentiment        string   `json:"sentiment"`
	ScriptCompliance bool     `json:"script_compliance"`
	ScriptViolations []string `json:"script_violations"`
	ActionItems      []string `json:"action_items"`
	Score            int      `json:"score"`
	Topics           []string `json:"topics"`
	CallOutcome      string   `json:"call_outcome"`

	// Только при анализе по методике
	MethodologyID *int64                        `json:"methodology_id,omitempty"`
	Criteria      []methodology.CriterionResult `json:"criteria,omitempty"`
}

var (
	sentiments = []string{"positive", "neutral", "negative"}
	outcomes   = []string{"sold", "callback", "rejected", "escalated", "consultation", "other"}
)

func strArray() map[string]any {
	return map[string]any{"type": "array", "items": map[string]any{"type": "string"}}
}

// commonProperties — поля ответа, общие для анализа с методикой и без.
func commonProperties() map[string]any {
	return map[string]any{
		"summary":      map[string]any{"type": "string"},
		"sentiment":    map[string]any{"type": "string", "enum": sentiments},
		"action_items": strArray(),
		"topics":       strArray(),
		"call_outcome": map[string]any{"type": "string", "enum": outcomes},
	}
}

var commonRequired = []string{"summary", "sentiment", "action_items", "topics", "call_outcome"}

// normalize приводит поля к допустимым значениям: модель может ошибиться
// даже при ответе по схеме, а массивы nil ушли бы в БД как null.
func (r *AnalysisResult) normalize() {
	if !contains(sentiments, r.Sentiment) {
		r.Sentiment = "neutral"
	}
	if r.Score < 1 || r.Score > 10 {
		r.Score = 5
	}
	if !contains(outcomes, r.CallOutcome) {
		r.CallOutcome = "other"
	}
	if r.Summary == "" {
		r.Summary = "Анализ выполнен"
	}
	if r.ScriptViolations == nil {
		r.ScriptViolations = []string{}
	}
	if r.ActionItems == nil {
		r.ActionItems = []string{}
	}
	if r.Topics == nil {
		r.Topics = []string{}
	}
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// callInfo — метаданные звонка для промпта.
func callInfo(call *callRow) string {
	name := call.EmployeeName
	if name == "" {
		name = "неизвестен"
	}
	direction := map[string]string{"inbound": "входящий", "outbound": "исходящий"}[call.Direction]
	if direction == "" {
		direction = "не указано"
	}
	return fmt.Sprintf("ИНФОРМАЦИЯ О ЗВОНКЕ:\n- Сотрудник: %s\n- Направление: %s\n- Длительность: %d сек\n",
		name, direction, call.Duration)
}

func transcriptBlock(transcript string) string {
	if text, cut := truncateRunes(transcript, maxTranscriptRunes); cut {
		transcript = text + "\n...[текст обрезан]"
	}
	return "РАСШИФРОВКА:\n" + transcript + "\n"
}

const fieldsHelp = `Заполни поля ответа:
- summary: краткое резюме звонка в 2–3 предложения;
- sentiment: настроение клиента — positive, neutral или negative;
- action_items: что нужно сделать после звонка (пустой массив, если ничего);
- topics: ключевые темы разговора;
- call_outcome: итог — sold (продажа), callback (договорились о следующем контакте),
  rejected (отказ), escalated (передан выше), consultation (консультация), other.`

// ── анализ без методики ─────────────────────────────────────────

const defaultSystem = `Ты — специалист по анализу качества звонков в колл-центре.
Проанализируй расшифровку телефонного разговора между сотрудником и клиентом.`

func analyzeDefault(ctx context.Context, model *llm.Client, call *callRow, transcript string) (*AnalysisResult, error) {
	props := commonProperties()
	props["script_compliance"] = map[string]any{"type": "boolean"}
	props["script_violations"] = strArray()
	props["score"] = map[string]any{"type": "integer", "minimum": 1, "maximum": 10}
	schema := map[string]any{
		"type":       "object",
		"properties": props,
		"required":   append([]string{"script_compliance", "script_violations", "score"}, commonRequired...),
	}

	prompt := callInfo(call) + "\n" + transcriptBlock(transcript) + "\n" + fieldsHelp + `
- script_compliance: соблюдал ли сотрудник стандарты вежливого делового общения;
- script_violations: нарушения (пустой массив, если их нет);
- score: общая оценка работы сотрудника от 1 до 10.`

	var r AnalysisResult
	if err := model.GenerateJSON(ctx, defaultSystem, prompt, schema, 0.2, &r); err != nil {
		return nil, err
	}
	r.normalize()
	return &r, nil
}

// ── анализ по методике компании ─────────────────────────────────

var weightLabels = map[int]string{
	methodology.WeightCritical:  "критично",
	methodology.WeightImportant: "важно",
	methodology.WeightOptional:  "желательно",
}

const criteriaHelp = `Для КАЖДОГО критерия из списка верни элемент массива criteria:
- id: идентификатор критерия из квадратных скобок;
- result: passed — выполнен, failed — нарушен, na — не применим к этому звонку
  (ситуация не возникла, например клиент не возражал);
- quote: дословная цитата из расшифровки, подтверждающая решение (пустая строка, если цитаты нет);
- comment: одно предложение — почему такое решение.
Оценивай только по тексту расшифровки, не додумывай.`

func analyzeWithMethodology(ctx context.Context, model *llm.Client, m *methodology.Methodology, call *callRow, transcript string) (*AnalysisResult, error) {
	ids := make([]string, len(m.Criteria))
	var list strings.Builder
	for i, c := range m.Criteria {
		ids[i] = c.ID
		fmt.Fprintf(&list, "[%s] (%s) %s — %s\n", c.ID, weightLabels[c.Weight], c.Name, c.Description)
	}

	props := commonProperties()
	props["criteria"] = map[string]any{
		"type": "array", "minItems": len(ids), "maxItems": len(ids),
		"items": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id":      map[string]any{"type": "string", "enum": ids},
				"result":  map[string]any{"type": "string", "enum": []string{methodology.ResultPassed, methodology.ResultFailed, methodology.ResultNA}},
				"quote":   map[string]any{"type": "string"},
				"comment": map[string]any{"type": "string"},
			},
			"required": []string{"id", "result", "quote", "comment"},
		},
	}
	schema := map[string]any{
		"type":       "object",
		"properties": props,
		"required":   append([]string{"criteria"}, commonRequired...),
	}

	prompt := callInfo(call) + "\nКРИТЕРИИ ПРОВЕРКИ:\n" + list.String() + "\n" +
		transcriptBlock(transcript) + "\n" + criteriaHelp + "\n\n" + fieldsHelp

	var out struct {
		AnalysisResult
		Criteria []methodology.CriterionResult `json:"criteria"`
	}
	if err := model.GenerateJSON(ctx, m.Prompt, prompt, schema, 0.2, &out); err != nil {
		return nil, err
	}

	r := out.AnalysisResult
	r.Criteria = mergeCriteria(m.Criteria, out.Criteria)
	r.Score, r.ScriptCompliance, r.ScriptViolations = methodology.Score(r.Criteria)
	r.MethodologyID = &m.ID
	r.normalize()
	return &r, nil
}

// mergeCriteria сопоставляет ответ модели с критериями методики по id:
// порядок и названия берутся из методики, пропущенные критерии — «не оценён».
func mergeCriteria(criteria []methodology.Criterion, answers []methodology.CriterionResult) []methodology.CriterionResult {
	byID := make(map[string]methodology.CriterionResult, len(answers))
	for _, a := range answers {
		if _, dup := byID[a.ID]; !dup {
			byID[a.ID] = a
		}
	}
	out := make([]methodology.CriterionResult, len(criteria))
	for i, c := range criteria {
		a, ok := byID[c.ID]
		switch {
		case !ok:
			a = methodology.CriterionResult{Result: methodology.ResultNA, Comment: "Модель не оценила критерий"}
		case a.Result != methodology.ResultPassed && a.Result != methodology.ResultFailed:
			a.Result = methodology.ResultNA
		}
		a.ID, a.Name, a.Weight = c.ID, c.Name, c.Weight
		a.Quote = strings.TrimSpace(a.Quote)
		a.Comment = strings.TrimSpace(a.Comment)
		out[i] = a
	}
	return out
}
