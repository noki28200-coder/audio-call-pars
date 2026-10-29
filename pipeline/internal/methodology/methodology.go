// Пакет methodology — методики анализа звонков.
//
// Админ компании загружает свои правила разговора с клиентом, локальная LLM
// превращает их в промпт для анализатора и список проверяемых критериев.
// Версии: draft (можно править) → active (ровно одна на организацию) →
// archived (прежние активные; на любую можно откатиться). Активные и архивные
// версии не меняются: проанализированные звонки ссылаются на них.
package methodology

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"callparser/internal/llm"
)

const (
	StatusDraft    = "draft"
	StatusActive   = "active"
	StatusArchived = "archived"

	// DefaultOrg — пока одна компания на установку; поле нужно на будущее.
	DefaultOrg = "default"
)

// Веса критериев.
const (
	WeightOptional  = 1 // желательно
	WeightImportant = 2 // важно
	WeightCritical  = 3 // критично: провал ограничивает оценку
)

type Criterion struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Weight      int    `json:"weight"`
}

type Methodology struct {
	ID          int64       `json:"id"`
	Version     int         `json:"version"`
	Title       string      `json:"title"`
	SourceText  string      `json:"source_text"`
	Prompt      string      `json:"prompt"`
	Criteria    []Criterion `json:"criteria"`
	Status      string      `json:"status"`
	CreatedAt   time.Time   `json:"created_at"`
	ActivatedAt *time.Time  `json:"activated_at"`
}

var (
	ErrNotFound = errors.New("методика не найдена")
	ErrNotDraft = errors.New("менять и удалять можно только черновик")
)

// ── хранение ────────────────────────────────────────────────────

const selectCols = `id, version, title, source_text, prompt, criteria, status, created_at, activated_at`

func scan(row pgx.Row) (*Methodology, error) {
	m := &Methodology{}
	err := row.Scan(&m.ID, &m.Version, &m.Title, &m.SourceText, &m.Prompt, &m.Criteria,
		&m.Status, &m.CreatedAt, &m.ActivatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return m, err
}

// Active возвращает активную методику организации или nil, если её нет.
func Active(ctx context.Context, db *pgxpool.Pool, org string) (*Methodology, error) {
	m, err := scan(db.QueryRow(ctx,
		`SELECT `+selectCols+` FROM methodologies WHERE organization_id = $1 AND status = 'active'`, org))
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	return m, err
}

func Get(ctx context.Context, db *pgxpool.Pool, org string, id int64) (*Methodology, error) {
	return scan(db.QueryRow(ctx,
		`SELECT `+selectCols+` FROM methodologies WHERE organization_id = $1 AND id = $2`, org, id))
}

// List возвращает все версии, новые сверху.
func List(ctx context.Context, db *pgxpool.Pool, org string) ([]*Methodology, error) {
	rows, err := db.Query(ctx,
		`SELECT `+selectCols+` FROM methodologies WHERE organization_id = $1 ORDER BY version DESC`, org)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*Methodology{}
	for rows.Next() {
		m, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// CreateDraft сохраняет новую версию-черновик со следующим номером.
func CreateDraft(ctx context.Context, db *pgxpool.Pool, org string, m *Methodology) (*Methodology, error) {
	if err := Validate(m); err != nil {
		return nil, err
	}
	return scan(db.QueryRow(ctx,
		`INSERT INTO methodologies (organization_id, version, title, source_text, prompt, criteria, status)
		 VALUES ($1, (SELECT COALESCE(MAX(version), 0) + 1 FROM methodologies WHERE organization_id = $1),
		         $2, $3, $4, $5, 'draft')
		 RETURNING `+selectCols,
		org, m.Title, m.SourceText, m.Prompt, m.Criteria))
}

// UpdateDraft меняет название, промпт и критерии черновика.
func UpdateDraft(ctx context.Context, db *pgxpool.Pool, org string, id int64, title, prompt string, criteria []Criterion) (*Methodology, error) {
	m := &Methodology{Title: title, Prompt: prompt, Criteria: criteria}
	if err := Validate(m); err != nil {
		return nil, err
	}
	updated, err := scan(db.QueryRow(ctx,
		`UPDATE methodologies SET title = $3, prompt = $4, criteria = $5
		 WHERE organization_id = $1 AND id = $2 AND status = 'draft'
		 RETURNING `+selectCols,
		org, id, m.Title, m.Prompt, m.Criteria))
	if errors.Is(err, ErrNotFound) {
		return nil, notDraftOrMissing(ctx, db, org, id)
	}
	return updated, err
}

func DeleteDraft(ctx context.Context, db *pgxpool.Pool, org string, id int64) error {
	tag, err := db.Exec(ctx,
		`DELETE FROM methodologies WHERE organization_id = $1 AND id = $2 AND status = 'draft'`, org, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return notDraftOrMissing(ctx, db, org, id)
	}
	return nil
}

func notDraftOrMissing(ctx context.Context, db *pgxpool.Pool, org string, id int64) error {
	if _, err := Get(ctx, db, org, id); err != nil {
		return err
	}
	return ErrNotDraft
}

// Activate делает версию активной, прежняя активная уходит в архив.
// Так же работает откат: активировать архивную версию.
func Activate(ctx context.Context, db *pgxpool.Pool, org string, id int64) (*Methodology, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	m, err := scan(tx.QueryRow(ctx,
		`SELECT `+selectCols+` FROM methodologies WHERE organization_id = $1 AND id = $2 FOR UPDATE`, org, id))
	if err != nil {
		return nil, err
	}
	if m.Status == StatusActive {
		return m, nil
	}
	if err := Validate(m); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE methodologies SET status = 'archived' WHERE organization_id = $1 AND status = 'active'`, org); err != nil {
		return nil, err
	}
	m, err = scan(tx.QueryRow(ctx,
		`UPDATE methodologies SET status = 'active', activated_at = NOW() WHERE id = $1 RETURNING `+selectCols, id))
	if err != nil {
		return nil, err
	}
	return m, tx.Commit(ctx)
}

// Validate проверяет методику и нормализует критерии: id по порядку, вес 1..3.
func Validate(m *Methodology) error {
	m.Title = strings.TrimSpace(m.Title)
	m.Prompt = strings.TrimSpace(m.Prompt)
	if m.Prompt == "" {
		return errors.New("промпт не может быть пустым")
	}
	criteria := make([]Criterion, 0, len(m.Criteria))
	for _, c := range m.Criteria {
		c.Name = strings.TrimSpace(c.Name)
		c.Description = strings.TrimSpace(c.Description)
		if c.Name == "" {
			continue
		}
		c.Weight = min(max(c.Weight, WeightOptional), WeightCritical)
		c.ID = fmt.Sprintf("c%d", len(criteria)+1)
		criteria = append(criteria, c)
	}
	if len(criteria) == 0 {
		return errors.New("нужен хотя бы один критерий")
	}
	if len(criteria) > MaxCriteria {
		return fmt.Errorf("слишком много критериев: %d (максимум %d)", len(criteria), MaxCriteria)
	}
	m.Criteria = criteria
	return nil
}

// MaxCriteria — больше критериев локальная модель проверяет ненадёжно.
const MaxCriteria = 25

// ── генерация из текста админа ──────────────────────────────────

// MaxSourceRunes — сколько текста методики отдаём модели (~10 тыс. токенов).
const MaxSourceRunes = 30000

const generateSystem = `Ты — эксперт по контролю качества телефонных разговоров отдела продаж и поддержки.
Админ компании прислал методику (свод правил) ведения разговора с клиентом.
Твоя задача — превратить её в настройку для ИИ-аналитика, который будет проверять расшифровки звонков.

Верни:
1. "title" — короткое название методики (до 60 символов).
2. "prompt" — инструкция для ИИ-аналитика на русском языке, от второго лица («Ты — ...»):
   роль аналитика, чем занимается компания и что она продаёт, цель разговора по методике,
   ключевые техники и запреты, как трактовать спорные случаи. 150–400 слов.
   НЕ описывай формат ответа и не перечисляй критерии — это добавит система.
3. "criteria" — от 5 до 20 критериев, которые можно проверить ТОЛЬКО по тексту расшифровки:
   - "name": короткая формулировка (до 80 символов), например «Назвал своё имя и компанию».
   - "description": как понять, что критерий выполнен: что именно должен сказать или сделать
     сотрудник, примеры фраз; когда критерий не применим.
   - "weight": 3 — критично (обязательные пункты и все запреты), 2 — важно, 1 — желательно.
     Опирайся на то, что сама методика называет главным.
   Запреты формулируй как выполненный критерий при отсутствии нарушения,
   например «Не перебивает клиента», «Не обещает несогласованных скидок».
   Не дублируй критерии и не придумывай правил, которых нет в методике.`

var generateSchema = map[string]any{
	"type": "object",
	"properties": map[string]any{
		"title":  map[string]any{"type": "string"},
		"prompt": map[string]any{"type": "string"},
		"criteria": map[string]any{
			"type": "array", "minItems": 1, "maxItems": MaxCriteria,
			"items": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name":        map[string]any{"type": "string"},
					"description": map[string]any{"type": "string"},
					"weight":      map[string]any{"type": "integer", "enum": []int{1, 2, 3}},
				},
				"required": []string{"name", "description", "weight"},
			},
		},
	},
	"required": []string{"title", "prompt", "criteria"},
}

// Generate просит модель составить промпт и критерии по тексту методики.
// Результат не сохраняется — это делает вызывающий код (как черновик).
func Generate(ctx context.Context, model *llm.Client, sourceText string) (*Methodology, error) {
	sourceText = strings.TrimSpace(sourceText)
	if sourceText == "" {
		return nil, errors.New("текст методики пустой")
	}
	text := sourceText
	if r := []rune(text); len(r) > MaxSourceRunes {
		text = string(r[:MaxSourceRunes]) + "\n...[текст обрезан]"
	}

	var out Methodology
	if err := model.GenerateJSON(ctx, generateSystem, "МЕТОДИКА КОМПАНИИ:\n\n"+text, generateSchema, 0.3, &out); err != nil {
		return nil, err
	}
	out.SourceText = sourceText
	if err := Validate(&out); err != nil {
		return nil, fmt.Errorf("модель вернула неполный результат: %w", err)
	}
	return &out, nil
}

// ── оценка звонка по критериям ──────────────────────────────────

// Результаты проверки одного критерия.
const (
	ResultPassed = "passed"
	ResultFailed = "failed"
	ResultNA     = "na"
)

// CriterionResult — результат проверки критерия в звонке. Название и вес
// копируются из методики, чтобы карточка звонка читалась без неё.
type CriterionResult struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Weight  int    `json:"weight"`
	Result  string `json:"result"`
	Quote   string `json:"quote"`
	Comment string `json:"comment"`
}

// criticalCap — максимальная оценка, если провален критичный критерий.
const criticalCap = 4

// Score считает оценку 1..10 как долю выполненных критериев с учётом весов
// (неприменимые не считаются). Провал критичного критерия ограничивает оценку
// значением criticalCap. Также возвращает названия проваленных критериев.
func Score(results []CriterionResult) (score int, compliant bool, violations []string) {
	var passed, total int
	criticalFailed := false
	violations = []string{}
	for _, r := range results {
		switch r.Result {
		case ResultPassed:
			passed += r.Weight
			total += r.Weight
		case ResultFailed:
			total += r.Weight
			violations = append(violations, r.Name)
			if r.Weight >= WeightCritical {
				criticalFailed = true
			}
		}
	}
	if total == 0 {
		return 5, true, violations // проверять было нечего
	}
	score = 1 + (9*passed+total/2)/total // округление до ближайшего
	if criticalFailed {
		score = min(score, criticalCap)
	}
	return score, len(violations) == 0, violations
}
