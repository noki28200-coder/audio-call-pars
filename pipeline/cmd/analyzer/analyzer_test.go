package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"callparser/internal/llm"
	"callparser/internal/methodology"
)

func TestSplitSentences(t *testing.T) {
	got := splitSentences([]segment{
		{Start: 0, End: 10, Text: "Как к вам обращаться? Сергей."},
		{Start: 10, End: 12, Text: "Очень приятно"},
	})
	want := []segment{
		{Start: 0, End: 7.241, Text: "Как к вам обращаться?"},
		{Start: 7.241, End: 10, Text: "Сергей."}, // последнее кончается с фрагментом
		{Start: 10, End: 12, Text: "Очень приятно"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v", got)
	}
}

func TestMergeTurns(t *testing.T) {
	got := mergeTurns([]segment{
		{Start: 0, End: 1, Text: "Добрый день.", Speaker: SpeakerEmployee},
		{Start: 1, End: 2, Text: "Как вас зовут?", Speaker: SpeakerEmployee},
		{Start: 2, End: 3, Text: "Сергей.", Speaker: SpeakerClient},
	})
	if len(got) != 2 || got[0].Text != "Добрый день. Как вас зовут?" || got[0].End != 2 || got[1].Speaker != SpeakerClient {
		t.Fatalf("got %+v", got)
	}
	if dialogText(got) != "Сотрудник: Добрый день. Как вас зовут?\nКлиент: Сергей." {
		t.Fatalf("dialog: %q", dialogText(got))
	}
}

func TestMergeCriteria(t *testing.T) {
	criteria := []methodology.Criterion{
		{ID: "c1", Name: "Представился", Weight: 3},
		{ID: "c2", Name: "Выявил потребность", Weight: 2},
		{ID: "c3", Name: "Назначил шаг", Weight: 2},
	}
	got := mergeCriteria(criteria, []methodology.CriterionResult{
		{ID: "c2", Result: "failed", Quote: " цитата "},
		{ID: "c1", Result: "passed"},
		{ID: "c1", Result: "failed"}, // дубликат игнорируется
		{ID: "c9", Result: "passed"}, // чужой id игнорируется
	})
	if len(got) != 3 || got[0].Result != "passed" || got[0].Name != "Представился" ||
		got[1].Result != "failed" || got[1].Quote != "цитата" || got[2].Result != "na" {
		t.Fatalf("got %+v", got)
	}
}

func TestNormalize(t *testing.T) {
	r := AnalysisResult{Sentiment: "angry", Score: 42, CallOutcome: "maybe"}
	r.normalize()
	if r.Sentiment != "neutral" || r.Score != 5 || r.CallOutcome != "other" || r.Summary == "" {
		t.Fatalf("got %+v", r)
	}
	if r.Topics == nil || r.ActionItems == nil || r.ScriptViolations == nil {
		t.Fatal("массивы должны быть пустыми, а не nil (иначе в БД уйдёт null)")
	}
}

func TestTruncateRunes(t *testing.T) {
	s := strings.Repeat("я", 10)
	got, cut := truncateRunes(s, 3)
	if !cut || got != "яяя" || !utf8.ValidString(got) {
		t.Fatalf("got %q cut=%v", got, cut)
	}
	if got, cut := truncateRunes(s, 10); cut || got != s {
		t.Fatalf("короткая строка не должна обрезаться: %q", got)
	}
}

// fakeModel отвечает заранее заданным JSON и запоминает последний запрос.
func fakeModel(t *testing.T, answer any) (*llm.Client, *map[string]any) {
	t.Helper()
	var last map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&last)
		content, _ := json.Marshal(answer)
		json.NewEncoder(w).Encode(map[string]any{"message": map[string]any{"content": string(content)}})
	}))
	t.Cleanup(srv.Close)
	return llm.New(srv.URL, "test", 5*time.Second), &last
}

func TestAnalyzeWithMethodology(t *testing.T) {
	model, last := fakeModel(t, map[string]any{
		"summary": "Договорились о тесте", "sentiment": "positive", "call_outcome": "callback",
		"action_items": []string{"Привезти набор"}, "topics": []string{"доставка"},
		"criteria": []map[string]any{
			{"id": "c1", "result": "passed", "quote": "меня зовут Анна", "comment": "представилась"},
			{"id": "c2", "result": "failed", "quote": "", "comment": "перебила"},
		},
	})
	m := &methodology.Methodology{ID: 7, Prompt: "Ты — аналитик ОфисМаркет", Criteria: []methodology.Criterion{
		{ID: "c1", Name: "Представилась", Weight: 2},
		{ID: "c2", Name: "Не перебивает", Weight: 3},
	}}
	r, err := analyzeWithMethodology(context.Background(), model, m, &callRow{Direction: "inbound"}, "Сотрудник: Добрый день")
	if err != nil {
		t.Fatal(err)
	}
	// критичный провал → оценка не выше 4
	if r.Score != 4 || r.ScriptCompliance || !reflect.DeepEqual(r.ScriptViolations, []string{"Не перебивает"}) ||
		*r.MethodologyID != 7 || len(r.Criteria) != 2 || r.CallOutcome != "callback" {
		t.Fatalf("got %+v", r)
	}

	msgs := (*last)["messages"].([]any)
	system := msgs[0].(map[string]any)["content"].(string)
	user := msgs[1].(map[string]any)["content"].(string)
	if system != m.Prompt || !strings.Contains(user, "[c2] (критично) Не перебивает") || !strings.Contains(user, "входящий") {
		t.Fatalf("промпт собран неверно:\n%s\n---\n%s", system, user)
	}
}

func TestAssignRoles(t *testing.T) {
	model, _ := fakeModel(t, map[string]any{"speakers": []string{"employee", "client", "client"}})
	turns, err := assignRoles(context.Background(), model,
		[]segment{{Start: 0, End: 4, Text: "Как вас зовут? Сергей. Звоню насчёт доставки."}}, "inbound")
	if err != nil {
		t.Fatal(err)
	}
	if len(turns) != 2 || turns[0].Speaker != SpeakerEmployee || turns[1].Text != "Сергей. Звоню насчёт доставки." {
		t.Fatalf("got %+v", turns)
	}

	// Число ролей не совпало с числом предложений — ошибка, а не сдвинутая разметка
	model, _ = fakeModel(t, map[string]any{"speakers": []string{"employee"}})
	if _, err := assignRoles(context.Background(), model, []segment{{Text: "Раз. Два."}}, ""); err == nil {
		t.Fatal("ожидалась ошибка")
	}
}
