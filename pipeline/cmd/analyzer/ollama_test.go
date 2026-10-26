package main

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestParseAnalysis(t *testing.T) {
	t.Run("ответ в markdown-фенсах", func(t *testing.T) {
		raw := "```json\n{\"summary\":\"Клиент купил\",\"sentiment\":\"positive\",\"score\":9,\"call_outcome\":\"sold\"}\n```"
		got, err := parseAnalysis(raw)
		if err != nil {
			t.Fatal(err)
		}
		if got.Summary != "Клиент купил" || got.Sentiment != "positive" || got.Score != 9 || got.CallOutcome != "sold" {
			t.Fatalf("unexpected result: %+v", got)
		}
	})

	t.Run("недопустимые значения нормализуются", func(t *testing.T) {
		got, err := parseAnalysis(`{"sentiment":"angry","score":42,"call_outcome":"maybe"}`)
		if err != nil {
			t.Fatal(err)
		}
		if got.Sentiment != "neutral" || got.Score != 5 || got.CallOutcome != "other" || got.Summary == "" {
			t.Fatalf("unexpected result: %+v", got)
		}
		if got.Topics == nil || got.ActionItems == nil || got.ScriptViolations == nil {
			t.Fatal("массивы должны быть пустыми, а не nil (иначе в БД уйдёт null)")
		}
	})

	t.Run("не JSON", func(t *testing.T) {
		if _, err := parseAnalysis("не могу проанализировать"); err == nil {
			t.Fatal("ожидалась ошибка")
		}
	})
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

func TestBuildPromptTruncates(t *testing.T) {
	p := buildPrompt("", "", "", 0, strings.Repeat("ё", maxTranscriptRunes+100))
	if !utf8.ValidString(p) || !strings.Contains(p, "[текст обрезан]") || !strings.Contains(p, "неизвестен") {
		t.Fatal("промпт собран неверно")
	}
}

func TestHasModel(t *testing.T) {
	installed := []string{"gemma3:12b", "llama3:latest"}
	cases := map[string]bool{
		"gemma3:12b": true,
		"gemma3:4b":  false, // раньше совпадало по префиксу «gemma3:»
		"llama3":     true,  // без тега = :latest
		"mistral":    false,
	}
	for model, want := range cases {
		if got := hasModel(installed, model); got != want {
			t.Errorf("hasModel(%q) = %v, want %v", model, got, want)
		}
	}
}
