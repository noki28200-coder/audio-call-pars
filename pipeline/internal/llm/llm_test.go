package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHasModel(t *testing.T) {
	installed := []string{"qwen3.5:9b", "llama3:latest"}
	cases := map[string]bool{
		"qwen3.5:9b": true,
		"qwen3.5:4b": false, // не по префиксу «qwen3.5:»
		"llama3":     true,  // без тега = :latest
		"mistral":    false,
	}
	for model, want := range cases {
		if got := hasModel(installed, model); got != want {
			t.Errorf("hasModel(%q) = %v, want %v", model, got, want)
		}
	}
}

func TestExtractJSON(t *testing.T) {
	if got := ExtractJSON("```json\n{\"a\":1}\n```"); got != `{"a":1}` {
		t.Fatalf("got %q", got)
	}
}

func TestGenerateJSON(t *testing.T) {
	var req map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&req)
		w.Write([]byte(`{"message":{"content":"{\"answer\":\"да\"}"}}`))
	}))
	defer srv.Close()

	var out struct{ Answer string }
	c := New(srv.URL, "m", 5*time.Second)
	if err := c.GenerateJSON(context.Background(), "sys", "q", map[string]any{"type": "object"}, 0.1, &out); err != nil {
		t.Fatal(err)
	}
	if out.Answer != "да" {
		t.Fatalf("answer %q", out.Answer)
	}
	if req["think"] != false || req["format"] == nil || len(req["messages"].([]any)) != 2 {
		t.Fatalf("запрос к Ollama: %v", req)
	}
}
