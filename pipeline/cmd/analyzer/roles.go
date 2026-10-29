package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"callparser/internal/llm"
)

// Роли участников разговора.
const (
	SpeakerEmployee = "employee"
	SpeakerClient   = "client"
)

var speakerLabels = map[string]string{SpeakerEmployee: "Сотрудник", SpeakerClient: "Клиент"}

// segment — фрагмент расшифровки (формат transcript_segments).
type segment struct {
	Start   float64 `json:"start"`
	End     float64 `json:"end"`
	Text    string  `json:"text"`
	Speaker string  `json:"speaker,omitempty"`
}

// maxRoleSentences — дальше разметка одним запросом становится ненадёжной;
// такие звонки анализируются без ролей.
const maxRoleSentences = 400

// sentenceEndRe — конец предложения: знак препинания и пробел после него.
var sentenceEndRe = regexp.MustCompile(`[.!?…]+\s+`)

// splitSentences делит фрагменты Whisper на предложения: в одном фрагменте
// часто оказываются реплики обоих собеседников. Время делится пропорционально
// длине текста — для показа в карточке звонка этой точности хватает.
func splitSentences(segs []segment) []segment {
	out := make([]segment, 0, len(segs))
	for _, s := range segs {
		text := strings.TrimSpace(s.Text)
		var parts []string
		last := 0
		for _, loc := range sentenceEndRe.FindAllStringIndex(text, -1) {
			parts = append(parts, strings.TrimSpace(text[last:loc[1]]))
			last = loc[1]
		}
		if rest := strings.TrimSpace(text[last:]); rest != "" {
			parts = append(parts, rest)
		}

		total := utf8.RuneCountInString(text)
		t := s.Start
		for i, p := range parts {
			end := s.End
			if total > 0 && i < len(parts)-1 {
				end = t + (s.End-s.Start)*float64(utf8.RuneCountInString(p))/float64(total)
			}
			out = append(out, segment{Start: round3(t), End: round3(end), Text: p})
			t = end
		}
	}
	return out
}

func round3(f float64) float64 { return float64(int64(f*1000+0.5)) / 1000 }

// mergeTurns склеивает соседние предложения одного собеседника в реплики.
func mergeTurns(sents []segment) []segment {
	var out []segment
	for _, s := range sents {
		if n := len(out); n > 0 && out[n-1].Speaker == s.Speaker {
			out[n-1].Text += " " + s.Text
			out[n-1].End = s.End
			continue
		}
		out = append(out, s)
	}
	return out
}

const rolesSystem = `Ты получаешь расшифровку телефонного разговора сотрудника компании с клиентом,
разбитую на пронумерованные предложения. Определи, кто произносит каждое предложение:
"employee" — сотрудник компании, "client" — клиент.

Как отличить:
- Сотрудник: открывает разговор (приветствие, название компании, своё имя), задаёт вопросы
  о ситуации и потребностях клиента, рассказывает о продукте, называет цены, предлагает
  варианты и время («вам удобнее в четверг или в пятницу?»), подводит итог, благодарит за время.
- Клиент: отвечает на вопросы о себе и своей компании, называет своё имя, описывает проблемы,
  возражает («дорого», «подумаю»), выбирает из предложенных вариантов («давайте в четверг»).
- Вопрос и ответ на него почти всегда произносят разные люди.
- Реплики обычно чередуются, но несколько предложений подряд может сказать один человек.

Ответ — массив по одному элементу на каждое предложение, строго по порядку:
n — номер предложения из квадратных скобок, speaker — кто его произносит.`

// assignRoles размечает, где говорит сотрудник, а где клиент, и возвращает
// реплики. При ошибке звонок анализируется без ролей.
func assignRoles(ctx context.Context, model *llm.Client, segs []segment, direction string) ([]segment, error) {
	sents := splitSentences(segs)
	if len(sents) == 0 || len(sents) > maxRoleSentences {
		return nil, fmt.Errorf("неподходящая длина расшифровки: %d предложений", len(sents))
	}

	var b strings.Builder
	if hint := map[string]string{
		"inbound":  "Входящий звонок: клиент позвонил в компанию.\n\n",
		"outbound": "Исходящий звонок: сотрудник позвонил клиенту.\n\n",
	}[direction]; hint != "" {
		b.WriteString(hint)
	}
	for i, s := range sents {
		fmt.Fprintf(&b, "[%d] %s\n", i+1, s.Text)
	}

	// Номер рядом с ролью: без него модель на длинном списке сбивается
	// и приписывает роль соседнему предложению
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"speakers": map[string]any{
				"type": "array", "minItems": len(sents), "maxItems": len(sents),
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"n":       map[string]any{"type": "integer"},
						"speaker": map[string]any{"type": "string", "enum": []string{SpeakerEmployee, SpeakerClient}},
					},
					"required": []string{"n", "speaker"},
				},
			},
		},
		"required": []string{"speakers"},
	}
	var out struct {
		Speakers []struct {
			N       int    `json:"n"`
			Speaker string `json:"speaker"`
		} `json:"speakers"`
	}
	if err := model.GenerateJSON(ctx, rolesSystem, b.String(), schema, 0, &out); err != nil {
		return nil, err
	}
	if len(out.Speakers) != len(sents) {
		return nil, fmt.Errorf("модель вернула %d ролей на %d предложений", len(out.Speakers), len(sents))
	}
	for _, sp := range out.Speakers {
		if sp.N < 1 || sp.N > len(sents) || sents[sp.N-1].Speaker != "" {
			return nil, fmt.Errorf("неверный или повторный номер предложения %d", sp.N)
		}
		if _, ok := speakerLabels[sp.Speaker]; !ok {
			return nil, fmt.Errorf("неизвестная роль %q", sp.Speaker)
		}
		sents[sp.N-1].Speaker = sp.Speaker
	}
	return mergeTurns(sents), nil
}

// hasRoles — расшифровка уже размечена (например, при повторном анализе).
func hasRoles(segs []segment) bool {
	if len(segs) == 0 {
		return false
	}
	for _, s := range segs {
		if s.Speaker == "" {
			return false
		}
	}
	return true
}

// dialogText — расшифровка для анализа: «Сотрудник: ...» / «Клиент: ...».
func dialogText(turns []segment) string {
	lines := make([]string, len(turns))
	for i, t := range turns {
		lines[i] = speakerLabels[t.Speaker] + ": " + t.Text
	}
	return strings.Join(lines, "\n")
}
