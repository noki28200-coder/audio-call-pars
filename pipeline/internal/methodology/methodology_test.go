package methodology

import (
	"reflect"
	"testing"
)

func TestScore(t *testing.T) {
	r := func(weight int, result string) CriterionResult {
		return CriterionResult{Name: result, Weight: weight, Result: result}
	}
	cases := []struct {
		name      string
		results   []CriterionResult
		score     int
		compliant bool
	}{
		{"всё выполнено", []CriterionResult{r(3, "passed"), r(1, "passed")}, 10, true},
		{"ничего не применимо", []CriterionResult{r(3, "na")}, 5, true},
		{"всё провалено", []CriterionResult{r(2, "failed"), r(1, "failed")}, 1, false},
		// passed 2+2=4 из 5 → 1 + 9*4/5 = 8.2 → 8
		{"провал желательного", []CriterionResult{r(2, "passed"), r(2, "passed"), r(1, "failed")}, 8, false},
		// без ограничения было бы 1 + 9*6/9 = 7, но критичный провал → 4
		{"провал критичного", []CriterionResult{r(3, "passed"), r(3, "passed"), r(3, "failed")}, 4, false},
		{"неприменимые не влияют", []CriterionResult{r(2, "passed"), r(3, "na")}, 10, true},
	}
	for _, c := range cases {
		score, compliant, _ := Score(c.results)
		if score != c.score || compliant != c.compliant {
			t.Errorf("%s: score=%d compliant=%v, want %d %v", c.name, score, compliant, c.score, c.compliant)
		}
	}
	_, _, violations := Score([]CriterionResult{{Name: "Перебивал", Weight: 3, Result: "failed"}})
	if !reflect.DeepEqual(violations, []string{"Перебивал"}) {
		t.Errorf("violations: %v", violations)
	}
}

func TestValidate(t *testing.T) {
	m := &Methodology{
		Prompt: "  Ты — аналитик  ",
		Criteria: []Criterion{
			{Name: "Поздоровался", Weight: 7},
			{Name: "   "}, // пустые выкидываются
			{Name: "Не перебивал", Weight: 0},
		},
	}
	if err := Validate(m); err != nil {
		t.Fatal(err)
	}
	want := []Criterion{{ID: "c1", Name: "Поздоровался", Weight: 3}, {ID: "c2", Name: "Не перебивал", Weight: 1}}
	if !reflect.DeepEqual(m.Criteria, want) || m.Prompt != "Ты — аналитик" {
		t.Fatalf("got %+v %q", m.Criteria, m.Prompt)
	}
	if err := Validate(&Methodology{Prompt: "x"}); err == nil {
		t.Error("без критериев должна быть ошибка")
	}
	if err := Validate(&Methodology{Criteria: want}); err == nil {
		t.Error("без промпта должна быть ошибка")
	}
}
