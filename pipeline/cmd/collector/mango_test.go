package main

import "testing"

func TestParseRecords(t *testing.T) {
	body := []byte(`{"results":[{"records":[
		{"recording":"rec-1","emp_id":12345678,"emp_name":"Иван","from_number":"79990001122",
		 "to_number":"74950001122","call_direction":0,"duration":65,"start":1730000000},
		{"recording":"rec-2","emp_id":null,"call_direction":1,"duration":"12"},
		{"recording":"","emp_id":1}
	]}]}`)

	recs, err := parseRecords(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("ожидалось 2 записи (без recording пропускается), получено %d", len(recs))
	}

	r := recs[0]
	if r.EmployeeID != "12345678" {
		t.Errorf("большой числовой ID испорчен: %q", r.EmployeeID)
	}
	if r.Direction != "inbound" || r.Duration != 65 || r.StartTime == nil || r.StartTime.Unix() != 1730000000 {
		t.Errorf("unexpected record: %+v", r)
	}

	r = recs[1]
	if r.EmployeeID != "" || r.EmployeeName != "" {
		t.Errorf("null/отсутствующие поля должны быть пустыми, а не \"<nil>\": %+v", r)
	}
	if r.Direction != "outbound" || r.Duration != 12 || r.StartTime != nil {
		t.Errorf("unexpected record: %+v", r)
	}
}
