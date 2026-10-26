package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ── клиент MangoOffice API ──────────────────────────────────────

type mangoClient struct {
	baseURL string
	apiKey  string
	apiSalt string
	http    *http.Client
}

// sign — подпись запроса: sha256(api_key + json + api_salt).
func (c *mangoClient) sign(jsonBody string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(c.apiKey+jsonBody+c.apiSalt)))
}

// post отправляет подписанный запрос и возвращает тело ответа.
func (c *mangoClient) post(ctx context.Context, endpoint string, params map[string]any) ([]byte, error) {
	jsonBytes, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	jsonBody := string(jsonBytes)
	form := url.Values{
		"vpbx_api_key": {c.apiKey},
		"sign":         {c.sign(jsonBody)},
		"json":         {jsonBody},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(c.baseURL, "/")+"/"+strings.TrimLeft(endpoint, "/"),
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("mango API %s %d: %.300s", endpoint, resp.StatusCode, body)
	}
	return body, nil
}

// mangoRecord — одна запись из ответа stats/calls/records.
type mangoRecord struct {
	RecordingID  string
	EmployeeID   string
	EmployeeName string
	PhoneFrom    string
	PhoneTo      string
	Direction    string
	Duration     int
	StartTime    *time.Time
}

func (c *mangoClient) listRecords(ctx context.Context, from, to time.Time) ([]mangoRecord, error) {
	body, err := c.post(ctx, "stats/calls/records/", map[string]any{
		"date_from": from.Unix(),
		"date_to":   to.Unix(),
		"fields":    "start,finish,duration,from_number,to_number,is_recorded,recording,emp_id,emp_name,call_direction",
	})
	if err != nil {
		return nil, err
	}
	return parseRecords(body)
}

func parseRecords(body []byte) ([]mangoRecord, error) {
	var data struct {
		Results []struct {
			Records []map[string]any `json:"records"`
		} `json:"results"`
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber() // иначе большие ID превращаются в 1.234567e+06
	if err := dec.Decode(&data); err != nil {
		return nil, fmt.Errorf("parse mango response: %w", err)
	}

	var out []mangoRecord
	for _, group := range data.Results {
		for _, r := range group.Records {
			rec := mangoRecord{
				RecordingID:  field(r, "recording"),
				EmployeeID:   field(r, "emp_id"),
				EmployeeName: field(r, "emp_name"),
				PhoneFrom:    field(r, "from_number"),
				PhoneTo:      field(r, "to_number"),
				Direction:    "outbound",
			}
			if rec.RecordingID == "" {
				continue
			}
			if field(r, "call_direction") == "0" {
				rec.Direction = "inbound"
			}
			if d, err := strconv.ParseFloat(field(r, "duration"), 64); err == nil {
				rec.Duration = int(d)
			}
			if ts, err := strconv.ParseFloat(field(r, "start"), 64); err == nil && ts > 0 {
				t := time.Unix(int64(ts), 0).UTC()
				rec.StartTime = &t
			}
			out = append(out, rec)
		}
	}
	return out, nil
}

// field возвращает значение поля строкой: "" для отсутствующего или null.
func field(r map[string]any, key string) string {
	switch v := r[key].(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(v)
	case json.Number:
		return v.String()
	case bool:
		return strconv.FormatBool(v)
	default:
		return fmt.Sprint(v)
	}
}

func (c *mangoClient) downloadRecording(ctx context.Context, recordingID string) ([]byte, error) {
	return c.post(ctx, "stats/calls/recording/file/", map[string]any{"recording_id": recordingID})
}
