package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"

	"callparser/internal/worker"
)

type transcriptResult struct {
	Language            string           `json:"language"`
	LanguageProbability float64          `json:"language_probability"`
	Segments            []map[string]any `json:"segments"`
	FullText            string           `json:"full_text"`
}

type whisperClient struct {
	baseURL string
	http    *http.Client
}

// transcribe отправляет аудио в whisperx-service потоком, не держа файл в памяти целиком.
func (c *whisperClient) transcribe(ctx context.Context, audio io.Reader, filename, language string) (*transcriptResult, error) {
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		pw.CloseWithError(writeForm(mw, audio, filename, language))
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/transcribe", pr)
	if err != nil {
		pr.Close()
		return nil, err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode >= 400 {
		err := fmt.Errorf("whisperx %d: %s", resp.StatusCode, body)
		// 400/422 — файл пустой или не декодируется: повтор не поможет.
		if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnprocessableEntity {
			return nil, worker.Permanent(err)
		}
		return nil, err
	}

	var result transcriptResult
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	return &result, nil
}

func writeForm(mw *multipart.Writer, audio io.Reader, filename, language string) error {
	if err := mw.WriteField("language", language); err != nil {
		return err
	}
	fw, err := mw.CreateFormFile("file", filename)
	if err != nil {
		return err
	}
	if _, err := io.Copy(fw, audio); err != nil {
		return fmt.Errorf("read audio: %w", err)
	}
	return mw.Close()
}
