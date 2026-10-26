// Пакет app — общий старт сервисов: логгер, переменные окружения,
// контекст с остановкой по сигналу, ожидание зависимостей.
package app

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

// Init настраивает логгер и возвращает контекст, который отменяется по SIGINT/SIGTERM.
func Init() (context.Context, context.CancelFunc) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
}

// ── переменные окружения ────────────────────────────────────────

func Getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func MustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("обязательная переменная окружения не задана", "var", key)
		os.Exit(1)
	}
	return v
}

func GetInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		slog.Warn("некорректное число в переменной окружения, беру значение по умолчанию", "var", key, "default", def)
	}
	return def
}

// Fatal пишет ошибку и завершает процесс.
func Fatal(msg string, err error) {
	slog.Error(msg, "err", err)
	os.Exit(1)
}

// ── ожидание зависимостей ───────────────────────────────────────

// WaitHTTP ждёт, пока url не ответит 200. Возвращает false, если попытки кончились
// или контекст отменён.
func WaitHTTP(ctx context.Context, name, url string, attempts int, delay time.Duration) bool {
	client := &http.Client{Timeout: 10 * time.Second}
	for i := range attempts {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				slog.Info(name + " готов")
				return true
			}
		}
		slog.Warn(name+" ожидание", "attempt", i+1)
		if !Sleep(ctx, delay) {
			return false
		}
	}
	return false
}

// Sleep спит d или до отмены контекста. Возвращает false, если контекст отменён.
func Sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
