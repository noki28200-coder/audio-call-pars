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

// ── healthcheck ─────────────────────────────────────────────────

// HeartbeatFile обновляется, пока сервис жив и видит свои зависимости.
// Healthcheck в docker-compose проверяет, что файл свежий.
const HeartbeatFile = "/tmp/healthy"

// Heartbeat раз в 30 секунд выполняет checks (например, Ping БД и Kafka)
// и при успехе всех обновляет HeartbeatFile. Работает до отмены ctx.
func Heartbeat(ctx context.Context, checks ...func(context.Context) error) {
	go func() {
		for {
			if err := runChecks(ctx, checks); err != nil {
				slog.Warn("healthcheck не пройден", "err", err)
			} else if err := touch(HeartbeatFile); err != nil {
				slog.Warn("не удалось обновить heartbeat", "err", err)
			}
			if !Sleep(ctx, 30*time.Second) {
				return
			}
		}
	}()
}

func runChecks(ctx context.Context, checks []func(context.Context) error) error {
	for _, check := range checks {
		checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := check(checkCtx)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

func touch(path string) error {
	now := time.Now()
	if err := os.Chtimes(path, now, now); err == nil {
		return nil
	}
	return os.WriteFile(path, nil, 0o644)
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
