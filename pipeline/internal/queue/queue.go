// Пакет queue — топики Kafka, формат событий и общие операции с клиентом.
//
// События между этапами «тонкие»: в них только call_id. Все данные звонка
// каждый этап читает из PostgreSQL, поэтому событие можно безопасно
// отправить повторно (это делает sweeper в пакете worker).
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"callparser/internal/app"
)

const (
	TopicCallsReady         = "calls-ready"
	TopicTranscriptionsDone = "transcriptions-done"
	TopicAnalysisDone       = "analysis-done"
	TopicErrors             = "calls-errors"
)

// CallEvent — событие «звонок готов к следующему этапу».
type CallEvent struct {
	CallID string `json:"call_id"`
}

// ErrorEvent уходит в TopicErrors при неудачной обработке.
type ErrorEvent struct {
	CallID    string `json:"call_id"`
	Stage     string `json:"stage"`
	Error     string `json:"error"`
	Permanent bool   `json:"permanent"`
}

func Bootstrap() string {
	return app.Getenv("KAFKA_BOOTSTRAP", "kafka:9092")
}

// NewProducer создаёт клиент только для отправки.
func NewProducer(bootstrap string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(strings.Split(bootstrap, ",")...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
}

// NewConsumer создаёт клиент, читающий topic в группе group, с ручным коммитом.
// Им же можно отправлять сообщения.
func NewConsumer(bootstrap, group, topic string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(strings.Split(bootstrap, ",")...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.SessionTimeout(60*time.Second),
		kgo.HeartbeatInterval(20*time.Second),
	)
}

// WaitReady ждёт доступности брокера.
func WaitReady(ctx context.Context, c *kgo.Client) error {
	for i := range 20 {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := c.Ping(pingCtx)
		cancel()
		if err == nil {
			slog.Info("kafka готова")
			return nil
		}
		slog.Warn("kafka ожидание", "attempt", i+1, "err", err)
		if !app.Sleep(ctx, 3*time.Second) {
			return ctx.Err()
		}
	}
	return errors.New("kafka недоступна")
}

// Publish сериализует v в JSON и синхронно отправляет в topic.
func Publish(ctx context.Context, c *kgo.Client, topic string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: b}).FirstErr()
}
