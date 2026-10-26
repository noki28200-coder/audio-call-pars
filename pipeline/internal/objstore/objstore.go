// Пакет objstore — клиент S3-совместимого хранилища (SeaweedFS) из переменных окружения.
package objstore

import (
	"context"
	"log/slog"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"callparser/internal/app"
)

type Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
}

func ConfigFromEnv() Config {
	return Config{
		Endpoint:  app.MustEnv("S3_ENDPOINT"),
		AccessKey: app.MustEnv("S3_ACCESS_KEY"),
		SecretKey: app.MustEnv("S3_SECRET_KEY"),
		Bucket:    app.Getenv("S3_BUCKET", "call-audios"),
	}
}

// New создаёт клиент. Endpoint может быть с http:// или https://.
func New(cfg Config) (*minio.Client, error) {
	return minio.New(
		strings.TrimPrefix(strings.TrimPrefix(cfg.Endpoint, "http://"), "https://"),
		&minio.Options{
			Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
			Secure: strings.HasPrefix(cfg.Endpoint, "https://"),
			// SeaweedFS адресует бакеты через путь, а не поддомен
			BucketLookup: minio.BucketLookupPath,
		},
	)
}

// EnsureBucket создаёт бакет, если его нет.
func EnsureBucket(ctx context.Context, c *minio.Client, bucket string) error {
	exists, err := c.BucketExists(ctx, bucket)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	if err := c.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		// Бакет мог создать параллельно стартующий сервис
		if resp := minio.ToErrorResponse(err); resp.Code == "BucketAlreadyOwnedByYou" || resp.Code == "BucketAlreadyExists" {
			return nil
		}
		return err
	}
	slog.Info("бакет создан", "bucket", bucket)
	return nil
}
