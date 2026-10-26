// Пакет objstore — клиент MinIO (S3) из переменных окружения.
package objstore

import (
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
		Endpoint:  app.MustEnv("MINIO_ENDPOINT"),
		AccessKey: app.Getenv("MINIO_ACCESS_KEY", "minioadmin"),
		SecretKey: app.Getenv("MINIO_SECRET_KEY", "minioadmin"),
		Bucket:    app.Getenv("MINIO_BUCKET", "call-audios"),
	}
}

// New создаёт клиент. Endpoint может быть с http:// или https://.
func New(cfg Config) (*minio.Client, error) {
	return minio.New(
		strings.TrimPrefix(strings.TrimPrefix(cfg.Endpoint, "http://"), "https://"),
		&minio.Options{
			Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
			Secure: strings.HasPrefix(cfg.Endpoint, "https://"),
		},
	)
}
