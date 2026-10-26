# Один образ на все Go-сервисы конвейера. Какой собрать — задаёт SERVICE
# (имя папки в cmd/): collector | transcriber | analyzer.
FROM golang:1.23-alpine AS builder
ARG SERVICE
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN test -n "$SERVICE" && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /service ./cmd/${SERVICE}

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /service /service
USER nobody
CMD ["/service"]
