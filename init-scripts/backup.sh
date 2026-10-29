#!/bin/sh
# Автоматический бэкап: дамп PostgreSQL + архив хранилища записей (том s3_data).
# Запускается сервисом backup из docker-compose.yml и работает в бесконечном цикле.
#
# Восстановление:
#   docker compose exec -T postgres pg_restore -U callsuser -d callsdb --clean --if-exists < backups/db-<дата>.dump
#   docker compose stop s3
#   docker run --rm -v audio-call-parser-go_s3_data:/data -v "$PWD/backups":/b alpine \
#     sh -c 'rm -rf /data/* && tar xzf /b/s3-<дата>.tar.gz -C /data'
#   docker compose start s3
set -eu

DIR=/backups
INTERVAL_HOURS=${BACKUP_INTERVAL_HOURS:-24}
KEEP_DAYS=${BACKUP_KEEP_DAYS:-7}

log() { echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') [backup] $*"; }

backup() {
    stamp=$(date -u +%Y-%m-%d_%H%M)

    # Пишем во временный файл: оборванный бэкап не должен выглядеть готовым.
    # Проверки явные: внутри «backup || ...» set -e не действует.
    pg_dump -h postgres -U "$PGUSER" -d "$PGDATABASE" -Fc -f "$DIR/db-$stamp.dump.tmp" &&
        mv "$DIR/db-$stamp.dump.tmp" "$DIR/db-$stamp.dump" || return 1

    tar czf "$DIR/s3-$stamp.tar.gz.tmp" -C /s3-data . &&
        mv "$DIR/s3-$stamp.tar.gz.tmp" "$DIR/s3-$stamp.tar.gz" || return 1

    find "$DIR" -name '*.tmp' -delete
    find "$DIR" \( -name 'db-*.dump' -o -name 's3-*.tar.gz' \) -mtime +"$KEEP_DAYS" -delete
    log "готово: db-$stamp.dump ($(du -h "$DIR/db-$stamp.dump" | cut -f1)), s3-$stamp.tar.gz ($(du -h "$DIR/s3-$stamp.tar.gz" | cut -f1))"
}

mkdir -p "$DIR"
log "каждые ${INTERVAL_HOURS} ч, храню ${KEEP_DAYS} дн., папка ./backups"

while true; do
    # После перезапуска не делаем лишний бэкап, если свежий уже есть
    if [ -z "$(find "$DIR" -name 'db-*.dump' -mmin -$((INTERVAL_HOURS * 60)) | head -1)" ]; then
        backup || log "ОШИБКА бэкапа, повтор через час"
        sleep 3600
    else
        sleep 600
    fi
done
