#!/bin/bash
set -euo pipefail

DB_DUMP_FILE_NAME="$(date -u +'%Y-%m-%dT%H-%M-%SZ')-db.sql.gz"
TAR_FILE_NAME="$(date -u +'%Y-%m-%dT%H-%M-%SZ')-files.tar.gz"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Read in config/secrets from .env file
if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a; . "$SCRIPT_DIR/.env"; set +a
else
  echo "$SCRIPT_DIR/.env file not found! Exiting."
  exit 1
fi

if [ ! -f "$SCRIPT_DIR/my.cnf" ]; then
  echo "$SCRIPT_DIR/my.cnf file not found! Exiting."
  exit 1
fi

echo "Starting backup at $(date)"
mkdir -p "$BACKUP_DEST"

echo "Authenticating with B2"
# Credentials are read from env vars, don't echo them to stdout
b2 account authorize > /dev/null

echo "Dumping DB"
mysqldump --defaults-extra-file="$SCRIPT_DIR/my.cnf" --single-transaction "$MYSQL_DB" | gzip > "$BACKUP_DEST/$DB_DUMP_FILE_NAME"

echo "Creating a snapshot of $BACKUP_SOURCE"
tar czf "$BACKUP_DEST/$TAR_FILE_NAME" "$BACKUP_SOURCE"

echo "Uploading DB dump to Backblaze B2"
b2 file upload "$BUCKET_NAME" "$BACKUP_DEST/$DB_DUMP_FILE_NAME" "$DB_DUMP_FILE_NAME"

echo "Uploading files snapshot to Backblaze B2"
b2 file upload "$BUCKET_NAME" "$BACKUP_DEST/$TAR_FILE_NAME" "$TAR_FILE_NAME"

echo "Cleaning up"
rm "$BACKUP_DEST/$DB_DUMP_FILE_NAME"
rm "$BACKUP_DEST/$TAR_FILE_NAME"

echo "Deauthenticating B2"
b2 account clear

echo "Backup completed at $(date)"

# TODO - secondary backup with a different tool.
# Maybe a plain encrypted zip with a retention policy on the bucket.
# TODO - delete old backups from B2 according to retention policy.