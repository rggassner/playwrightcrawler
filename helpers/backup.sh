#!/bin/bash
set -euo pipefail

CONFIG="../config.py"

# --- Parse optional argument ---
OUTPUT_DIR=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--output-dir /path/to/backups]"
      exit 1
      ;;
  esac
done

# Extract Elasticsearch connection details
HOST=$(grep 'ELASTICSEARCH_HOST' "$CONFIG" | cut -d"'" -f2)
PORT=$(grep 'ELASTICSEARCH_PORT' "$CONFIG" | grep -o '[0-9]\+')
USER=$(grep 'ELASTICSEARCH_USER' "$CONFIG" | cut -d"'" -f2)
PASSWORD=$(grep 'ELASTICSEARCH_PASSWORD' "$CONFIG" | cut -d"'" -f2)

# Extract both base index names
CONTENT_INDEX=$(grep 'CONTENT_INDEX' "$CONFIG" | cut -d"'" -f2)
LINKS_INDEX=$(grep 'LINKS_INDEX' "$CONFIG" | cut -d"'" -f2)

# --- Determine backup directory ---
if [[ -n "$OUTPUT_DIR" ]]; then
  BACKUP_DIR="${OUTPUT_DIR%/}/es_backups_$(date +%Y%m%d_%H%M%S)"
else
  BACKUP_DIR="es_backups_$(date +%Y%m%d_%H%M%S)"
fi
mkdir -p "$BACKUP_DIR"

echo "Backup output directory: $BACKUP_DIR"
echo

# --- Function to export and compress an index ---
backup_index() {
  local INDEX=$1
  local BASE_PATH="$BACKUP_DIR/$INDEX"
  local MAPPING_FILE="${BASE_PATH}.mapping.json"
  local DATA_FILE="${BASE_PATH}.data.json"

  echo "Backing up index: $INDEX"

  # Dump mapping
  NODE_TLS_REJECT_UNAUTHORIZED=0 elasticdump \
    --input="https://$USER:$PASSWORD@$HOST:$PORT/$INDEX" \
    --output="$MAPPING_FILE" \
    --type=mapping || echo "Warning: Failed mapping export for $INDEX"

  # Compress mapping if it exists
  if [ -f "$MAPPING_FILE" ]; then
    xz -ze9 --stdout < "$MAPPING_FILE" > "${MAPPING_FILE}.xz"
    rm "$MAPPING_FILE"
  fi

  # Dump data
  NODE_TLS_REJECT_UNAUTHORIZED=0 elasticdump \
    --input="https://$USER:$PASSWORD@$HOST:$PORT/$INDEX" \
    --output="$DATA_FILE" \
    --type=data || echo "Warning: Failed data export for $INDEX"

  # Compress data if it exists
  if [ -f "$DATA_FILE" ]; then
    xz -ze9 --stdout < "$DATA_FILE" > "${DATA_FILE}.xz"
    rm "$DATA_FILE"
  fi

  echo "Finished backup for: $INDEX"
  echo
}

# --- Function to list all matching indexes for a pattern ---
list_indexes() {
  local PATTERN=$1
  curl -s -k -u "$USER:$PASSWORD" "https://$HOST:$PORT/_cat/indices/${PATTERN}-*?h=index" | awk '{print $1}'
}

# --- Backup all matching indexes for both base names ---
for BASE_INDEX in "$CONTENT_INDEX" "$LINKS_INDEX"; do
  echo "Scanning for indexes matching: ${BASE_INDEX}-*"
  for INDEX in $(list_indexes "$BASE_INDEX"); do
    backup_index "$INDEX"
  done
done

echo "All indexes backed up successfully in $BACKUP_DIR"

