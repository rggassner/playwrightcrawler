#!/bin/bash
set -euo pipefail

CONFIG="../config.py"
BACKUP_DIR="${1:-.}"  # Directory containing your .xz backup files (default = current dir)
RESTORE_PREFIX="${RESTORE_PREFIX:-restored_}"  # Optional prefix (e.g. restored_), leave empty for original names

# Extract Elasticsearch connection details
HOST=$(grep 'ELASTICSEARCH_HOST' "$CONFIG" | cut -d"'" -f2)
PORT=$(grep 'ELASTICSEARCH_PORT' "$CONFIG" | grep -o '[0-9]\+')
USER=$(grep 'ELASTICSEARCH_USER' "$CONFIG" | cut -d"'" -f2)
PASSWORD=$(grep 'ELASTICSEARCH_PASSWORD' "$CONFIG" | cut -d"'" -f2)

# Ensure elasticdump exists
if ! command -v elasticdump &>/dev/null; then
  echo "elasticdump not found. Please install it first: npm install -g elasticdump"
  exit 1
fi

echo "Starting Elasticsearch restore from: $BACKUP_DIR"
echo "RESTORE_PREFIX: '${RESTORE_PREFIX}'"
echo

# Function to restore a single index from mapping + data
restore_index() {
  local BASE_FILE=$1
  local RESTORE_INDEX="${RESTORE_PREFIX}${BASE_FILE}"

  echo "Restoring index: $RESTORE_INDEX"

  local MAPPING_FILE_XZ="${BACKUP_DIR}/${BASE_FILE}.mapping.json.xz"
  local DATA_FILE_XZ="${BACKUP_DIR}/${BASE_FILE}.data.json.xz"

  if [[ ! -f "$MAPPING_FILE_XZ" || ! -f "$DATA_FILE_XZ" ]]; then
    echo "Missing mapping or data for $BASE_FILE, skipping."
    return
  fi

  # Decompress mapping and data files temporarily
  xz -dc "$MAPPING_FILE_XZ" > "${BACKUP_DIR}/${BASE_FILE}.mapping.json"
  xz -dc "$DATA_FILE_XZ" > "${BACKUP_DIR}/${BASE_FILE}.data.json"

  # Create index mapping
  NODE_TLS_REJECT_UNAUTHORIZED=0 elasticdump \
    --input="${BACKUP_DIR}/${BASE_FILE}.mapping.json" \
    --output="https://$USER:$PASSWORD@$HOST:$PORT/$RESTORE_INDEX" \
    --type=mapping || echo "Warning: mapping import failed for $BASE_FILE"

  # Import data
  NODE_TLS_REJECT_UNAUTHORIZED=0 elasticdump \
    --input="${BACKUP_DIR}/${BASE_FILE}.data.json" \
    --output="https://$USER:$PASSWORD@$HOST:$PORT/$RESTORE_INDEX" \
    --type=data || echo "Warning: data import failed for $BASE_FILE"

  # Cleanup temporary files
  rm -f "${BACKUP_DIR}/${BASE_FILE}.mapping.json" "${BACKUP_DIR}/${BASE_FILE}.data.json"

  echo "Finished restoring: $RESTORE_INDEX"
  echo
}

# Find all mapping backup files (one per index)
for MAPPING_FILE in "${BACKUP_DIR}"/*.mapping.json.xz; do
  [[ -e "$MAPPING_FILE" ]] || { echo "No mapping files found in $BACKUP_DIR"; exit 1; }
  BASE_FILE=$(basename "$MAPPING_FILE" .mapping.json.xz)
  restore_index "$BASE_FILE"
done

echo "All indexes restored successfully!"

