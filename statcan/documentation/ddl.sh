#!/bin/bash

# Configuration
CONTAINER_NAME="pg-data"
DB_USER="statcan"
DB_NAME="statcan"

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_FILE="${SCRIPT_DIR}/complete_schema.sql"

echo "🔍 Checking if container '${CONTAINER_NAME}' is running..."

# Check if container exists and is running
if ! docker ps | grep -q ${CONTAINER_NAME}; then
    echo "❌ Container '${CONTAINER_NAME}' is not running!"
    echo "💡 Start it with: cd /mnt/data/db && ./start_postgres.sh"
    exit 1
fi

echo "✅ Container '${CONTAINER_NAME}' is running"
echo ""
echo "📤 Dumping complete DDL for database: ${DB_NAME}"
echo "📁 Output file: ${OUTPUT_FILE}"
echo ""

# Execute pg_dump inside the container and redirect output to host file
docker exec ${CONTAINER_NAME} pg_dump \
  --username=${DB_USER} \
  --dbname=${DB_NAME} \
  --schema-only \
  --no-owner \
  --no-privileges \
  --create \
  --clean \
  --if-exists \
  --verbose \
  > ${OUTPUT_FILE} 2>&1

# Check if the dump was successful
if [ $? -eq 0 ] && [ -s ${OUTPUT_FILE} ]; then
    echo "✅ Schema dump completed successfully!"
    echo "📁 File location: $(pwd)/${OUTPUT_FILE}"
    echo "📊 File size: $(du -h ${OUTPUT_FILE} | cut -f1)"
    echo ""
    echo "🔍 Quick verification:"
    echo "   - Total lines: $(wc -l < ${OUTPUT_FILE})"
    echo "   - Contains schemas: $(grep -c "CREATE SCHEMA" ${OUTPUT_FILE})"
    echo "   - Contains tables: $(grep -c "CREATE TABLE" ${OUTPUT_FILE})"
    echo "   - Contains indexes: $(grep -c "CREATE.*INDEX" ${OUTPUT_FILE})"
else
    echo "❌ Schema dump failed!"
    echo "📋 Error details:"
    if [ -f ${OUTPUT_FILE} ]; then
        cat ${OUTPUT_FILE}
    fi
    exit 1
fi

echo ""
echo "🎯 Ready to commit to Git!"
echo "   git add ${OUTPUT_FILE}"
echo "   git commit -m \"Add complete database schema DDL\""
