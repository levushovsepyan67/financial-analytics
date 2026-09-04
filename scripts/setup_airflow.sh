# Скрипт инициализации Airflow Connections и Variables
# Запуск: docker-compose exec airflow-webserver bash /opt/airflow/project/scripts/setup_airflow.sh

set -e

echo "=== Создание Airflow Connections ==="

airflow connections delete clickhouse_default 2>/dev/null || true
airflow connections add clickhouse_default \
  --conn-type generic \
  --conn-host clickhouse \
  --conn-port 9000 \
  --conn-login analyst \
  --conn-password analyst123 \
  --conn-schema market_db

airflow connections delete greenplum_default 2>/dev/null || true
airflow connections add greenplum_default \
  --conn-type postgres \
  --conn-host analytics-db \
  --conn-port 5432 \
  --conn-login gpadmin \
  --conn-password gpadmin123 \
  --conn-schema market_analytics

airflow connections delete minio_default 2>/dev/null || true
airflow connections add minio_default \
  --conn-type aws \
  --conn-extra '{"endpoint_url": "http://minio:9000", "aws_access_key_id": "minioadmin", "aws_secret_access_key": "minioadmin123"}'

echo "=== Создание Airflow Variables ==="

airflow variables set telegram_bot_token "${TELEGRAM_BOT_TOKEN:-}"
airflow variables set telegram_chat_id "${TELEGRAM_CHAT_ID:-}"

echo "=== Готово! Connections и Variables настроены ==="
