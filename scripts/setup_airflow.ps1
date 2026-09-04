# PowerShell: инициализация Airflow Connections
# docker-compose exec airflow-webserver bash /opt/airflow/project/scripts/setup_airflow.sh

Write-Host "Для настройки Airflow выполните в контейнере:"
Write-Host "  docker-compose exec airflow-webserver bash /opt/airflow/project/scripts/setup_airflow.sh"
Write-Host ""
Write-Host "Или вручную в Airflow UI (http://localhost:8080):"
Write-Host "  Admin -> Connections -> Add:"
Write-Host "    clickhouse_default: host=clickhouse, port=9000, login=analyst"
Write-Host "    greenplum_default:  host=analytics-db, port=5432, login=gpadmin"
