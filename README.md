# Financial Market Analytics Platform

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Airflow 2.8](https://img.shields.io/badge/airflow-2.8-orange.svg)](https://airflow.apache.org/)
[![ClickHouse](https://img.shields.io/badge/clickhouse-23.8-yellow.svg)](https://clickhouse.com/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**Платформа аналитики финансовых рынков** — pet-проект, демонстрирующий полный data engineering стек: сбор данных из публичных API, Data Lake (MinIO/S3), OLAP (ClickHouse), аналитика (PostgreSQL/Greenplum), PySpark-обработку, детекцию аномалий, риск-метрики и оркестрацию через Apache Airflow.

> Каждые 15 минут pipeline собирает OHLCV-данные по акциям (MOEX, Yahoo) и криптовалютам (Binance), валидирует их, сохраняет в Data Lake и ClickHouse, запускает ансамбль из 3 детекторов аномалий, считает VaR/Sharpe/CVaR и отправляет Telegram-алерты при критических событиях.

**Подробный разбор каждого файла, потоков данных и запуска «для чайников»:** [PROJECT_GUIDE.md](PROJECT_GUIDE.md)

---

## Содержание

- [Архитектура](#архитектура)
- [Стек технологий](#стек-технологий)
- [Быстрый старт](#быстрый-старт)
- [Структура проекта](#структура-проекта)
- [Компоненты](#компоненты)
- [Модели и формулы](#модели-и-формулы)
- [Airflow DAG](#airflow-dag)
- [Power BI](#power-bi)
- [Тестирование](#тестирование)
- [Конфигурация](#конфигурация)
- [Roadmap](#roadmap)

---

## Архитектура

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         APACHE AIRFLOW (оркестрация)                    │
│                    schedule: */15 * * * * (каждые 15 мин)               │
└─────────────────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  MOEX ISS    │    │  Binance API │    │ Yahoo Finance│
│  (акции РФ)  │    │  (крипта)    │    │  (US stocks) │
└──────┬───────┘    └──────┬───────┘    └──────┬───────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           ▼
              ┌────────────────────────┐
              │   OHLCV Validator      │
              │  (hard + soft checks)  │
              └───────────┬────────────┘
                          │
            ┌─────────────┼─────────────┐
            ▼             ▼             ▼
    ┌──────────────┐ ┌──────────┐ ┌──────────────┐
    │ MinIO (S3)   │ │ClickHouse│ │ PostgreSQL   │
    │  Data Lake   │ │  (OLAP)  │ │ (Analytics)  │
    │  JSON-Lines  │ │  ticks   │ │  VaR, CORR   │
    └──────┬───────┘ └────┬─────┘ └──────────────┘
           │              │
           ▼              ▼
    ┌──────────────┐ ┌──────────────────────────┐
    │   PySpark    │ │  Anomaly Ensemble (3)    │
    │  Features    │ │  Z-score + IF + GARCH    │
    │  SMA,RSI,Vol │ └───────────┬──────────────┘
    └──────────────┘             │
                                 ▼
                    ┌────────────────────────┐
                    │  Risk Metrics          │
                    │  VaR×3, CVaR, Sharpe   │
                    │  Sortino, Drawdown     │
                    └───────────┬────────────┘
                                ▼
                    ┌────────────────────────┐
                    │  Telegram Alerts       │
                    │  (critical anomalies)  │
                    └────────────────────────┘
```

### Слои данных

| Слой | Хранилище | Формат | Назначение |
|------|-----------|--------|------------|
| Raw (Bronze) | MinIO S3 | JSON-Lines, Hive-партиции | Неизменяемые сырые данные |
| Processed (Silver) | MinIO S3 | Parquet | Рассчитанные фичи |
| Serving (Gold) | ClickHouse | MergeTree | OLAP-запросы, дашборды |
| Analytics | PostgreSQL | Tables | VaR, корреляции, MPP-запросы |

---

## Стек технологий

| Инструмент | Роль | Порт |
|------------|------|------|
| **Python 3.10+** | Ingestion, ML, скрипты | — |
| **Apache Airflow 2.8** | Оркестрация DAG | 8080 |
| **ClickHouse 23.8** | OLAP, быстрые агрегаты | 8123 / 9000 |
| **MinIO** | S3-совместимый Data Lake | 9002 / 9001 |
| **PostgreSQL 15** | Analytics DB (Greenplum-совместимый) | 5433 |
| **PySpark 3.5** | Расчёт технических индикаторов | — |
| **Redis + Celery** | Очередь задач Airflow | 6379 |
| **Flower** | Мониторинг воркеров | 5555 |
| **scikit-learn** | Isolation Forest | — |
| **arch** | GARCH(1,1) модели | — |
| **Power BI** | Визуализация (вне Docker) | — |

---

## Быстрый старт

### Требования

- Docker Desktop 4GB+ RAM
- Python 3.10+
- Git

### 1. Клонирование и настройка

```bash
git clone <your-repo-url> financial-analytics-platform
cd financial-analytics-platform

# Виртуальное окружение
python -m venv venv
venv\Scripts\activate          # Windows
# source venv/bin/activate     # Linux/Mac

pip install -r requirements.txt
cp .env.example .env
```

### 2. Запуск инфраструктуры

```bash
docker-compose up -d
```

Подождите 2–3 минуты, пока все сервисы стартуют.

| Сервис | URL | Логин |
|--------|-----|-------|
| Airflow UI | http://localhost:8080 | admin / admin |
| ClickHouse Play | http://localhost:8123/play | analyst / analyst123 |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Flower | http://localhost:5555 | — |

### 3. Проверка ClickHouse

```sql
-- http://localhost:8123/play
SHOW TABLES FROM market_db;
SELECT count() FROM market_db.market_ticks;
```

### 4. Локальный запуск pipeline (без Airflow)

```bash
python scripts/run_pipeline_local.py
```

### 5. Запуск коннекторов по отдельности

```bash
python -m ingestion.connectors.moex
python -m ingestion.connectors.binance
python -m ingestion.connectors.yahoo_finance
```

### 6. Airflow DAG

1. Откройте http://localhost:8080
2. Настройте connections (один раз):
   ```bash
   docker-compose exec airflow-webserver bash /opt/airflow/project/scripts/setup_airflow.sh
   ```
3. Найдите DAG `market_pipeline`, включите и нажмите ▶ Trigger

### 7. Telegram-алерты (опционально)

1. Создайте бота через [@BotFather](https://t.me/BotFather)
2. Узнайте chat_id через [@userinfobot](https://t.me/userinfobot)
3. Добавьте в `.env`:
   ```
   TELEGRAM_BOT_TOKEN=your_token
   TELEGRAM_CHAT_ID=your_chat_id
   ```

---

## Структура проекта

```
financial-analytics-platform/
├── config/
│   ├── settings.py          # Централизованная конфигурация
│   └── symbols.yaml         # Списки тикеров
├── ingestion/
│   ├── connectors/          # Binance, MOEX, Yahoo Finance
│   ├── validators/          # OHLCV валидация
│   ├── utils/               # Telegram notifier
│   └── schemas.py           # Единая схема данных
├── processing/
│   ├── spark_jobs/          # PySpark: SMA, RSI, volatility
│   └── loaders/             # ClickHouse, S3, Greenplum
├── models/
│   ├── anomaly/             # Z-score, Isolation Forest, GARCH, Ensemble
│   ├── risk/                # VaR (3 метода), Sharpe, CVaR
│   └── portfolio/           # Оптимизация Марковица
├── dags/
│   └── market_pipeline_dag.py  # Главный Airflow DAG
├── sql/
│   ├── clickhouse/          # Схемы + Materialized Views
│   └── greenplum/           # VaR, корреляции
├── scripts/
│   └── run_pipeline_local.py
├── tests/                   # pytest
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Компоненты

### Коннекторы (Ingestion)

Все коннекторы возвращают единый формат OHLCV:

| Колонка | Тип | Описание |
|---------|-----|----------|
| `ts` | datetime (UTC) | Начало свечи |
| `symbol` | str | Тикер (SBER, BTCUSDT) |
| `asset_type` | str | stock / crypto / forex |
| `source` | str | moex / binance / yahoo_finance |
| `open, high, low, close` | float | Цены |
| `volume` | float | Объём |
| `ingested_at` | datetime (UTC) | Время загрузки |

**Особенности:**
- Rate limiting между запросами
- Валидация OHLCV (high ≥ low, цены > 0)
- Обработка ошибок per-symbol (один упавший символ не ломает batch)
- Конвертация MOEX-времени из Europe/Moscow в UTC

### ClickHouse (OLAP)

Таблицы:
- `market_ticks` — сырые OHLCV (партиции по asset_type + месяц)
- `market_features` — технические индикаторы
- `anomalies` — обнаруженные аномалии
- `risk_metrics` — VaR, Sharpe, CVaR
- `mv_daily_ohlcv` — Materialized View для дневных свечей

### PySpark Features

| Фича | Формула | Описание |
|------|---------|----------|
| `log_return` | ln(close_t / close_{t-1}) | Логарифмический возврат |
| `sma_7`, `sma_30` | avg(close) за N периодов | Скользящие средние |
| `ema_14` | EMA за 14 периодов | Экспоненциальная MA |
| `volatility_14d` | std(log_return, 14) | Краткосрочная волатильность |
| `rsi_14` | 100 - 100/(1+RS) | Relative Strength Index |
| `volume_zscore` | (vol - μ) / σ | Аномальный объём |
| `price_range_pct` | (high - low) / close | Внутридневной диапазон |

```bash
python processing/spark_jobs/calculate_features.py
```

---

## Модели и формулы

### Детекция аномалий (ансамбль 2 из 3)

| Детектор | Метод | Сильные стороны |
|----------|-------|-----------------|
| **Z-score** | \|z\| > 3.0 (rolling 30) | Быстрый, интерпретируемый |
| **Isolation Forest** | sklearn, contamination=1% | Многомерный, без предположений о распределении |
| **GARCH(1,1)** | \|residual\| > 3σ | Учитывает кластеризацию волатильности |

**Severity:**
- score < 3 → low
- 3–5 → medium
- 5–8 → high
- ≥ 8 → critical (Telegram alert)

### Value at Risk (3 метода)

```
Исторический:  VaR = percentile(returns, 5%)
Параметрический: VaR = μ + z_0.05 × σ
Monte Carlo:   VaR = percentile(simulate(μ, σ, 10000), 5%)
CVaR(95%):     E[loss | loss > VaR(95%)]
```

### Портфельная оптимизация (Марковиц)

```
max Sharpe = (E[R_p] - R_f) / σ_p
subject to: Σw_i = 1, 0 ≤ w_i ≤ 0.3
```

---

## Airflow DAG

**DAG ID:** `market_pipeline`  
**Расписание:** `*/15 * * * *` (каждые 15 минут)

```
start
  ├── ingest_moex ──────────┐
  ├── ingest_yahoo_stocks ──┤
  ├── ingest_yahoo_forex ───┤
  └── ingest_crypto ────────┴── calculate_features ── spark_features_batch
                                        │
                               run_anomaly_detection
                                        │
                                  check_alerts (Branch)
                                    ├── send_telegram_alert ──┐
                                    └── no_alert ─────────────┤
                                                              │
                                    run_greenplum_analytics ──┴── run_risk_metrics
                                                                    └── run_portfolio ── end
```

**Параметры:**
- `catchup=False` — не запускать пропущенные периоды
- `max_active_runs=1` — один run одновременно
- `retries=3` с exponential backoff

---

## Power BI

### Подключение к ClickHouse

1. Установите [ClickHouse ODBC Driver](https://github.com/ClickHouse/clickhouse-odbc/releases)
2. Power BI → Получить данные → ODBC
3. Connection string:
   ```
   Driver={ClickHouse ODBC Driver};Server=localhost;Port=8123;Database=market_db;UID=analyst;PWD=analyst123
   ```

### Рекомендуемые дашборды

| Дашборд | Компоненты |
|---------|------------|
| **Market Overview** | Тепловая карта log_return, топ-5 волатильных, Fear Index |
| **Risk Dashboard** | VaR по 3 методам, Efficient Frontier, Sharpe/Sortino |
| **Anomaly Tracker** | Timeline аномалий, donut по severity, matrix детекторов |

### DAX: Fear Index

```dax
[Fear Index] =
VAR current_vol = AVERAGEX(LASTDATE(market_features[ts]), AVERAGE(market_features[volatility_14d]))
VAR hist_vol = AVERAGEX(FILTER(market_features, market_features[ts] < TODAY() - 30), market_features[volatility_14d])
RETURN DIVIDE(current_vol, hist_vol) * 100
```

---

## Тестирование

```bash
# Все тесты
pytest tests/ -v

# С покрытием
pytest tests/ --cov=ingestion --cov=models --cov-report=term-missing

# Только коннекторы
pytest tests/test_connectors.py -v

# Только модели
pytest tests/test_models.py -v
```

---

## Конфигурация

Все настройки в `.env` (см. `.env.example`):

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `CLICKHOUSE_HOST` | localhost | Хост ClickHouse |
| `MINIO_ENDPOINT` | http://localhost:9002 | MinIO API |
| `MINIO_BUCKET` | data-lake | Имя бакета |
| `GP_PORT` | 5433 | Analytics PostgreSQL |
| `TELEGRAM_BOT_TOKEN` | — | Токен бота |
| `SPARK_MASTER` | local[*] | Spark master |

Списки тикеров — `config/symbols.yaml`.

---

## Roadmap

- [x] Коннекторы: Binance, MOEX, Yahoo Finance (stocks + forex)
- [x] Единая схема OHLCV + валидация + Great Expectations
- [x] ClickHouse: схемы, MV, загрузчик
- [x] MinIO Data Lake с Hive-партициями
- [ ] PySpark + Pandas: полный набор фичей
- [x] Ансамбль аномалий (Z-score + IF + GARCH)
- [x] VaR (3 метода) + Sharpe + CVaR + Sortino + Drawdown
- [x] Оптимизация портфеля Марковица
- [x] Airflow DAG: полный pipeline (ingest → features → anomaly → GP → risk → portfolio)
- [ ] Telegram алерты
- [x] Greenplum/PostgreSQL analytics (returns, correlation, VaR)
- [x] pytest тесты (30+)
- [x] CI/CD (GitHub Actions)
- [x] Jupyter EDA notebook
- [ ] Power BI SQL-запросы
- [x] 55+ активов в config/symbols.yaml
- [ ] Power BI шаблон (.pbix) — создаётся вручную по powerbi/queries.sql
- [ ] Kubernetes деплой (production)

---

## Лицензия

MIT License. Проект создан в образовательных целях. Не является финансовой рекомендацией.

---
