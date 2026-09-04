# Полный гид по проекту Financial Analytics Platform

Этот файл — подробный разбор проекта «на пальцах».  
Цель: чтобы ты понимал **каждый файл**, **куда текут данные** и **как всё запустить**, даже если раньше не работал с Airflow / ClickHouse / Spark.

Если коротко: система каждые 15 минут **скачивает цены** с бирж, **проверяет** их, **складывает в хранилища**, **считает индикаторы и риски**, **ищет аномалии** и при необходимости **пишет в Telegram**.

---

## 1. Идея проекта одной фразой

Представь конвейер на заводе:

1. **Сборщики** ходят на биржи и приносят свечи (OHLCV).
2. **Контроль качества** выкидывает мусор (отрицательные цены и т.п.).
3. **Склад сырья** (MinIO / S3) хранит «как пришло».
4. **Быстрая витрина** (ClickHouse) нужна для дашбордов и аналитики.
5. **Цех обработки** считает скользящие средние, RSI, доходности.
6. **Лаборатория** ищет странности (аномалии) и считает риск (VaR).
7. **Диспетчер** (Airflow) запускает всё по расписанию.
8. **Сигнал** (Telegram) приходит, если что-то критичное.

OHLCV — это стандарт биржевой свечи:

| Буква | Смысл |
|-------|--------|
| **O**pen | цена открытия периода |
| **H**igh | максимум |
| **L**ow | минимум |
| **C**lose | цена закрытия (самая важная) |
| **V**olume | объём торгов |

Все времена в проекте хранятся в **UTC** (всемирное время), чтобы не путать Москву, Нью-Йорк и биржи.

---

## 2. Карта папок (что где лежит)

```
financial-analytics-platform/
│
├── config/              настройки и списки тикеров
├── ingestion/           СБОР данных с API
├── processing/          ОБРАБОТКА и ЗАГРУЗКА в базы
├── models/              математика: аномалии, риск, портфель
├── dags/                расписание Airflow (главный конвейер)
├── sql/                 схемы таблиц ClickHouse и Postgres
├── scripts/             запуск руками, без Airflow
├── tests/               автопроверки кода
├── notebooks/           Jupyter для исследования данных
├── powerbi/             SQL для дашбордов
├── data/                тестовые CSV (результаты ручных запусков)
├── logs/                логи Airflow
├── plugins/             место под плагины Airflow (пока пусто)
│
├── docker-compose.yml   «одна кнопка» — поднять все сервисы
├── requirements.txt     список Python-библиотек
├── .env / .env.example  пароли и адреса (секреты)
├── README.md            краткий обзор
└── PROJECT_GUIDE.md     этот файл
```

Файлы `__init__.py` нужны Python, чтобы папка считалась пакетом (можно писать `from ingestion.connectors import ...`).  
Файлы `.gitkeep` нужны Git, чтобы пустые папки не исчезли из репозитория. Сами по себе они ничего не считают.

---

## 3. Как данные бегут по системе (последовательность)

Ниже — полный путь одной порции данных.

```
Биржи (MOEX / Binance / Yahoo)
        │
        ▼
Коннекторы  (ingestion/connectors/*.py)
        │  таблица одинакового вида: ts, symbol, open, high, low, close, volume...
        ▼
Валидация  (OHLCVValidator + Great Expectations)
        │  плохие строки выкидываются
        ▼
        ┌──────────────────┬─────────────────────┐
        ▼                  ▼                     │
   MinIO (S3)         ClickHouse              (сырьё)
   data-lake          market_ticks
   JSON-Lines
        │                  │
        │                  ▼
        │           calculate_features (pandas)
        │           + опционально Spark job
        │                  │
        │                  ▼
        │           ClickHouse: market_features
        │                  │
        ├──────────────────┤
        ▼                  ▼
   Ансамбль аномалий    Риск + портфель
   → ClickHouse         → ClickHouse: risk_metrics
     anomalies          → Postgres: market_returns,
                          correlation_matrix, risk_metrics_gp
        │
        ▼
   Если есть critical/high аномалии → Telegram
```

### Куда что попадает (хранилища)

| Место | Что хранит | Зачем |
|-------|------------|--------|
| **MinIO** бакет `data-lake` | сырые JSONL-файлы по партициям `source/asset_type/year=.../month=.../day=.../hour=...` | «озеро данных»: сырьё не портим, можно пересчитать всё заново |
| **ClickHouse** `market_ticks` | те же OHLCV, но в колоночной БД | быстрые отчёты, дашборды |
| **ClickHouse** `market_features` | SMA, RSI, log_return, волатильность… | вход для аномалий и риска |
| **ClickHouse** `anomalies` | найденные странные точки | трекер аномалий |
| **ClickHouse** `risk_metrics` | VaR, Sharpe, CVaR… | риск-дашборд |
| **ClickHouse** `mv_daily_ohlcv` | дневные свечи, считаются сами при вставке в ticks | удобные дневные графики |
| **Postgres analytics-db** `market_returns` | доходности | тяжёлая SQL-аналитика (как Greenplum в учебном roadmap) |
| **Postgres** `correlation_matrix` | корреляции пар активов | «кто с кем движется вместе» |
| **Postgres** `risk_metrics_gp` | VaR/Sharpe ещё раз в SQL-слое | дублирующий аналитический слой |

### Что делает главный DAG по шагам

Файл: `dags/market_pipeline_dag.py`. Расписание: каждые **15 минут**.

1. **start** — просто точка старта (пустая задача).
2. **Параллельно** четыре сбора:
   - `ingest_moex` — акции РФ
   - `ingest_yahoo_stocks` — акции США
   - `ingest_yahoo_forex` — валюты
   - `ingest_crypto` — крипта Binance  
   Каждый вызывает `ingest_dataframe`: валидация → MinIO → `market_ticks`.
3. **calculate_features** — читает `market_ticks` за 30 дней, считает индикаторы pandas’ом, пишет в `market_features`.
4. **spark_features_batch** — пытается запустить Spark-джоб (большой объём / Parquet в MinIO). Если Java/Spark нет — пишет skip и **не валит** весь DAG.
5. **run_anomaly_detection** — ансамбль из 3 детекторов, пишет в `anomalies`.
6. **check_alerts** — развилка:
   - если есть критичные аномалии → `send_telegram_alert`
   - иначе → `no_alert`
7. **run_greenplum_analytics** — копирует доходности в Postgres, считает корреляции и VaR.
8. **run_risk_metrics** — те же риск-метрики в ClickHouse.
9. **run_portfolio** — веса портфеля по Марковицу (Sharpe).
10. **end** — финиш.

---

## 4. Как запустить (пошагово, как для новичка)

Нужно:

- установленный **Docker Desktop**
- **Python 3.10+**
- интернет (чтобы качать данные с бирж)

### Шаг A. Один раз настроить Python

Открой PowerShell в папке проекта:

```powershell
cd C:\Users\ovsep\financial-analytics-platform

python -m venv venv
.\venv\Scripts\activate

pip install -r requirements.txt

copy .env.example .env
```

Файл `.env` — твои локальные пароли. Его **нельзя** выкладывать в Git (он в `.gitignore`).

### Шаг B. Поднять всю инфраструктуру

```powershell
docker compose up -d
```

Подожди 2–3 минуты. Потом открой в браузере:

| Что | Адрес | Логин |
|-----|--------|--------|
| Airflow (диспетчер) | http://localhost:8080 | `admin` / `admin` |
| ClickHouse (SQL-игра) | http://localhost:8123/play | пользователь `analyst`, пароль `analyst123` |
| MinIO (файловое S3) | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| Flower (воркеры Airflow) | http://localhost:5555 | — |

Остановить всё:

```powershell
docker compose down
```

Данные в Docker-томах при `down` **не удаляются**. Чтобы стереть базы полностью: `docker compose down -v`.

### Шаг C. (Опционально) Connections в Airflow

```powershell
docker compose exec airflow-webserver bash /opt/airflow/project/scripts/setup_airflow.sh
```

На Windows то же самое можно прочитать в `scripts/setup_airflow.ps1`.

### Шаг D. Два способа прогнать логику

**Вариант 1 — без Airflow (удобно отлаживать):**

```powershell
.\venv\Scripts\activate
python scripts\run_pipeline_local.py
```

Скрипт сам сходит на биржи (не все 55 тикеров, а первые 5 из каждой группы — чтобы не ждать час), положит данные в MinIO/ClickHouse, посчитает фичи, аномалии, портфель.

**Вариант 2 — как «в проде»:**

1. Открой http://localhost:8080
2. Найди DAG `market_pipeline`
3. Включи тумблер слева
4. Нажми ▶ (Trigger)

Логи каждой задачи — клик по квадратику → Log.

### Шаг E. Проверить, что данные появились

В ClickHouse Play:

```sql
SELECT count() FROM market_db.market_ticks;
SELECT count() FROM market_db.market_features;
SELECT * FROM market_db.anomalies LIMIT 20;
```

В MinIO Console: бакет `data-lake`, папки `moex/`, `binance/`, `yahoo_finance/`.

### Шаг F. Тесты кода (не ходят в Docker обязательно)

```powershell
pytest tests/ -v
```

### Шаг G. Jupyter (исследование)

```powershell
jupyter notebook notebooks\01_eda.ipynb
```

### Шаг H. Telegram (по желанию)

1. В Telegram: `@BotFather` → `/newbot` → получишь токен.
2. `@userinfobot` → узнаешь свой `chat_id`.
3. Впиши в `.env`:

```
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

Без этого алерты просто логируются как «Telegram не настроен».

### Шаг I. Отдельные коннекторы (посмотреть сырые данные)

```powershell
python -m ingestion.connectors.moex
python -m ingestion.connectors.binance
python -m ingestion.connectors.yahoo_finance
```

Они сохранят CSV в папку `data/`.

---

## 5. Порты — кто с кем разговаривает

Важно не путать **два Postgres** и **два порта 9000**.

| Сервис | Порт на твоём компьютере | Внутри Docker | Зачем |
|--------|--------------------------|---------------|--------|
| Airflow UI | 8080 | 8080 | веб-интерфейс |
| Postgres **Airflow** | 5432 | 5432 | метаданные DAG, логи запусков |
| Postgres **аналитика** | **5433** | 5432 контейнера `analytics-db` | «Greenplum»-слой |
| Redis | не проброшен наружу | 6379 | очередь задач Celery |
| ClickHouse HTTP | 8123 | 8123 | браузер / Power BI ODBC |
| ClickHouse Native | 9000 | 9000 | Python `clickhouse-driver` |
| MinIO API | **9002** | 9000 | boto3 / Spark с хоста |
| MinIO Console | 9001 | 9001 | веб-UI файлов |
| Flower | 5555 | 5555 | мониторинг воркеров |

Из **Airflow-контейнера** ClickHouse зовут `clickhouse`, MinIO — `http://minio:9000`, аналитическая БД — `analytics-db`.  
С **твоего ноутбука** ClickHouse — `localhost:9000`, MinIO — `http://localhost:9002`, аналитика — `localhost:5433`.

Это задано в `config/settings.py` и переменных `docker-compose.yml`.

---

## 6. Разбор каждого файла

### Корень проекта

#### `docker-compose.yml`

Это «рецепт кухни»: какие контейнеры поднять.

Сервисы:

- `postgres` — база Airflow
- `analytics-db` — Postgres 15 под аналитику; при старте выполняет `sql/greenplum/schema_and_var.sql`
- `redis` — очередь задач
- `airflow-init` — создаёт БД Airflow и пользователя admin
- `airflow-webserver`, `scheduler`, `worker`, `triggerer`, `flower` — сам оркестратор
- `clickhouse` — OLAP; при старте выполняет SQL из `sql/clickhouse/`
- `minio` — S3
- `minio-init` — создаёт бакет `data-lake`

Папка проекта монтируется в контейнер как `/opt/airflow/project`, поэтому DAG видит твой Python-код.

#### `requirements.txt`

Список библиотек: pandas, yfinance, clickhouse-driver, pyspark, airflow, sklearn, arch, pytest и т.д.  
`pip install -r requirements.txt` ставит их в venv.

#### `.env.example`

Шаблон настроек без секретов. Копируешь в `.env`.

#### `.env`

Твои реальные значения (хосты, пароли, токен Telegram). Не коммитится.

#### `.gitignore`

Что Git не должен хранить: `.env`, `venv/`, логи, кэш pytest, CSV в `data/` и т.д.

#### `LICENSE`

MIT — можно показывать в портфолио, копировать код с указанием копирайта.

#### `pytest.ini`

Настройки pytest: откуда брать тесты, как показывать ошибки.

#### `README.md`

Короткий «витринный» текст для GitHub: стек, быстрый старт, формулы.

#### `PROJECT_GUIDE.md`

Этот подробный учебник.

#### `.github/workflows/ci.yml`

Автотесты на GitHub: при push ставит Python, гоняет pytest и flake8. Локально этот файл сам не запускается.

---

### Папка `config/`

#### `config/settings.py`

**Один вход** для адресов и паролей. Читает `.env` через `python-dotenv`.

Отсюда берутся:

- ClickHouse host/port/user
- MinIO endpoint, ключи, бакет
- Postgres аналитики (GP_*)
- Telegram
- Spark master / память
- паузы между запросами к API (rate limit)

Если поменять порт MinIO — меняешь здесь (или в `.env`), а не в десяти файлах.

#### `config/symbols.yaml`

Списки тикеров: MOEX, Yahoo stocks, Binance crypto, Yahoo forex.  
Сейчас **больше 50** инструментов. DAG и локальный скрипт читают этот файл.

---

### Папка `ingestion/` — сбор данных

#### `ingestion/__init__.py`

Пустой маркер пакета.

#### `ingestion/schemas.py`

**Единый формат** всех источников.

Колонки, которые обязаны быть:

`ts`, `symbol`, `asset_type`, `source`, `open`, `high`, `low`, `close`, `volume`, `ingested_at`

Функция `normalize_ohlcv()` приводит любой DataFrame к этим колонкам (лишнее отбрасывает, недостающее заполняет).  
Поэтому Sber из MOEX и BTC из Binance выглядят одинаково и могут лежать в одной таблице.

#### `ingestion/connectors/binance.py`

Класс `BinanceConnector`.

- Ходит на `https://api.binance.com/api/v3/klines`
- Свечи крипты (BTCUSDT и др.)
- Между запросами `sleep` (rate limit)
- Парсит массив массивов Binance в таблицу
- Проверяет: цены > 0, high ≥ low, нет дублей
- `main()` внизу файла — тестовый запуск из командной строки

`asset_type = crypto`, `source = binance`.

#### `ingestion/connectors/moex.py`

Класс `MoexConnector`.

- API Московской биржи ISS
- Акции TQBR (SBER, GAZP…)
- Время приходит как Москва **без таймзоны** → код помечает `Europe/Moscow` и переводит в UTC
- Интервалы: 1m, 10m, 1h, 1d (так умеет MOEX)

`asset_type = stock`, `source = moex`.

#### `ingestion/connectors/yahoo_finance.py`

Класс `YahooFinanceConnector`.

- Библиотека `yfinance` (Yahoo Finance)
- Акции США и валютные пары (`EURUSD=X`)
- `asset_type` передаётся аргументом: `stock` или `forex`
- `source = yahoo_finance`

#### `ingestion/connectors/__init__.py`

Реэкспорт трёх классов, чтобы импортировать коротко.

#### `ingestion/validators/ohlcv_validator.py`

Класс `OHLCVValidator`.

Проверяет:

- есть ли нужные колонки
- нет ли NULL в ts/symbol/close
- цены положительные
- логика свечи (high не ниже low и т.д.)
- объём ≥ 0
- дубли (ts + symbol)

`validate()` — отчёт (passed / errors / warnings).  
`clean()` — выкидывает плохие строки.  
`hard_fail=True` — если данные совсем плохие, **останавливает** pipeline (так делает `ingest_dataframe`).

#### `ingestion/validators/great_expectations_validator.py`

Обёртка над библиотекой Great Expectations (промышленный фреймворк качества данных).  
Если GE не установился или упал — **тихо переключается** на `OHLCVValidator`.  
Для новичка: «вторая проверка качества, если получится».

#### `ingestion/validators/__init__.py`

Экспорт валидатора.

#### `ingestion/utils/telegram_notifier.py`

Функция `send_telegram_message()`.  
POST на `api.telegram.org`. Если токена нет — предупреждение в лог, без падения программы.

---

### Папка `processing/` — обработка и запись

#### `processing/pipeline_helpers.py`

Главный «клей» ingestion:

`ingest_dataframe(df, source, asset_type)`:

1. GE-валидация (мягкая)
2. OHLCV hard-валидация
3. `clean()`
4. запись в MinIO через `S3Writer`
5. вставка в ClickHouse `market_ticks`

Возвращает `(число_строк, s3_ключ)`. Это вызывают все ingest-таски DAG.

#### `processing/features/feature_calculator.py`

**Pandas-версия** расчёта фичей (то, что реально крутится в Airflow всегда).

По каждому `symbol` отдельно:

- `log_return` = ln(close / предыдущий close)
- SMA 7 и 30
- EMA 14
- волатильность 14 и 30 (стд. отклонение доходностей)
- z-score объёма
- внутридневной размах `(high-low)/close`
- RSI 14

Первая строка каждого тикера без «вчерашней» цены отбрасывается.

Результат идёт в `market_features`.

#### `processing/spark_jobs/calculate_features.py`

**Та же идея**, но на **PySpark** (для больших данных).

- Читает JSONL из MinIO (`s3a://data-lake/...`)
- Считает похожие колонки оконными функциями Spark
- Пишет **Parquet** в `s3a://data-lake/processed/features`

Нужны Java и пакеты Hadoop-AWS. На обычном ноутбуке без Java задача DAG может пропуститься (`|| echo Spark skipped`).  
Для портфолио это показывает: «умею и pandas, и Spark».

#### `processing/jobs/analytics_jobs.py`

Работа с Postgres-аналитикой и портфелем:

- `load_returns_to_gp` — колонки ts/symbol/close/log_return/volume → `market_returns`
- `calculate_correlation_matrix` — корреляции пар символов → `correlation_matrix`
- `calculate_gp_risk_metrics` — VaR/Sharpe → `risk_metrics_gp`
- `run_portfolio_optimization` — вызывает Марковица, возвращает веса

#### `processing/loaders/s3_writer.py`

Класс `S3Writer` (библиотека boto3).

- Создаёт бакет, если его нет
- Берёт **первую** дату в DataFrame и строит Hive-путь  
  пример: `moex/stock/year=2024/month=06/day=03/hour=07/data.jsonl`
- Пишет JSON Lines (каждая строка файла — один JSON-объект)

Минус текущего упрощения: весь кусок данных за один вызов пишется в **один** hour-партишн по первой строке. Для учебного проекта это нормально; в проде резали бы по каждому часу отдельно.

#### `processing/loaders/clickhouse_loader.py`

Класс `ClickHouseLoader`.

- `insert_dataframe` — вставка батчами по 10 000 строк (Native protocol, порт 9000)
- `execute_query` — SELECT → pandas
- `get_last_ts` — последняя дата по символу (для инкремента; символ передаётся **параметром**, не склейкой в SQL)
- короткие методы: `insert_features`, `insert_anomalies`, `insert_risk_metrics`

#### `processing/loaders/greenplum_loader.py`

Класс `GreenplumLoader`.

Несмотря на имя, в Docker это **PostgreSQL**.  
Вставка через `COPY FROM STDIN` (быстро).  
Хост по умолчанию из settings: `localhost:5433`. Из Airflow — `analytics-db`.

---

### Папка `models/` — математика

#### `models/anomaly/zscore_detector.py`

Правило: если значение сильно уехало от скользящего среднего (обычно |z| > 3) — аномалия.  
Простой и понятный метод.

#### `models/anomaly/isolation_forest_detector.py`

Алгоритм sklearn: «аномалии легче изолировать деревьями».  
Смотрит сразу несколько колонок: доходность, объём, волатильность, размах.

#### `models/anomaly/garch_detector.py`

Модель GARCH(1,1) из пакета `arch`: волатильность вчера влияет на сегодня.  
Аномалия, если фактическая доходность слишком большая относительно **прогнозной** волатильности.  
Нужно достаточно длинный ряд (~50+ точек). Если `arch` не встал — детектор просто возвращает пусто.

#### `models/anomaly/ensemble.py`

**Голосование:** точка считается аномалией, если **минимум 2 из 3** детекторов согласны.  
Считает `ensemble_score` и `severity`: low / medium / high / critical.  
`count_critical()` — сколько high+critical (для Telegram).

#### `models/risk/var_calculator.py`

VaR — «с вероятностью 95% убыток за день не хуже вот этой цифры» (обычно отрицательное число).

Три метода:

1. **Исторический** — просто 5-й перцентиль прошлых доходностей
2. **Параметрический** — формула нормального распределения μ + zσ
3. **Monte Carlo** — много случайных сценариев

Плюс **CVaR** — средний убыток в хвосте хуже VaR.

#### `models/risk/metrics.py`

- Sharpe, Sortino
- максимальная просадка (drawdown)
- `calculate_risk_metrics(df, symbol)` — собирает всё в один словарь для записи в БД

#### `models/portfolio/markowitz.py`

Классика: найти веса активов, чтобы **Sharpe был максимальный**.  
Ограничения: сумма весов = 1, каждый вес от 0 до 30% (без коротких продаж).  
`efficient_frontier()` — упрощённая кривая для графиков.

---

### Папка `dags/`

#### `dags/market_pipeline_dag.py`

Сердце автоматизации. Airflow **подхватывает любой .py в папке dags**.

Важно:

- `schedule_interval="*/15 * * * *"` — cron «каждые 15 минут»
- `catchup=False` — не догоняет пропущенные дни, если компьютер был выключен
- `max_active_runs=1` — не запускает второй прогон, пока первый не закончился
- `retries=3` — при ошибке сети повторит задачу

XCom — маленькие записки между задачами («сколько строк скачали», «сколько аномалий»). Большие таблицы через XCom **не** передают — только счётчики и пути.

---

### Папка `sql/`

#### `sql/clickhouse/01_schema.sql`

Создаёт БД `market_db` и таблицы:

- `market_ticks`
- `market_features`
- `anomalies`
- `risk_metrics`

Движок **MergeTree**: колоночное хранение, партиции по месяцу, сортировка `(symbol, ts)` — быстро фильтровать «дай Сбер за март».

ClickHouse при первом старте контейнера сам выполняет файлы из `/docker-entrypoint-initdb.d` (у нас примонтирована папка `sql/clickhouse`).

#### `sql/clickhouse/02_materialized_views.sql`

Materialized View `mv_daily_ohlcv`: при каждой вставке в `market_ticks` ClickHouse **на лету** копит дневные open/high/low/close/volume.  
Чтобы прочитать итог, нужны функции `argMinMerge` / `maxMerge` и т.д. (пример в `powerbi/queries.sql`).

#### `sql/greenplum/schema_and_var.sql`

Таблицы аналитического Postgres. Внизу закомментированный пример VaR через `PERCENTILE_CONT` — так бы считали в настоящем Greenplum/Postgres SQL.

---

### Папка `scripts/`

#### `scripts/run_pipeline_local.py`

Весь конвейер **одним файлом**, без Airflow. Удобно отлаживать и демонстрировать.  
Тикеров меньше, чем в полном DAG (срезы `[:5]`), чтобы быстро.

#### `scripts/setup_airflow.sh`

Bash: создаёт Connections в Airflow (clickhouse, postgres, minio).

#### `scripts/setup_airflow.ps1`

Подсказка для Windows, как вызвать тот же sh внутри контейнера.

---

### Папка `tests/`

Автотесты **не обязаны** поднимать Docker. Они проверяют логику на искусственных таблицах.

| Файл | Что проверяет |
|------|----------------|
| `conftest.py` | общие фикстуры: готовый тестовый OHLCV и фичи |
| `test_connectors.py` | схема, парсинг Binance/MOEX, ошибка на кривой интервал |
| `test_validators.py` | валидатор пропускает хорошее и режет отрицательные цены |
| `test_features.py` | появляются log_return, SMA, RSI |
| `test_models.py` | VaR отрицательный, 99% «хуже» 95%, ансамбль, Марковиц |
| `test_analytics.py` | корреляции и оптимизация не падают |
| `test_dag.py` | в DAG-файле есть все task_id, в yaml ≥ 50 тикеров |

---

### Остальное

#### `notebooks/01_eda.ipynb`

Живой блокнот: скачать MOEX → валидация → фичи → график цены/волатильности → аномалии.  
Для понимания данных глазами, не для продакшена.

#### `powerbi/queries.sql`

Готовые SELECT для Power BI / DBeaver. Сам файл `.pbix` нужно собрать руками в Power BI Desktop (это GUI Microsoft, его нельзя честно «сгенерировать» кодом в этом репозитории).

Подключение ODBC:

```
Driver={ClickHouse ODBC Driver};Server=localhost;Port=8123;Database=market_db;UID=analyst;PWD=analyst123
```

Идея дашбордов из roadmap:

1. Market Overview — цены, теплокарта доходностей
2. Risk — VaR тремя методами, Sharpe
3. Anomaly Tracker — таблица аномалий, severity

#### `data/test_binance.csv`, `data/test_moex.csv`

Следы прошлых ручных запусков коннекторов. Не обязательны для работы Docker.

#### `plugins/`

Пустая заготовка Airflow (свои операторы). Сейчас не используется.

#### `logs/`

Сюда Airflow пишет логи запусков (том из docker-compose).

---

## 7. Словарь терминов простыми словами

| Слово | Простыми словами |
|-------|------------------|
| **API** | «окошко» сайта биржи: ты просишь JSON с ценами |
| **DataFrame** | таблица в Python (как Excel, но в коде) |
| **Data Lake** | склад сырых файлов (у нас MinIO) |
| **OLAP** | база, заточенная под отчёты «посчитать среднее за год» (ClickHouse) |
| **DAG** | граф задач без циклов: A потом B, C параллельно |
| **Оркестрация** | робот-диспетчер, который запускает шаги по часам |
| **Партиция** | папка/кусок таблицы за месяц или час, чтобы не сканировать всё |
| **Rate limit** | биржа просит не долбить API слишком часто |
| **Log-return** | логарифм отношения цен — удобная доходность для статистики |
| **VaR** | оценка «насколько больно может быть в плохой день» |
| **Ансамбль** | несколько моделей голосуют, чтобы меньше ложных тревог |
| **XCom** | записка между задачами Airflow |

---

## 8. Что смотреть, если сломалось

| Симптом | Куда смотреть |
|---------|----------------|
| `docker compose` не стартует | мало RAM у Docker (нужно ~4 ГБ), занят порт 8080/5432/9000 |
| Airflow UI пустой / DAG Import Error | логи `airflow-scheduler`: не импортировался модуль, PYTHONPATH |
| MinIO connection refused с ноутбука | endpoint должен быть `http://localhost:9002`, не 9000 |
| ClickHouse insert error | таблицы ещё не созданы — пересоздай контейнер или выполни SQL вручную |
| Пустой MOEX | выходные/праздники РФ, или тикер не на TQBR |
| Yahoo пустой | лимиты Yahoo, нужен VPN иногда |
| Spark skipped | нет Java / Hadoop — для учёбы достаточно pandas-фичей |
| Telegram не уходит | пустые `TELEGRAM_*` в `.env` |

Логи Docker:

```powershell
docker compose logs -f airflow-scheduler
docker compose logs -f clickhouse
```

---

## 9. Как рассказывать это на собеседовании (2 минуты)

«Я собрал data platform для рыночных данных. Источники — MOEX, Binance и Yahoo. Сырьё кладу в S3-совместимый Data Lake (MinIO) с Hive-партициями и одновременно в ClickHouse для OLAP. Фичи (log-return, SMA, RSI, волатильность) считаю pandas’ом в Airflow и дублирую Spark-джобой в Parquet. Аномалии ловлю ансамблем Z-score + Isolation Forest + GARCH. Риск — три метода VaR, CVaR, Sharpe; портфель — Марковиц. Аналитический SQL-слой на Postgres имитирует Greenplum. Оркестрация — Airflow Celery каждые 15 минут, алерты в Telegram. Есть pytest и GitHub Actions.»

---

## 10. Чеклист «я понимаю свой проект»

Отметь себе:

- [ ] Знаю, что OHLCV и зачем UTC
- [ ] Могу назвать три источника и три хранилища
- [ ] Понимаю разницу `market_ticks` и `market_features`
- [ ] Знаю, зачем MinIO, если уже есть ClickHouse (сырьё vs витрина)
- [ ] Могу нарисовать путь от `ingest_moex` до Telegram
- [ ] Умею поднять `docker compose` и открыть Airflow
- [ ] Умею запустить `scripts/run_pipeline_local.py`
- [ ] Знаю, где поменять список тикеров (`config/symbols.yaml`)
- [ ] Знаю, где пароли (`.env`, не в Git)

Если все пункты ясны — ты уже не «чайник», а человек, который может защитить архитектуру.
