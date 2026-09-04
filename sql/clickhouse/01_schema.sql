-- ClickHouse: основные таблицы платформы
CREATE DATABASE IF NOT EXISTS market_db;

-- Сырые тиковые данные
CREATE TABLE IF NOT EXISTS market_db.market_ticks
(
    ts          DateTime,
    symbol      String,
    asset_type  LowCardinality(String),
    source      LowCardinality(String),
    open        Float64,
    high        Float64,
    low         Float64,
    close       Float64,
    volume      Float64,
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY (asset_type, toYYYYMM(ts))
ORDER BY (symbol, ts)
SETTINGS index_granularity = 8192;

-- Рассчитанные технические индикаторы
CREATE TABLE IF NOT EXISTS market_db.market_features
(
    ts              DateTime,
    symbol          String,
    asset_type      LowCardinality(String),
    close           Float64,
    volume          Float64,
    log_return      Float64,
    sma_7           Float64,
    sma_30          Float64,
    ema_14          Float64,
    volatility_14d  Float64,
    volatility_30d  Float64,
    rsi_14          Float64,
    volume_zscore   Float64,
    price_range_pct Float64,
    calculated_at   DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts);

-- Обнаруженные аномалии
CREATE TABLE IF NOT EXISTS market_db.anomalies
(
    detected_at DateTime DEFAULT now(),
    ts          DateTime,
    symbol      String,
    anomaly_type LowCardinality(String),
    detector    LowCardinality(String),
    severity    LowCardinality(String),
    score       Float64,
    close_price Float64,
    description String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(detected_at)
ORDER BY (detected_at, symbol);

-- Риск-метрики
CREATE TABLE IF NOT EXISTS market_db.risk_metrics
(
    calculated_at   DateTime DEFAULT now(),
    symbol          String,
    var_95_hist     Float64,
    var_99_hist     Float64,
    var_95_param    Float64,
    var_99_param    Float64,
    var_95_mc       Float64,
    cvar_95         Float64,
    sharpe_ratio    Float64,
    sortino_ratio   Float64,
    max_drawdown    Float64,
    observations    UInt32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(calculated_at)
ORDER BY (symbol, calculated_at);
