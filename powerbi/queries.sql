# Power BI — SQL-запросы для подключения к ClickHouse
# Используйте в Power BI: Получить данные → ODBC → вставьте запрос

-- ============================================================
-- 1. MARKET OVERVIEW: последние 30 дней фичей
-- ============================================================
SELECT
    ts,
    symbol,
    asset_type,
    close,
    log_return,
    sma_7,
    sma_30,
    volatility_14d,
    rsi_14,
    volume_zscore
FROM market_db.market_features
WHERE ts >= now() - INTERVAL 30 DAY
ORDER BY symbol, ts;

-- ============================================================
-- 2. RISK DASHBOARD: последние риск-метрики
-- ============================================================
SELECT
    calculated_at,
    symbol,
    var_95_hist,
    var_99_hist,
    var_95_param,
    var_95_mc,
    cvar_95,
    sharpe_ratio,
    sortino_ratio,
    max_drawdown,
    observations
FROM market_db.risk_metrics
WHERE calculated_at >= now() - INTERVAL 7 DAY
ORDER BY calculated_at DESC, symbol;

-- ============================================================
-- 3. ANOMALY TRACKER: последние 50 аномалий
-- ============================================================
SELECT
    detected_at,
    ts,
    symbol,
    anomaly_type,
    detector,
    severity,
    score,
    close_price,
    description
FROM market_db.anomalies
WHERE detected_at >= now() - INTERVAL 30 DAY
ORDER BY detected_at DESC
LIMIT 50;

-- ============================================================
-- 4. DAILY OHLCV (из Materialized View)
-- ============================================================
SELECT
    symbol,
    date,
    argMinMerge(open)  AS open,
    maxMerge(high)     AS high,
    minMerge(low)      AS low,
    argMaxMerge(close) AS close,
    sumMerge(total_volume) AS volume
FROM market_db.mv_daily_ohlcv
GROUP BY symbol, date
ORDER BY symbol, date;

-- ============================================================
-- 5. FEAR INDEX (для DAX в Power BI)
-- ============================================================
SELECT
    toDate(ts) AS date,
    avg(volatility_14d) AS avg_volatility
FROM market_db.market_features
WHERE ts >= now() - INTERVAL 60 DAY
GROUP BY date
ORDER BY date;
