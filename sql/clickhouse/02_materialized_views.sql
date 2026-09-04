-- Materialized View: дневная агрегация OHLCV
CREATE MATERIALIZED VIEW IF NOT EXISTS market_db.mv_daily_ohlcv
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (symbol, date)
AS SELECT
    symbol,
    toDate(ts) AS date,
    argMinState(open, ts)  AS open,
    maxState(high)         AS high,
    minState(low)          AS low,
    argMaxState(close, ts) AS close,
    sumState(volume)       AS total_volume
FROM market_db.market_ticks
GROUP BY symbol, date;
