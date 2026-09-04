-- Greenplum / PostgreSQL Analytics Schema
-- В dev-окружении используется PostgreSQL (analytics-db) как Greenplum-совместимый слой

CREATE TABLE IF NOT EXISTS market_returns (
    ts          TIMESTAMP NOT NULL,
    symbol      VARCHAR(20) NOT NULL,
    asset_type  VARCHAR(10),
    close       DECIMAL(18,6),
    log_return  DECIMAL(18,8),
    volume      DECIMAL(20,2)
);

CREATE INDEX IF NOT EXISTS idx_market_returns_symbol_ts ON market_returns (symbol, ts);

CREATE TABLE IF NOT EXISTS correlation_matrix (
    calculated_date DATE NOT NULL,
    symbol_1        VARCHAR(20),
    symbol_2        VARCHAR(20),
    correlation     DECIMAL(8,6),
    observations    INTEGER,
    period_days     INTEGER DEFAULT 252
);

CREATE TABLE IF NOT EXISTS risk_metrics_gp (
    calculated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    symbol          VARCHAR(20) NOT NULL,
    var_95          DECIMAL(18,8),
    var_99          DECIMAL(18,8),
    cvar_95         DECIMAL(18,8),
    sharpe_ratio    DECIMAL(10,6),
    observations    INTEGER
);

-- VaR 95% / 99% через PERCENTILE_CONT (работает в PostgreSQL 9.4+)
-- SELECT
--     symbol,
--     PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY log_return) AS var_95,
--     PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY log_return) AS var_99,
--     COUNT(*) AS observations
-- FROM market_returns
-- WHERE ts >= CURRENT_TIMESTAMP - INTERVAL '252 days'
-- GROUP BY symbol
-- ORDER BY var_95 ASC;
