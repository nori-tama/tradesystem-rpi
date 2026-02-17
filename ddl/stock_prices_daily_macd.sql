-- MACD (Moving Average Convergence Divergence) per code/day.
DROP TABLE IF EXISTS `stock_prices_daily_macd`;
CREATE TABLE `stock_prices_daily_macd` (
    trade_date DATE NOT NULL,
    code VARCHAR(12) NOT NULL,
    window_short INT NOT NULL DEFAULT 12,
    window_long INT NOT NULL DEFAULT 26,
    window_signal INT NOT NULL DEFAULT 9,
    ema_short DOUBLE,
    ema_long DOUBLE,
    macd DOUBLE,
    `signal` DOUBLE,
    histogram DOUBLE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, code, window_short, window_long, window_signal),
    KEY idx_code_windows (code, window_short, window_long, window_signal),
    KEY idx_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
