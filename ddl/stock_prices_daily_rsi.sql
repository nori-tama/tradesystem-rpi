-- RSI (Relative Strength Index) per code/day.
DROP TABLE IF EXISTS `stock_prices_daily_rsi`;
CREATE TABLE `stock_prices_daily_rsi` (
    trade_date DATE NOT NULL,
    code VARCHAR(12) NOT NULL,
    `window` INT NOT NULL DEFAULT 14,
    rsi DOUBLE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, code, `window`),
    KEY idx_code_window (code, `window`),
    KEY idx_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
