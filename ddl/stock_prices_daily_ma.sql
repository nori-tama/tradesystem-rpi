DROP TABLE IF EXISTS `stock_prices_daily_ma`;
CREATE TABLE `stock_prices_daily_ma` (
    trade_date DATE NOT NULL,
    code VARCHAR(10) NOT NULL,
    ma5 DECIMAL(15,6),
    ma25 DECIMAL(15,6),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
