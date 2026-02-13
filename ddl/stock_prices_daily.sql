DROP TABLE IF EXISTS `stock_prices_daily`;
CREATE TABLE `stock_prices_daily` (
  `trade_date` DATE NOT NULL,
  `code` VARCHAR(8) NOT NULL,
  `open` DECIMAL(10,2) NOT NULL,
  `high` DECIMAL(10,2) NOT NULL,
  `low` DECIMAL(10,2) NOT NULL,
  `close` DECIMAL(10,2) NOT NULL,
  `volume` BIGINT NOT NULL,
  `ingested` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`trade_date`, `code`),
  KEY `idx_stock_prices_daily_code` (`code`),
  KEY `idx_stock_prices_daily_code_date` (`code`, `trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
