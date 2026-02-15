-- ARIMA forecast (up to N business days ahead) per code/base date.
DROP TABLE IF EXISTS `stock_prices_daily_arima_forecast`;
CREATE TABLE `stock_prices_daily_arima_forecast` (
    forecast_base_date DATE NOT NULL,
    code VARCHAR(12) NOT NULL,
    horizon INT NOT NULL,
    target_trade_date DATE NOT NULL,
    predicted_close DECIMAL(15,6),
    model_order VARCHAR(20) NOT NULL,
    train_points INT NOT NULL,
    aic DOUBLE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (forecast_base_date, code, horizon),
    KEY idx_code_target_trade_date (code, target_trade_date),
    KEY idx_target_trade_date (target_trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
