-- XGBoost binary signal output per code/day.
DROP TABLE IF EXISTS `stock_prices_daily_xgb_signal`;
CREATE TABLE `stock_prices_daily_xgb_signal` (
    trade_date DATE NOT NULL,
    code VARCHAR(12) NOT NULL,
    horizon INT NOT NULL,
    model_version VARCHAR(64) NOT NULL,
    trained_end_date DATE NOT NULL,
    predicted_prob DOUBLE,
    predicted_label TINYINT,
    actual_label TINYINT,
    actual_return DOUBLE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, code, horizon, model_version),
    KEY idx_model_trade_date (model_version, trade_date),
    KEY idx_code_trade_date (code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
