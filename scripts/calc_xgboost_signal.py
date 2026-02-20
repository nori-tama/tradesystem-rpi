#!/usr/bin/env python3
"""Train XGBoost on technical features and persist test-period signals."""

import argparse
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd
import pymysql
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
from xgboost import XGBClassifier

from common.db import get_connection
from common.logger import get_logger


NUMERIC_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "ma5",
    "ma25",
    "rsi",
    "macd",
    "macd_signal",
    "histogram",
]

FEATURE_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume_log",
    "close_return_1d",
    "close_return_5d",
    "volume_change_1d",
    "ma_gap",
    "ma_ratio",
    "rsi",
    "macd",
    "macd_signal",
    "histogram",
    "range_pct",
    "oc_change",
    "code_id",
]

SOURCE_TABLE = "stock_prices_daily"
MA_TABLE = "stock_prices_daily_ma"
RSI_TABLE = "stock_prices_daily_rsi"
MACD_TABLE = "stock_prices_daily_macd"
TARGET_TABLE = "stock_prices_daily_xgb_signal"

# RSI算出時に参照する期間（日数）。
RSI_WINDOW = 14
# MACD短期EMAの期間（日数）。
MACD_WINDOW_SHORT = 12
# MACD長期EMAの期間（日数）。
MACD_WINDOW_LONG = 26
# MACDシグナル線EMAの期間（日数）。
MACD_WINDOW_SIGNAL = 9
# 何営業日先の終値上昇を予測対象にするか。
HORIZON = 5
# 時系列分割時の学習データ比率（残りは評価データ）。
TRAIN_RATIO = 0.8
# 学習を実行するために必要な最小学習行数。
MIN_TRAIN_ROWS = 350
# 予測結果保存時に付与するモデル識別名。
MODEL_VERSION = "xgb_cls_v2"
# 確率を売買シグナル(0/1)へ変換する閾値。
PREDICTION_THRESHOLD = 0.5
# XGBoostの決定木本数。
N_ESTIMATORS = 400
# 各決定木の最大深さ。
MAX_DEPTH = 5
# 勾配ブースティングの学習率。
LEARNING_RATE = 0.05
# 学習時にサンプル行を使用する比率。
SUBSAMPLE = 0.9
# 学習時にサンプル列を使用する比率。
COLSAMPLE_BYTREE = 0.9
# 乱数シード（再現性確保用）。
RANDOM_STATE = 42


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train XGBoost signal model from daily technical features."
    )
    parser.add_argument(
        "--codes",
        default="",
        help="銘柄コードをカンマ区切りで指定。省略時はDBから取得。",
    )
    return parser.parse_args()


def resolve_codes(conn: pymysql.Connection, codes_arg: str) -> List[str]:
    if codes_arg:
        return [code.strip() for code in codes_arg.split(",") if code.strip()]

    sql = "SELECT DISTINCT code FROM tse_listings ORDER BY code"
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return [row[0] for row in cursor.fetchall()]


def build_feature_sql(
    source_table: str,
    ma_table: str,
    rsi_table: str,
    macd_table: str,
    code_count: int,
) -> str:
    code_placeholders = ", ".join(["%s"] * code_count)
    return f"""
    SELECT
        p.trade_date,
        p.code,
        p.open,
        p.high,
        p.low,
        p.close,
        p.volume,
        ma.ma5,
        ma.ma25,
        rsi.rsi,
        macd.macd,
        macd.`signal` AS macd_signal,
        macd.histogram
    FROM `{source_table}` p
    LEFT JOIN `{ma_table}` ma
      ON p.code = ma.code
     AND p.trade_date = ma.trade_date
    LEFT JOIN `{rsi_table}` rsi
      ON p.code = rsi.code
     AND p.trade_date = rsi.trade_date
     AND rsi.`window` = %s
    LEFT JOIN `{macd_table}` macd
      ON p.code = macd.code
     AND p.trade_date = macd.trade_date
     AND macd.window_short = %s
     AND macd.window_long = %s
     AND macd.window_signal = %s
    WHERE p.code IN ({code_placeholders})
    ORDER BY p.trade_date, p.code
    """


def fetch_feature_rows(
    conn: pymysql.Connection,
    codes: List[str],
) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame()

    sql = build_feature_sql(
        SOURCE_TABLE,
        MA_TABLE,
        RSI_TABLE,
        MACD_TABLE,
        len(codes),
    )

    params: List = [
        RSI_WINDOW,
        MACD_WINDOW_SHORT,
        MACD_WINDOW_LONG,
        MACD_WINDOW_SIGNAL,
    ]
    params.extend(codes)

    with conn.cursor() as cursor:
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()

    columns = [
        "trade_date",
        "code",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "ma5",
        "ma25",
        "rsi",
        "macd",
        "macd_signal",
        "histogram",
    ]
    return pd.DataFrame(rows, columns=columns)


def build_dataset(df: pd.DataFrame, horizon: int) -> pd.DataFrame:
    if df.empty:
        return df

    data = df.copy()
    data["trade_date"] = pd.to_datetime(data["trade_date"])
    for column in NUMERIC_COLUMNS:
        data[column] = pd.to_numeric(data[column], errors="coerce")

    data = data.sort_values(["code", "trade_date"]).reset_index(drop=True)

    grouped = data.groupby("code")
    data["future_close"] = grouped["close"].shift(-horizon)
    data["actual_return"] = (data["future_close"] / data["close"]) - 1.0
    data["label"] = (data["actual_return"] > 0).astype(float)
    data.loc[data["future_close"].isna(), "label"] = np.nan

    data["range_pct"] = (data["high"] - data["low"]) / data["close"]
    data["oc_change"] = (data["close"] - data["open"]) / data["open"]
    data["ma_gap"] = (data["ma5"] - data["ma25"]) / data["close"]
    data["ma_ratio"] = data["ma5"] / data["ma25"]
    data["volume_log"] = np.log1p(data["volume"])
    data["close_return_1d"] = grouped["close"].pct_change(1)
    data["close_return_5d"] = grouped["close"].pct_change(5)
    data["volume_change_1d"] = grouped["volume"].pct_change(1)
    data["code_id"] = pd.factorize(data["code"])[0].astype(float)

    data = data.replace([np.inf, -np.inf], np.nan)
    return data


def split_by_date(df: pd.DataFrame, train_ratio: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    labeled = df[df["label"].notna()].copy()
    unique_dates = sorted(labeled["trade_date"].unique())
    if len(unique_dates) < 2:
        raise ValueError("学習/評価に必要な日付数が不足しています。")

    split_idx = int(len(unique_dates) * train_ratio)
    split_idx = max(1, min(split_idx, len(unique_dates) - 1))
    split_date = unique_dates[split_idx - 1]

    train_df = labeled[labeled["trade_date"] <= split_date].copy()
    test_df = labeled[labeled["trade_date"] > split_date].copy()
    return train_df, test_df


def make_train_test_matrix(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series, pd.DataFrame]:
    train_matrix = train_df.dropna(subset=FEATURE_COLUMNS + ["label"]).copy()
    test_matrix = test_df.dropna(subset=FEATURE_COLUMNS + ["label", "actual_return"]).copy()

    if train_matrix.empty:
        raise ValueError("前処理後の学習データが0件です。")
    if test_matrix.empty:
        raise ValueError("前処理後の評価データが0件です。")

    x_train = train_matrix[FEATURE_COLUMNS]
    y_train = train_matrix["label"].astype(int)
    x_test = test_matrix[FEATURE_COLUMNS]
    y_test = test_matrix["label"].astype(int)
    return x_train, y_train, x_test, y_test, test_matrix


def build_model(y_train: pd.Series) -> XGBClassifier:
    positives = int((y_train == 1).sum())
    negatives = int((y_train == 0).sum())
    if positives == 0 or negatives == 0:
        raise ValueError("学習データが単一クラスです。学習期間や条件を見直してください。")

    scale_pos_weight = negatives / positives
    return XGBClassifier(
        n_estimators=N_ESTIMATORS,
        max_depth=MAX_DEPTH,
        learning_rate=LEARNING_RATE,
        subsample=SUBSAMPLE,
        colsample_bytree=COLSAMPLE_BYTREE,
        objective="binary:logistic",
        eval_metric="logloss",
        random_state=RANDOM_STATE,
        scale_pos_weight=scale_pos_weight,
        n_jobs=1,
    )


def evaluate_predictions(y_true: pd.Series, pred_prob: np.ndarray, pred_label: np.ndarray) -> Dict[str, float]:
    metrics: Dict[str, float] = {
        "accuracy": accuracy_score(y_true, pred_label),
        "f1": f1_score(y_true, pred_label, zero_division=0),
    }
    if len(np.unique(y_true)) >= 2:
        metrics["auc"] = roc_auc_score(y_true, pred_prob)
    else:
        metrics["auc"] = float("nan")
    return metrics


def make_persist_rows(
    test_df: pd.DataFrame,
    horizon: int,
    model_version: str,
    trained_end_date,
    pred_prob: np.ndarray,
    pred_label: np.ndarray,
) -> List[Tuple]:
    rows: List[Tuple] = []
    for row, probability, label_prediction in zip(
        test_df.itertuples(index=False),
        pred_prob,
        pred_label,
    ):
        rows.append(
            (
                row.trade_date.date(),
                row.code,
                horizon,
                model_version,
                trained_end_date,
                float(probability),
                int(label_prediction),
                int(row.label),
                float(row.actual_return),
            )
        )
    return rows


def upsert_rows(
    conn: pymysql.Connection,
    table: str,
    rows: Iterable[Tuple],
) -> int:
    sql = f"""
    INSERT INTO `{table}`
    (`trade_date`, `code`, `horizon`, `model_version`, `trained_end_date`,
     `predicted_prob`, `predicted_label`, `actual_label`, `actual_return`)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        `trained_end_date` = VALUES(`trained_end_date`),
        `predicted_prob` = VALUES(`predicted_prob`),
        `predicted_label` = VALUES(`predicted_label`),
        `actual_label` = VALUES(`actual_label`),
        `actual_return` = VALUES(`actual_return`)
    """
    with conn.cursor() as cursor:
        cursor.executemany(sql, list(rows))
        affected = cursor.rowcount
    conn.commit()
    return affected


def validate_config() -> None:
    if HORIZON <= 0:
        raise ValueError("horizon は1以上を指定してください。")
    if not (0.0 < TRAIN_RATIO < 1.0):
        raise ValueError("train-ratio は 0.0 より大きく 1.0 未満で指定してください。")
    if MIN_TRAIN_ROWS <= 0:
        raise ValueError("min-train-rows は1以上を指定してください。")
    if not (0.0 < PREDICTION_THRESHOLD < 1.0):
        raise ValueError("prediction-threshold は 0.0 より大きく 1.0 未満で指定してください。")


def process_one_code(
    conn: pymysql.Connection,
    logger,
    code: str,
) -> Dict[str, float]:
    features = fetch_feature_rows(conn, [code])
    if features.empty:
        logger.warning("[%s] 入力データが0件のためスキップします。", code)
        return {"status": 0.0, "affected": 0.0}

    dataset = build_dataset(features, HORIZON)
    train_df, test_df = split_by_date(dataset, TRAIN_RATIO)

    x_train, y_train, x_test, y_test, clean_test_df = make_train_test_matrix(train_df, test_df)
    if len(x_train) < MIN_TRAIN_ROWS:
        logger.warning(
            "[%s] 学習データ不足: %d < min-train-rows(%d) のためスキップします。",
            code,
            len(x_train),
            MIN_TRAIN_ROWS,
        )
        return {"status": 0.0, "affected": 0.0}

    model = build_model(y_train)
    model.fit(x_train, y_train)

    pred_prob = model.predict_proba(x_test)[:, 1]
    pred_label = (pred_prob >= PREDICTION_THRESHOLD).astype(int)
    metrics = evaluate_predictions(y_test, pred_prob, pred_label)

    logger.info("[%s] train rows: %d", code, len(x_train))
    logger.info("[%s] test rows: %d", code, len(x_test))
    logger.info("[%s] accuracy: %.6f", code, metrics["accuracy"])
    logger.info("[%s] f1: %.6f", code, metrics["f1"])
    if np.isnan(metrics["auc"]):
        logger.info("[%s] auc: N/A (テストデータが単一クラス)", code)
    else:
        logger.info("[%s] auc: %.6f", code, metrics["auc"])

    trained_end_date = train_df["trade_date"].max().date()
    rows = make_persist_rows(
        clean_test_df,
        HORIZON,
        MODEL_VERSION,
        trained_end_date,
        pred_prob,
        pred_label,
    )
    affected = upsert_rows(conn, TARGET_TABLE, rows)
    logger.info("[%s] 予測保存件数(affected rows): %d", code, affected)

    result: Dict[str, float] = {
        "status": 1.0,
        "affected": float(affected),
        "accuracy": float(metrics["accuracy"]),
        "f1": float(metrics["f1"]),
        "auc": float(metrics["auc"]),
    }
    return result


def main() -> None:
    args = parse_args()
    logger = get_logger("calc_xgboost_signal")
    validate_config()

    conn = get_connection()
    try:
        codes = resolve_codes(conn, args.codes)
        logger.info("対象銘柄数: %d", len(codes))
        if not codes:
            logger.warning("対象銘柄が0件のため終了します。")
            return

        success_count = 0
        skipped_count = 0
        failed_count = 0
        affected_total = 0
        metrics_rows: List[Dict[str, float]] = []

        for idx, code in enumerate(codes, start=1):
            logger.info("銘柄処理開始 (%d/%d): %s", idx, len(codes), code)
            try:
                result = process_one_code(conn, logger, code)
            except Exception as exc:
                failed_count += 1
                logger.exception("[%s] 処理失敗: %s", code, exc)
                continue

            if int(result["status"]) == 1:
                success_count += 1
                affected_total += int(result["affected"])
                metrics_rows.append(result)
            else:
                skipped_count += 1

        logger.info("===== XGBoost銘柄別処理サマリ =====")
        logger.info("成功: %d", success_count)
        logger.info("スキップ: %d", skipped_count)
        logger.info("失敗: %d", failed_count)
        logger.info("保存合計(affected rows): %d", affected_total)

        if metrics_rows:
            accuracy_avg = float(np.mean([row["accuracy"] for row in metrics_rows]))
            f1_avg = float(np.mean([row["f1"] for row in metrics_rows]))
            auc_values = [row["auc"] for row in metrics_rows if not np.isnan(row["auc"])]
            logger.info("平均accuracy: %.6f", accuracy_avg)
            logger.info("平均f1: %.6f", f1_avg)
            if auc_values:
                logger.info("平均auc: %.6f", float(np.mean(auc_values)))
            else:
                logger.info("平均auc: N/A (有効値なし)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
