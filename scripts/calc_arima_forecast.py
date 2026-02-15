#!/usr/bin/env python3
"""Forecast stock close prices with ARIMA and store to MySQL."""

import argparse
import math
import warnings
from typing import Iterable, List, Optional, Sequence, Tuple

import pymysql
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tools.sm_exceptions import ConvergenceWarning

from common.db import get_connection
from common.exchange_calendar import shift_exchange_business_day
from common.logger import get_logger


PREDICTED_CLOSE_MAX = 999999999.999999
PREDICTED_CLOSE_MIN = -999999999.999999


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ARIMAで株価終値を予測してMySQLに保存します。"
    )
    parser.add_argument(
        "--codes",
        default="",
        help="銘柄コードをカンマ区切りで指定。省略時はDBから取得。",
    )
    parser.add_argument("--source-table", default="stock_prices_daily")
    parser.add_argument("--target-table", default="stock_prices_daily_arima_forecast")
    parser.add_argument("--lookback", type=int, default=250)
    parser.add_argument("--horizon", type=int, default=5)
    parser.add_argument("--min-observations", type=int, default=60)
    parser.add_argument("--order", default="5,1,0")
    parser.add_argument("--fallback-order", default="1,1,1")
    return parser.parse_args()


def parse_order(order_text: str) -> Tuple[int, int, int]:
    parts = [part.strip() for part in order_text.split(",")]
    if len(parts) != 3:
        raise ValueError(f"order must be 'p,d,q': {order_text}")
    p, d, q = (int(parts[0]), int(parts[1]), int(parts[2]))
    if p < 0 or d < 0 or q < 0:
        raise ValueError(f"order values must be >= 0: {order_text}")
    return p, d, q


def resolve_codes(conn: pymysql.Connection, codes_arg: str) -> List[str]:
    if codes_arg:
        return [code.strip() for code in codes_arg.split(",") if code.strip()]

    sql = "SELECT DISTINCT code FROM tse_listings ORDER BY code"
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return [row[0] for row in cursor.fetchall()]


def fetch_recent_close_prices(
    conn: pymysql.Connection,
    table: str,
    code: str,
    lookback: int,
) -> List[Tuple]:
    sql = f"""
        SELECT t.trade_date, t.`close`
        FROM (
            SELECT trade_date, `close`
            FROM `{table}`
            WHERE code = %s
              AND `close` IS NOT NULL
            ORDER BY trade_date DESC
            LIMIT %s
        ) t
        ORDER BY t.trade_date
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (code, lookback))
        return list(cursor.fetchall())


def forecast_close_prices(
    closes: Sequence[float],
    horizon: int,
    primary_order: Tuple[int, int, int],
    fallback_order: Tuple[int, int, int],
    code: str,
    logger,
) -> Tuple[List[float], Tuple[int, int, int], Optional[float]]:
    try_orders = [primary_order]
    if fallback_order != primary_order:
        try_orders.append(fallback_order)

    last_error: Optional[Exception] = None
    for order in try_orders:
        try:
            model = ARIMA(
                list(closes),
                order=order,
                enforce_stationarity=False,
                enforce_invertibility=False,
            )
            with warnings.catch_warnings(record=True) as caught_warnings:
                warnings.simplefilter("always", ConvergenceWarning)
                result = model.fit()

            for caught in caught_warnings:
                if issubclass(caught.category, ConvergenceWarning):
                    logger.warning(
                        "%s ARIMA収束警告: order=%s, message=%s",
                        code,
                        f"{order[0]},{order[1]},{order[2]}",
                        str(caught.message),
                    )

            retvals = getattr(result, "mle_retvals", None)
            if isinstance(retvals, dict) and not retvals.get("converged", True):
                logger.warning(
                    "%s ARIMA未収束: order=%s, mle_retvals=%s",
                    code,
                    f"{order[0]},{order[1]},{order[2]}",
                    retvals,
                )

            forecast_values = result.forecast(steps=horizon)
            output = [float(value) for value in forecast_values]
            aic = float(result.aic) if result.aic is not None else None
            return output, order, aic
        except Exception as exc:
            last_error = exc

    raise RuntimeError(f"ARIMA fit failed: {last_error}")


def build_rows(
    code: str,
    forecast_base_date,
    predicted_values: Sequence[float],
    order: Tuple[int, int, int],
    train_points: int,
    aic: Optional[float],
    logger,
) -> List[Tuple]:
    rows: List[Tuple] = []
    order_text = f"{order[0]},{order[1]},{order[2]}"

    for step, predicted_close in enumerate(predicted_values, start=1):
        if not math.isfinite(predicted_close):
            logger.warning(
                "%s 予測値をスキップ: horizon=%d, order=%s, reason=not-finite, value=%s",
                code,
                step,
                order_text,
                predicted_close,
            )
            continue

        if predicted_close < PREDICTED_CLOSE_MIN or predicted_close > PREDICTED_CLOSE_MAX:
            logger.warning(
                "%s 予測値をスキップ: horizon=%d, order=%s, reason=out-of-range, value=%s",
                code,
                step,
                order_text,
                predicted_close,
            )
            continue

        target_trade_date = shift_exchange_business_day(forecast_base_date, step)
        rows.append(
            (
                forecast_base_date,
                code,
                step,
                target_trade_date,
                predicted_close,
                order_text,
                train_points,
                aic,
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
        (
            `forecast_base_date`,
            `code`,
            `horizon`,
            `target_trade_date`,
            `predicted_close`,
            `model_order`,
            `train_points`,
            `aic`
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            `target_trade_date` = VALUES(`target_trade_date`),
            `predicted_close` = VALUES(`predicted_close`),
            `model_order` = VALUES(`model_order`),
            `train_points` = VALUES(`train_points`),
            `aic` = VALUES(`aic`)
    """
    with conn.cursor() as cursor:
        row_list = list(rows)
        if not row_list:
            return 0
        cursor.executemany(sql, row_list)
        inserted = cursor.rowcount
    conn.commit()
    return inserted


def main() -> None:
    args = parse_args()
    logger = get_logger("calc_arima_forecast")

    if args.horizon <= 0:
        raise ValueError("--horizon は1以上を指定してください。")
    if args.lookback <= 0:
        raise ValueError("--lookback は1以上を指定してください。")
    if args.min_observations <= 1:
        raise ValueError("--min-observations は2以上を指定してください。")

    primary_order = parse_order(args.order)
    fallback_order = parse_order(args.fallback_order)

    conn = get_connection()
    try:
        codes = resolve_codes(conn, args.codes)
        total_codes = len(codes)
        predicted_codes = 0
        total_rows = 0
        inserted_rows = 0

        for code in codes:
            price_rows = fetch_recent_close_prices(
                conn, args.source_table, code, args.lookback
            )
            if len(price_rows) < args.min_observations:
                logger.info(
                    "%s データ不足: %d件 (必要: %d件)",
                    code,
                    len(price_rows),
                    args.min_observations,
                )
                continue

            closes = [float(row[1]) for row in price_rows if row[1] is not None]
            if len(closes) < args.min_observations:
                logger.info(
                    "%s 終値不足: %d件 (必要: %d件)",
                    code,
                    len(closes),
                    args.min_observations,
                )
                continue

            forecast_base_date = price_rows[-1][0]
            try:
                predicted_values, used_order, aic = forecast_close_prices(
                    closes=closes,
                    horizon=args.horizon,
                    primary_order=primary_order,
                    fallback_order=fallback_order,
                    code=code,
                    logger=logger,
                )
            except Exception as exc:
                logger.warning("%s ARIMA予測失敗: %s", code, exc)
                continue

            rows = build_rows(
                code=code,
                forecast_base_date=forecast_base_date,
                predicted_values=predicted_values,
                order=used_order,
                train_points=len(closes),
                aic=aic,
                logger=logger,
            )

            if not rows:
                logger.warning("%s 有効な予測値がないため保存をスキップ", code)
                continue

            inserted = upsert_rows(conn, args.target_table, rows)

            predicted_codes += 1
            total_rows += len(rows)
            inserted_rows += inserted
            logger.info(
                "%s 予測完了: horizon=%d, order=%s, rows=%d, inserted=%d",
                code,
                args.horizon,
                f"{used_order[0]},{used_order[1]},{used_order[2]}",
                len(rows),
                inserted,
            )

        logger.info("対象銘柄数: %d", total_codes)
        logger.info("予測成功銘柄数: %d", predicted_codes)
        logger.info("予測レコード数: %d", total_rows)
        logger.info("インサートレコード数: %d", inserted_rows)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
