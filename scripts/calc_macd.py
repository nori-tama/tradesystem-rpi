#!/usr/bin/env python3
"""Calculate MACD from daily stock prices and store to MySQL."""

import argparse
from typing import Iterable, List, Optional, Sequence, Tuple

import pymysql

from common.db import get_connection
from common.logger import get_logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calculate MACD and store to MySQL."
    )
    parser.add_argument(
        "--codes",
        default="",
        help="銘柄コードをカンマ区切りで指定。省略時はDBから取得。",
    )
    parser.add_argument("--source-table", default="stock_prices_daily")
    parser.add_argument("--target-table", default="stock_prices_daily_macd")
    parser.add_argument("--window-short", type=int, default=12)
    parser.add_argument("--window-long", type=int, default=26)
    parser.add_argument("--window-signal", type=int, default=9)
    return parser.parse_args()


def resolve_codes(conn: pymysql.Connection, codes_arg: str) -> List[str]:
    if codes_arg:
        return [code.strip() for code in codes_arg.split(",") if code.strip()]

    sql = "SELECT DISTINCT code FROM tse_listings ORDER BY code"
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return [row[0] for row in cursor.fetchall()]


def fetch_latest_macd_state(
    conn: pymysql.Connection,
    table: str,
    code: str,
    window_short: int,
    window_long: int,
    window_signal: int,
) -> Optional[Tuple]:
    sql = f"""
                SELECT trade_date, ema_short, ema_long, `signal`
        FROM `{table}`
        WHERE code = %s
          AND window_short = %s
          AND window_long = %s
          AND window_signal = %s
        ORDER BY trade_date DESC
        LIMIT 1
    """
    with conn.cursor() as cursor:
        cursor.execute(
            sql, (code, window_short, window_long, window_signal)
        )
        return cursor.fetchone()


def fetch_prices(
    conn: pymysql.Connection,
    table: str,
    code: str,
    start_date: Optional,
) -> List[Tuple]:
    if start_date is None:
        sql = f"""
            SELECT trade_date, `close`
            FROM `{table}`
            WHERE code = %s
            ORDER BY trade_date
        """
        params = (code,)
    else:
        sql = f"""
            SELECT trade_date, `close`
            FROM `{table}`
            WHERE code = %s
              AND trade_date > %s
            ORDER BY trade_date
        """
        params = (code, start_date)

    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        return list(cursor.fetchall())


def compute_macd(
    code: str,
    rows: Sequence[Tuple],
    window_short: int,
    window_long: int,
    window_signal: int,
    prev_ema_short: Optional[float],
    prev_ema_long: Optional[float],
    prev_signal: Optional[float],
) -> List[Tuple]:
    output: List[Tuple] = []
    if window_short <= 0 or window_long <= 0 or window_signal <= 0:
        return output

    alpha_short = 2.0 / (window_short + 1)
    alpha_long = 2.0 / (window_long + 1)
    alpha_signal = 2.0 / (window_signal + 1)

    ema_short = prev_ema_short
    ema_long = prev_ema_long
    signal = prev_signal

    for trade_date, close_v in rows:
        if close_v is None:
            continue

        close_f = float(close_v)

        if ema_short is None:
            ema_short = close_f
        else:
            ema_short = ((close_f - ema_short) * alpha_short) + ema_short

        if ema_long is None:
            ema_long = close_f
        else:
            ema_long = ((close_f - ema_long) * alpha_long) + ema_long

        macd = ema_short - ema_long
        if signal is None:
            signal = macd
        else:
            signal = ((macd - signal) * alpha_signal) + signal

        histogram = macd - signal

        output.append(
            (
                trade_date,
                code,
                window_short,
                window_long,
                window_signal,
                ema_short,
                ema_long,
                macd,
                signal,
                histogram,
            )
        )

    return output


def upsert_rows(
    conn: pymysql.Connection,
    table: str,
    rows: Iterable[Tuple],
) -> int:
    sql = f"""
    INSERT INTO `{table}`
    (`trade_date`, `code`, `window_short`, `window_long`, `window_signal`,
     `ema_short`, `ema_long`, `macd`, `signal`, `histogram`)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        `ema_short` = VALUES(`ema_short`),
        `ema_long` = VALUES(`ema_long`),
        `macd` = VALUES(`macd`),
        `signal` = VALUES(`signal`),
        `histogram` = VALUES(`histogram`)
    """
    with conn.cursor() as cursor:
        cursor.executemany(sql, list(rows))
        inserted = cursor.rowcount
    conn.commit()
    return inserted


def main() -> None:
    args = parse_args()
    logger = get_logger("calc_macd")

    if args.window_short >= args.window_long:
        raise ValueError("window-short は window-long より小さく指定してください。")

    conn = get_connection()
    try:
        codes = resolve_codes(conn, args.codes)
        total_codes = len(codes)
        total_rows = 0
        inserted_rows = 0

        for code in codes:
            latest_state = fetch_latest_macd_state(
                conn,
                args.target_table,
                code,
                args.window_short,
                args.window_long,
                args.window_signal,
            )

            if latest_state is None:
                latest_date = None
                prev_ema_short = None
                prev_ema_long = None
                prev_signal = None
            else:
                latest_date, prev_ema_short, prev_ema_long, prev_signal = latest_state

            price_rows = fetch_prices(
                conn,
                args.source_table,
                code,
                latest_date,
            )
            if not price_rows:
                logger.info("%s 追加計算対象なし", code)
                continue

            rows = compute_macd(
                code,
                price_rows,
                args.window_short,
                args.window_long,
                args.window_signal,
                float(prev_ema_short) if prev_ema_short is not None else None,
                float(prev_ema_long) if prev_ema_long is not None else None,
                float(prev_signal) if prev_signal is not None else None,
            )

            total_rows += len(rows)
            inserted = upsert_rows(conn, args.target_table, rows) if rows else 0
            inserted_rows += inserted
            logger.info("%s MACD計算: %5d件 (挿入: %5d)", code, len(rows), inserted)

        logger.info("対象銘柄数: %5d", total_codes)
        logger.info("計算レコード数: %5d", total_rows)
        logger.info("インサートレコード数: %5d", inserted_rows)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
