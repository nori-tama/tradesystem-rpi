#!/usr/bin/env python3
"""Calculate RSI from daily stock prices and store to MySQL."""

import argparse
from typing import Iterable, List, Optional, Sequence, Tuple

import pymysql

from common.db import get_connection
from common.logger import get_logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calculate RSI and store to MySQL."
    )
    parser.add_argument(
        "--codes",
        default="",
        help="銘柄コードをカンマ区切りで指定。省略時はDBから取得。",
    )
    parser.add_argument("--source-table", default="stock_prices_daily")
    parser.add_argument("--target-table", default="stock_prices_daily_rsi")
    parser.add_argument("--window", type=int, default=14)
    return parser.parse_args()


def resolve_codes(conn: pymysql.Connection, codes_arg: str) -> List[str]:
    if codes_arg:
        return [code.strip() for code in codes_arg.split(",") if code.strip()]

    sql = "SELECT DISTINCT code FROM tse_listings ORDER BY code"
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return [row[0] for row in cursor.fetchall()]


def fetch_prices(
    conn: pymysql.Connection, table: str, code: str
) -> List[Tuple]:
    sql = f"""
        SELECT trade_date, `close`
        FROM `{table}`
        WHERE code = %s
        ORDER BY trade_date
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (code,))
        return list(cursor.fetchall())


def fetch_latest_rsi_date(
    conn: pymysql.Connection, table: str, code: str, window: int
) -> Optional:
    sql = f"""
        SELECT MAX(trade_date)
        FROM `{table}`
        WHERE code = %s
          AND window = %s
        """
    with conn.cursor() as cursor:
        cursor.execute(sql, (code, window))
        row = cursor.fetchone()
        return row[0] if row and row[0] is not None else None


def compute_rsi(
    code: str,
    rows: Sequence[Tuple],
    window: int,
) -> List[Tuple]:
    output: List[Tuple] = []
    if window <= 0:
        return output

    prev_close: Optional[float] = None
    gains: List[float] = []
    losses: List[float] = []
    avg_gain: Optional[float] = None
    avg_loss: Optional[float] = None

    for trade_date, close_v in rows:
        if close_v is None:
            continue

        close_f = float(close_v)
        if prev_close is None:
            prev_close = close_f
            continue

        change = close_f - prev_close
        gain = change if change > 0 else 0.0
        loss = -change if change < 0 else 0.0

        if avg_gain is None:
            gains.append(gain)
            losses.append(loss)
            if len(gains) < window:
                prev_close = close_f
                continue
            avg_gain = sum(gains) / window
            avg_loss = sum(losses) / window
        else:
            avg_gain = ((avg_gain * (window - 1)) + gain) / window
            avg_loss = ((avg_loss * (window - 1)) + loss) / window

        if avg_loss == 0 and avg_gain == 0:
            rsi = 50.0
        elif avg_loss == 0:
            rsi = 100.0
        elif avg_gain == 0:
            rsi = 0.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

        output.append((trade_date, code, window, rsi))
        prev_close = close_f

    return output


def upsert_rows(
    conn: pymysql.Connection, table: str, rows: Iterable[Tuple]
) -> int:
    sql = f"""
    INSERT INTO `{table}`
    (`trade_date`, `code`, `window`, `rsi`)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        `rsi` = VALUES(`rsi`)
    """
    with conn.cursor() as cursor:
        cursor.executemany(sql, list(rows))
        inserted = cursor.rowcount
    conn.commit()
    return inserted


def main() -> None:
    args = parse_args()
    logger = get_logger("calc_rsi")

    conn = get_connection()
    try:
        codes = resolve_codes(conn, args.codes)
        total_codes = len(codes)
        total_rows = 0
        inserted_rows = 0

        for code in codes:
            latest_rsi_date = fetch_latest_rsi_date(
                conn, args.target_table, code, args.window
            )

            price_rows = fetch_prices(conn, args.source_table, code)
            if not price_rows:
                logger.info("%s 価格データなし", code)
                continue

            rows = compute_rsi(code, price_rows, args.window)
            if latest_rsi_date is not None:
                rows = [row for row in rows if row[0] > latest_rsi_date]

            total_rows += len(rows)
            if rows:
                inserted = upsert_rows(conn, args.target_table, rows)
            else:
                inserted = 0
            inserted_rows += inserted
            logger.info("%s RSI計算: %5d件 (挿入: %5d)", code, len(rows), inserted)

        logger.info("対象銘柄数: %5d", total_codes)
        logger.info("計算レコード数: %5d", total_rows)
        logger.info("インサートレコード数: %5d", inserted_rows)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
