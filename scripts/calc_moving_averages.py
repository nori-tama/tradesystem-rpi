#!/usr/bin/env python3
"""Calculate moving averages from daily stock prices and store to MySQL."""

import argparse
from datetime import timedelta
from collections import deque
from typing import Iterable, List, Optional, Sequence, Tuple

import pymysql

from common.db import get_connection
from common.logger import get_logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calculate 5/25 day moving averages and store to MySQL."
    )
    parser.add_argument(
        "--codes",
        default="",
        help="銘柄コードをカンマ区切りで指定。省略時はDBから取得。",
    )
    parser.add_argument("--source-table", default="stock_prices_daily")
    parser.add_argument("--target-table", default="stock_prices_daily_ma")
    parser.add_argument("--window-short", type=int, default=5)
    parser.add_argument("--window-long", type=int, default=25)
    return parser.parse_args()


def resolve_codes(conn: pymysql.Connection, codes_arg: str) -> List[str]:
    if codes_arg:
        return [code.strip() for code in codes_arg.split(",") if code.strip()]

    sql = "SELECT DISTINCT code FROM tse_listings ORDER BY code"
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return [row[0] for row in cursor.fetchall()]


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
          AND trade_date >= %s
        ORDER BY trade_date
        """
        params = (code, start_date)

    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        return list(cursor.fetchall())


def fetch_latest_ma_date(
    conn: pymysql.Connection, table: str, code: str
) -> Optional:
    sql = f"""
    SELECT MAX(trade_date)
    FROM `{table}`
    WHERE code = %s
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (code,))
        row = cursor.fetchone()
        return row[0] if row and row[0] is not None else None


def compute_moving_averages(
    code: str,
    rows: Sequence[Tuple],
    window_short: int,
    window_long: int,
) -> List[Tuple]:
    short_q: deque = deque()
    long_q: deque = deque()
    short_sum = 0.0
    long_sum = 0.0
    output: List[Tuple] = []

    for trade_date, close_v in rows:
        if close_v is None:
            continue

        close_f = float(close_v)
        short_q.append(close_f)
        short_sum += close_f
        if len(short_q) > window_short:
            short_sum -= short_q.popleft()

        long_q.append(close_f)
        long_sum += close_f
        if len(long_q) > window_long:
            long_sum -= long_q.popleft()

        ma5: Optional[float] = None
        ma25: Optional[float] = None
        if len(short_q) == window_short:
            ma5 = short_sum / window_short
        if len(long_q) == window_long:
            ma25 = long_sum / window_long

        output.append((trade_date, code, ma5, ma25))

    return output


def upsert_rows(
    conn: pymysql.Connection, table: str, rows: Iterable[Tuple]
) -> int:
    sql = f"""
    INSERT INTO `{table}`
    (`trade_date`, `code`, `ma5`, `ma25`)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        `ma5` = VALUES(`ma5`),
        `ma25` = VALUES(`ma25`)
    """
    with conn.cursor() as cursor:
        cursor.executemany(sql, list(rows))
        inserted = cursor.rowcount
    conn.commit()
    return inserted


def main() -> None:
    args = parse_args()
    logger = get_logger("calc_moving_averages")

    conn = get_connection()
    try:
        codes = resolve_codes(conn, args.codes)
        total_codes = len(codes)
        total_rows = 0
        inserted_rows = 0

        for code in codes:
            latest_ma_date = fetch_latest_ma_date(
                conn, args.target_table, code
            )
            start_date = None
            if latest_ma_date is not None:
                days_back = max(args.window_long - 1, 0)
                start_date = latest_ma_date - timedelta(days=days_back)

            price_rows = fetch_prices(
                conn, args.source_table, code, start_date
            )
            if not price_rows:
                logger.info("%s 価格データなし", code)
                continue

            logger.info(
                "%s 取得レコード数: %s (開始日: %s)",
                code,
                len(price_rows),
                start_date if start_date is not None else "全期間",
            )

            rows = compute_moving_averages(
                code, price_rows, args.window_short, args.window_long
            )
            if latest_ma_date is not None:
                rows = [row for row in rows if row[0] > latest_ma_date]
            total_rows += len(rows)
            if rows:
                inserted = upsert_rows(conn, args.target_table, rows)
            else:
                inserted = 0
            inserted_rows += inserted
            logger.info("%s 計算レコード数: %s", code, len(rows))
            logger.info("%s インサートレコード数: %s", code, inserted)

        logger.info("対象銘柄数: %s", total_codes)
        logger.info("計算レコード数: %s", total_rows)
        logger.info("インサートレコード数: %s", inserted_rows)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
