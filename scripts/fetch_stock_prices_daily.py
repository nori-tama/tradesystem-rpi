#!/usr/bin/env python3
"""日足株価を取得してMySQLに格納する。"""

import argparse
import time
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Tuple

import pymysql
import requests

from common.db import get_connection
from common.logger import get_logger

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; tradesystem-rpi/1.0)"
}


def parse_args() -> argparse.Namespace:
    # CLI引数を解析
    parser = argparse.ArgumentParser(
        description="日足株価を取得してMySQLに格納する。"
    )
    parser.add_argument(
        "--codes",
        default="",
        help="銘柄コードをカンマ区切りで指定。省略時はDBから取得。")
    parser.add_argument("--table", default="stock_prices_daily")
    parser.add_argument("--timeout", type=int, default=30)
    return parser.parse_args()


def resolve_codes(conn: pymysql.Connection, codes_arg: str) -> List[str]:
    # 対象銘柄コードを決定（指定がなければDBから取得）
    if codes_arg:
        return [code.strip() for code in codes_arg.split(",") if code.strip()]

    sql = "SELECT DISTINCT code FROM tse_listings ORDER BY code"
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return [row[0] for row in cursor.fetchall()]


def resolve_start_timestamp(conn: pymysql.Connection, table: str, code: str) -> int:
    # 既存データの最終日を起点に取得開始日時を求める
    sql = f"SELECT MAX(trade_date) FROM `{table}` WHERE code = %s"
    with conn.cursor() as cursor:
        cursor.execute(sql, (code,))
        result = cursor.fetchone()
        max_date = result[0] if result else None

    if max_date:
        # 既存データの翌日から取得して重複を避ける
        next_day = max_date + timedelta(days=1)
        return int(
            datetime.combine(next_day, datetime.min.time(), tzinfo=timezone.utc).timestamp()
        )

    return 0


def to_yahoo_symbol(code: str) -> str:
    # Yahoo Finance用のティッカーに変換
    return f"{code}.T"


def fetch_prices(
    code: str,
    start_ts: int,
    end_ts: int,
    timeout: int,
    logger,
) -> List[Tuple]:
    # Yahoo Financeから日足データを取得して整形
    params = {
        "period1": start_ts,
        "period2": end_ts,
        "interval": "1d",
        "events": "history",
    }
    url = YAHOO_CHART_URL.format(symbol=to_yahoo_symbol(code))
    # 429/通信エラー時は指数バックオフで再試行し、失敗時は空配列を返す
    retries = 3
    backoff_sec = 2
    payload = None
    for attempt in range(1, retries + 1):
        try:
            logger.debug("%s データ取得開始", code)
            resp = requests.get(url, params=params, timeout=timeout, headers=DEFAULT_HEADERS)
            if resp.status_code == 429:
                logger.warning("%s HTTP 429 取得制限のため再試行 (%s/%s)", code, attempt, retries)
                if attempt == retries:
                    resp.raise_for_status()
                time.sleep(backoff_sec * attempt)
                continue

            logger.debug("%s データ取得完了（ステータスコード: %s）", code, resp.status_code)
            resp.raise_for_status()
            payload = resp.json()
            break
        except requests.RequestException:
            logger.warning("%s 通信エラーのため再試行 (%s/%s)", code, attempt, retries)
            if attempt == retries:
                return []
            time.sleep(backoff_sec * attempt)

    if payload is None:
        return []

    result = payload.get("chart", {}).get("result")
    if not result:
        return []

    data = result[0]
    timestamps = data.get("timestamp") or []
    quote_list = data.get("indicators", {}).get("quote") or []
    if not quote_list:
        return []

    quote = quote_list[0]
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []

    rows: List[Tuple] = []
    for idx, ts in enumerate(timestamps):
        trade_date = datetime.fromtimestamp(ts, tz=timezone.utc).date()
        open_v = opens[idx] if idx < len(opens) else None
        high_v = highs[idx] if idx < len(highs) else None
        low_v = lows[idx] if idx < len(lows) else None
        close_v = closes[idx] if idx < len(closes) else None
        volume_v = volumes[idx] if idx < len(volumes) else None

        if None in (open_v, high_v, low_v, close_v, volume_v):
            continue

        rows.append((trade_date, code, open_v, high_v, low_v, close_v, volume_v))

    return rows


def insert_rows(
    conn: pymysql.Connection, table: str, rows: Iterable[Tuple]
) -> int:
    # 取得した行をDBへ一括登録（重複は無視）
    sql = f"""
    INSERT IGNORE INTO `{table}`
    (`trade_date`, `code`, `open`, `high`, `low`, `close`, `volume`)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    with conn.cursor() as cursor:
        cursor.executemany(sql, list(rows))
        inserted = cursor.rowcount
    conn.commit()
    return inserted


def main() -> None:
    # 全銘柄の取得・保存を実行
    args = parse_args()
    logger = get_logger("fetch_stock_prices_daily")
    jst = timezone(timedelta(hours=9))
    today_jst = datetime.now(tz=jst).date()
    end_ts = int(
        datetime.combine(today_jst, datetime.min.time(), tzinfo=jst).timestamp()
    )

    conn = get_connection()
    try:
        codes = resolve_codes(conn, args.codes)
        total_codes = len(codes)
        total_rows = 0
        inserted_rows = 0

        for code in codes:
            start_ts = resolve_start_timestamp(conn, args.table, code)
            if start_ts >= end_ts:
                logger.info("%s データ取得済みのためスキップ", code)
                continue
            rows = fetch_prices(code, start_ts, end_ts, args.timeout, logger)
            fetched_count = len(rows)
            total_rows += fetched_count
            if rows:
                inserted = insert_rows(conn, args.table, rows)
            else:
                inserted = 0
            inserted_rows += inserted
            logger.info("%s 取得レコード数: %5d", code, fetched_count)
            logger.info("%s インサートレコード数: %5d", code, inserted)

        logger.info("対象銘柄数: %s", total_codes)
        logger.info("取得レコード数: %s", total_rows)
        logger.info("インサートレコード数: %s", inserted_rows)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
