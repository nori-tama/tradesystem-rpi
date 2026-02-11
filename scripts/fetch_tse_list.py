#!/usr/bin/env python3
"""JPXの上場銘柄一覧を取得してMySQLに格納する。"""

import argparse
import io
from typing import List

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype
import pymysql
import requests

from common.db import get_connection
from common.logger import get_logger

JPX_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"

ALLOWED_MARKETS = {
    "プライム（内国株式）",
    "スタンダード（内国株式）",
    "グロース（内国株式）",
    "プライム（外国株式）",
    "スタンダード（外国株式）",
    "グロース（外国株式）",
}

COLUMN_MAP = {
    "日付": "listing_date",
    "コード": "code",
    "銘柄名": "name",
    "市場・商品区分": "market",
    "33業種コード": "sector33_code",
    "33業種区分": "sector33_name",
    "17業種コード": "sector17_code",
    "17業種区分": "sector17_name",
    "規模コード": "scale_code",
    "規模区分": "scale_name",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch JPX listed stocks and store into MySQL."
    )
    parser.add_argument("--table", default="tse_listings")
    parser.add_argument("--timeout", type=int, default=30)
    return parser.parse_args()


def fetch_dataframe(timeout: int) -> pd.DataFrame:
    resp = requests.get(JPX_URL, timeout=timeout)
    resp.raise_for_status()
    data = io.BytesIO(resp.content)
    df = pd.read_excel(data)
    # ヘッダの前後空白や全角スペースを除去して正規化する。
    df.columns = (
        df.columns.astype(str).str.replace("\u3000", " ").str.strip()
    )
    return df


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # 既知の列だけに絞り、スキーマを固定する。
    if "日付" not in df.columns:
        date_cols = [col for col in df.columns if "日付" in col]
        if len(date_cols) == 1:
            df = df.rename(columns={date_cols[0]: "日付"})

    df = df.rename(columns=COLUMN_MAP)
    keep_cols = [name for name in COLUMN_MAP.values() if name in df.columns]
    df = df[keep_cols]

    # 指定された市場・商品区分のみに絞り込む。
    if "market" in df.columns:
        df = df[df["market"].isin(ALLOWED_MARKETS)]

    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(4)

    if "listing_date" in df.columns:
        series = df["listing_date"]
        if is_datetime64_any_dtype(series):
            parsed = series
        elif is_numeric_dtype(series):
            numeric = pd.to_numeric(series, errors="coerce")
            numeric_int = numeric.dropna().astype("Int64")
            # 8桁のYYYYMMDD形式ならそれを優先して変換する。
            if numeric_int.between(19000101, 21001231).any():
                parsed = pd.to_datetime(
                    numeric_int.astype(str), format="%Y%m%d", errors="coerce"
                )
            # Excelシリアル日付は概ね20000以上になるため、それを目安に変換する。
            elif numeric.dropna().ge(20000).any():
                parsed = pd.to_datetime(
                    numeric, unit="D", origin="1899-12-30", errors="coerce"
                )
            else:
                parsed = pd.to_datetime(
                    numeric.astype("Int64").astype(str), errors="coerce"
                )
        else:
            parsed = pd.to_datetime(series.astype(str), errors="coerce")

        parsed = parsed.dt.date
        df["listing_date"] = parsed.astype(object).where(pd.notna(parsed), None)

    # INSERTに安全な値へ置換する。
    for col in df.columns:
        if col != "listing_date":
            df[col] = df[col].astype("string").fillna("")
    return df



def upsert_rows(conn: pymysql.Connection, table: str, df: pd.DataFrame) -> int:
    columns = list(df.columns)
    placeholders = ",".join(["%s"] * len(columns))
    cols_sql = ",".join([f"`{c}`" for c in columns])

    sql = f"""
    INSERT IGNORE INTO `{table}` ({cols_sql})
    VALUES ({placeholders})
    """

    values: List[tuple] = [tuple(row[c] for c in columns) for _, row in df.iterrows()]
    with conn.cursor() as cursor:
        cursor.executemany(sql, values)
        inserted = cursor.rowcount
    conn.commit()
    return inserted


def main() -> None:
    args = parse_args()
    logger = get_logger("fetch_tse_list")
    raw_df = fetch_dataframe(args.timeout)
    total_count = len(raw_df)
    df = normalize_dataframe(raw_df)
    matched_count = len(df)

    conn = get_connection()

    try:
        inserted = upsert_rows(conn, args.table, df)
        logger.info("総レコード数: %s", total_count)
        logger.info("該当レコード数: %s", matched_count)
        logger.info("インサートレコード数: %s", inserted)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
