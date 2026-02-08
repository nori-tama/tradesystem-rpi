#!/usr/bin/env python3
"""Fetch JPX listed stocks and store into MySQL."""

import argparse
import io
from typing import List

import pandas as pd
import pymysql
import requests

from db_common import get_connection

JPX_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"

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
    return df


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Keep only known columns for a stable schema.
    df = df.rename(columns=COLUMN_MAP)
    keep_cols = [name for name in COLUMN_MAP.values() if name in df.columns]
    df = df[keep_cols]

    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(4)

    if "listing_date" in df.columns:
        df["listing_date"] = pd.to_datetime(df["listing_date"], errors="coerce").dt.date

    # Replace NaN with safe values for inserts.
    for col in df.columns:
        if col == "listing_date":
            df[col] = df[col].where(pd.notna(df[col]), None)
        else:
            df[col] = df[col].fillna("")
    return df



def upsert_rows(conn: pymysql.Connection, table: str, df: pd.DataFrame) -> int:
    columns = list(df.columns)
    placeholders = ",".join(["%s"] * len(columns))
    cols_sql = ",".join([f"`{c}`" for c in columns])
    update_sql = ",".join([f"`{c}`=VALUES(`{c}`)" for c in columns if c != "code"])

    sql = f"""
    INSERT INTO `{table}` ({cols_sql})
    VALUES ({placeholders})
    ON DUPLICATE KEY UPDATE {update_sql};
    """

    values: List[tuple] = [tuple(row[c] for c in columns) for _, row in df.iterrows()]
    with conn.cursor() as cursor:
        cursor.executemany(sql, values)
    conn.commit()
    return len(values)


def main() -> None:
    args = parse_args()
    df = fetch_dataframe(args.timeout)
    df = normalize_dataframe(df)

    conn = get_connection()

    try:
        inserted = upsert_rows(conn, args.table, df)
        print(f"Upserted {inserted} rows into {args.table}.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
