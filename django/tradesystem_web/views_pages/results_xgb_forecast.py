from django.core.cache import cache
from django.core.paginator import Paginator
from django.db import connection
from django.shortcuts import render

from .common import shift_exchange_business_day, format_market_label


def results_xgb_forecast(request):
    selected_market = (request.GET.get("market") or "").strip()

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT market
            FROM tse_listings
            WHERE market IS NOT NULL AND market <> ''
            ORDER BY market
            """
        )
        markets = [row[0] for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT trade_date, model_version
            FROM stock_prices_daily_xgb_forecast
            ORDER BY trade_date DESC, updated_at DESC
            LIMIT 1
            """
        )
        latest_row = cursor.fetchone()

    latest_trade_date = latest_row[0] if latest_row else None
    latest_model_version = latest_row[1] if latest_row else None

    header_h1_date = (
        shift_exchange_business_day(latest_trade_date, 1)
        if latest_trade_date is not None
        else None
    )
    header_h2_date = (
        shift_exchange_business_day(latest_trade_date, 2)
        if latest_trade_date is not None
        else None
    )
    header_h3_date = (
        shift_exchange_business_day(latest_trade_date, 3)
        if latest_trade_date is not None
        else None
    )
    header_h4_date = (
        shift_exchange_business_day(latest_trade_date, 4)
        if latest_trade_date is not None
        else None
    )
    header_h5_date = (
        shift_exchange_business_day(latest_trade_date, 5)
        if latest_trade_date is not None
        else None
    )

    cache_key = (
        f'results_xgb_forecast:{latest_trade_date or "none"}:'
        f'{latest_model_version or "none"}:market:{selected_market or "all"}'
    )
    result_rows = cache.get(cache_key)

    if result_rows is None:
        with connection.cursor() as cursor:
            if latest_trade_date is not None:
                sql = """
                    SELECT
                        l.code,
                        l.name,
                        l.market,
                        %s AS model_version,
                        MAX(x.trained_end_date) AS trained_end_date,
                        MAX(CASE WHEN x.horizon = 1 THEN x.base_close END) AS base_close,
                        MAX(CASE WHEN x.horizon = 1 THEN x.predicted_close END) AS h1_close,
                        MAX(CASE WHEN x.horizon = 2 THEN x.predicted_close END) AS h2_close,
                        MAX(CASE WHEN x.horizon = 3 THEN x.predicted_close END) AS h3_close,
                        MAX(CASE WHEN x.horizon = 4 THEN x.predicted_close END) AS h4_close,
                        MAX(CASE WHEN x.horizon = 5 THEN x.predicted_close END) AS h5_close
                    FROM (
                        SELECT t.code, t.name, t.market
                        FROM tse_listings t
                        JOIN (
                            SELECT code, MAX(listing_date) AS latest_listing_date
                            FROM tse_listings
                            GROUP BY code
                        ) latest
                          ON latest.code = t.code
                         AND latest.latest_listing_date = t.listing_date
                    ) l
                    LEFT JOIN stock_prices_daily_xgb_forecast x
                      ON x.code = l.code
                     AND x.trade_date = %s
                     AND x.model_version = %s
                    WHERE 1 = 1
                """
                params = [
                    latest_model_version,
                    latest_trade_date,
                    latest_model_version,
                ]
                if selected_market:
                    sql += " AND l.market = %s"
                    params.append(selected_market)

                sql += """
                    GROUP BY l.code, l.name, l.market
                    ORDER BY l.code
                """
            else:
                sql = """
                    SELECT
                        l.code,
                        l.name,
                        l.market,
                        NULL AS model_version,
                        NULL AS trained_end_date,
                        NULL AS base_close,
                        NULL AS h1_close,
                        NULL AS h2_close,
                        NULL AS h3_close,
                        NULL AS h4_close,
                        NULL AS h5_close
                    FROM (
                        SELECT t.code, t.name, t.market
                        FROM tse_listings t
                        JOIN (
                            SELECT code, MAX(listing_date) AS latest_listing_date
                            FROM tse_listings
                            GROUP BY code
                        ) latest
                          ON latest.code = t.code
                         AND latest.latest_listing_date = t.listing_date
                    ) l
                    WHERE 1 = 1
                """
                params = []
                if selected_market:
                    sql += " AND l.market = %s"
                    params.append(selected_market)

                sql += " ORDER BY l.code"

            cursor.execute(sql, params)
            rows = cursor.fetchall()

        result_rows = []
        for (
            code,
            name,
            market,
            model_version,
            trained_end_date,
            base_close,
            h1_close,
            h2_close,
            h3_close,
            h4_close,
            h5_close,
        ) in rows:
                result_rows.append(
                {
                    "code": code,
                    "name": name or "-",
                    "market": format_market_label(market),
                    "model_version": model_version,
                    "trained_end_date": trained_end_date,
                    "base_close": float(base_close) if base_close is not None else None,
                    "h1_close": float(h1_close) if h1_close is not None else None,
                    "h2_close": float(h2_close) if h2_close is not None else None,
                    "h3_close": float(h3_close) if h3_close is not None else None,
                    "h4_close": float(h4_close) if h4_close is not None else None,
                    "h5_close": float(h5_close) if h5_close is not None else None,
                }
            )
        cache.set(cache_key, result_rows, 300)

    paginator = Paginator(result_rows, 15)
    page_number = request.GET.get("page")
    page_obj = paginator.get_page(page_number)

    context = {
        "result_rows": page_obj,
        "page_obj": page_obj,
        "markets": markets,
        "selected_market": selected_market,
        "trade_date": latest_trade_date,
        "model_version": latest_model_version,
        "header_h1_date": header_h1_date,
        "header_h2_date": header_h2_date,
        "header_h3_date": header_h3_date,
        "header_h4_date": header_h4_date,
        "header_h5_date": header_h5_date,
    }
    return render(request, "results_xgb_forecast.html", context)
