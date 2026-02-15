from django.core.cache import cache
from django.db import connection
from django.shortcuts import render

from .common import shift_exchange_business_day


def rankings_arima_forecast(request):
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
            SELECT MAX(forecast_base_date)
            FROM stock_prices_daily_arima_forecast
            """
        )
        latest_base_date_row = cursor.fetchone()

    latest_base_date = latest_base_date_row[0] if latest_base_date_row else None

    header_h1_date = (
        shift_exchange_business_day(latest_base_date, 1)
        if latest_base_date is not None
        else None
    )
    header_h2_date = (
        shift_exchange_business_day(latest_base_date, 2)
        if latest_base_date is not None
        else None
    )
    header_h3_date = (
        shift_exchange_business_day(latest_base_date, 3)
        if latest_base_date is not None
        else None
    )
    header_h4_date = (
        shift_exchange_business_day(latest_base_date, 4)
        if latest_base_date is not None
        else None
    )
    header_h5_date = (
        shift_exchange_business_day(latest_base_date, 5)
        if latest_base_date is not None
        else None
    )

    cache_key = (
        f'rankings_arima_forecast:{latest_base_date or "none"}:market:{selected_market or "all"}'
    )
    cached_context = cache.get(cache_key)
    if cached_context is not None:
        return render(request, 'rankings_arima_forecast.html', cached_context)

    with connection.cursor() as cursor:
        if latest_base_date is not None:
            sql = """
                SELECT
                    l.code,
                    l.name,
                    l.market,
                    %s AS forecast_base_date,
                    MAX(d.`close`) AS base_close,
                    MAX(CASE WHEN f.horizon = 1 THEN f.target_trade_date END) AS h1_trade_date,
                    MAX(CASE WHEN f.horizon = 1 THEN f.predicted_close END) AS h1_close,
                    MAX(CASE WHEN f.horizon = 2 THEN f.target_trade_date END) AS h2_trade_date,
                    MAX(CASE WHEN f.horizon = 2 THEN f.predicted_close END) AS h2_close,
                    MAX(CASE WHEN f.horizon = 3 THEN f.target_trade_date END) AS h3_trade_date,
                    MAX(CASE WHEN f.horizon = 3 THEN f.predicted_close END) AS h3_close,
                    MAX(CASE WHEN f.horizon = 4 THEN f.target_trade_date END) AS h4_trade_date,
                    MAX(CASE WHEN f.horizon = 4 THEN f.predicted_close END) AS h4_close,
                    MAX(CASE WHEN f.horizon = 5 THEN f.target_trade_date END) AS h5_trade_date,
                    MAX(CASE WHEN f.horizon = 5 THEN f.predicted_close END) AS h5_close
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
                LEFT JOIN stock_prices_daily_arima_forecast f
                  ON f.code = l.code
                 AND f.forecast_base_date = %s
                LEFT JOIN stock_prices_daily d
                  ON d.code = l.code
                 AND d.trade_date = %s
                WHERE 1 = 1
            """
            params = [latest_base_date, latest_base_date, latest_base_date]
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
                    NULL AS forecast_base_date,
                    NULL AS base_close,
                    NULL AS h1_trade_date,
                    NULL AS h1_close,
                    NULL AS h2_trade_date,
                    NULL AS h2_close,
                    NULL AS h3_trade_date,
                    NULL AS h3_close,
                    NULL AS h4_trade_date,
                    NULL AS h4_close,
                    NULL AS h5_trade_date,
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

    forecast_rows = []
    for (
        code,
        name,
        market,
        _forecast_base_date,
        base_close,
        h1_trade_date,
        h1_close,
        h2_trade_date,
        h2_close,
        h3_trade_date,
        h3_close,
        h4_trade_date,
        h4_close,
        h5_trade_date,
        h5_close,
    ) in rows:
        forecast_rows.append(
            {
                'code': code,
                'name': name or '-',
                'market': market or '-',
                'forecast_base_date': latest_base_date,
                'base_close': float(base_close) if base_close is not None else None,
                'h1_trade_date': h1_trade_date,
                'h1_close': float(h1_close) if h1_close is not None else None,
                'h2_trade_date': h2_trade_date,
                'h2_close': float(h2_close) if h2_close is not None else None,
                'h3_trade_date': h3_trade_date,
                'h3_close': float(h3_close) if h3_close is not None else None,
                'h4_trade_date': h4_trade_date,
                'h4_close': float(h4_close) if h4_close is not None else None,
                'h5_trade_date': h5_trade_date,
                'h5_close': float(h5_close) if h5_close is not None else None,
            }
        )

    context = {
        'forecast_rows': forecast_rows,
        'markets': markets,
        'selected_market': selected_market,
        'forecast_base_date': latest_base_date,
        'header_h1_date': header_h1_date,
        'header_h2_date': header_h2_date,
        'header_h3_date': header_h3_date,
        'header_h4_date': header_h4_date,
        'header_h5_date': header_h5_date,
    }
    cache.set(cache_key, context, 300)

    return render(request, 'rankings_arima_forecast.html', context)
