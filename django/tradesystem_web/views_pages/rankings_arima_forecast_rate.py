from django.core.cache import cache
from django.db import connection
from django.shortcuts import render


def _rankings_arima_forecast_rate(request, direction):
    selected_market = (request.GET.get("market") or "").strip()
    is_top = direction == 'top'

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
    if latest_base_date is None:
        return render(
            request,
            'rankings_arima_forecast.html',
            {
                'rise_top10': [],
                'fall_top10': [],
                'markets': markets,
                'selected_market': selected_market,
                'forecast_base_date': None,
                'ranking_direction_label': '上位' if is_top else '下位',
                'ranking_rows': [],
            },
        )

    cache_key = (
        f'rankings_arima_forecast_rate:{direction}:{latest_base_date}:market:{selected_market or "all"}'
    )
    cached_context = cache.get(cache_key)
    if cached_context is not None:
        return render(request, 'rankings_arima_forecast.html', cached_context)

    with connection.cursor() as cursor:
        base_sql = """
            SELECT
                f.code,
                l.name,
                l.market,
                d.`close` AS base_close,
                f.predicted_close,
                ((f.predicted_close - d.`close`) / d.`close`) * 100 AS forecast_rate
            FROM stock_prices_daily_arima_forecast f
            JOIN (
                SELECT t.code, t.name, t.market
                FROM tse_listings t
                JOIN (
                    SELECT code, MAX(listing_date) AS latest_listing_date
                    FROM tse_listings
                    GROUP BY code
                ) latest
                  ON latest.code = t.code
                 AND latest.latest_listing_date = t.listing_date
            ) l ON l.code = f.code
            LEFT JOIN stock_prices_daily d
              ON d.code = f.code
             AND d.trade_date = f.forecast_base_date
            WHERE f.forecast_base_date = %s
              AND f.horizon = 5
              AND f.predicted_close IS NOT NULL
              AND d.`close` IS NOT NULL
              AND d.`close` <> 0
        """

        rise_params = [latest_base_date]
        fall_params = [latest_base_date]
        if selected_market:
            base_sql += " AND l.market = %s"
            rise_params.append(selected_market)
            fall_params.append(selected_market)

        rise_sql = base_sql + " ORDER BY forecast_rate DESC LIMIT 10"
        fall_sql = base_sql + " ORDER BY forecast_rate ASC LIMIT 10"

        ranking_sql = rise_sql if is_top else fall_sql
        ranking_params = rise_params if is_top else fall_params
        cursor.execute(ranking_sql, ranking_params)
        ranking_rows_raw = cursor.fetchall()

    ranking_rows = []
    for code, name, market, base_close, predicted_close, forecast_rate in ranking_rows_raw:
        ranking_rows.append(
            {
                'code': code,
                'name': name or '-',
                'market': market or '-',
                'base_close': float(base_close),
                'predicted_close': float(predicted_close),
                'forecast_rate': float(forecast_rate),
                'score': float(forecast_rate),
            }
        )

    context = {
        'ranking_rows': ranking_rows,
        'ranking_direction': direction,
        'ranking_direction_label': '上位' if is_top else '下位',
        'markets': markets,
        'selected_market': selected_market,
        'forecast_base_date': latest_base_date,
    }
    cache.set(cache_key, context, 300)

    return render(request, 'rankings_arima_forecast.html', context)


def rankings_arima_forecast_rate_top(request):
    return _rankings_arima_forecast_rate(request, 'top')


def rankings_arima_forecast_rate_bottom(request):
    return _rankings_arima_forecast_rate(request, 'bottom')


def rankings_arima_forecast_rate(request):
    return rankings_arima_forecast_rate_top(request)
