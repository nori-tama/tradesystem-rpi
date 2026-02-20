from django.core.cache import cache
from django.db import connection
from django.shortcuts import render


def _rankings_xgb_forecast_rate(request, direction):
    selected_market = (request.GET.get("market") or "").strip()
    is_top = direction == 'top'
    ranking_horizon = 5

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
            WHERE horizon = %s
            ORDER BY trade_date DESC, updated_at DESC
            LIMIT 1
            """,
            [ranking_horizon],
        )
        latest_row = cursor.fetchone()

    latest_trade_date = latest_row[0] if latest_row else None
    latest_horizon = ranking_horizon if latest_row else None
    latest_model_version = latest_row[1] if latest_row else None

    if latest_trade_date is None:
        return render(
            request,
            "rankings_xgb_forecast.html",
            {
                "rise_top10": [],
                "fall_top10": [],
                "markets": markets,
                "selected_market": selected_market,
                "trade_date": None,
                "horizon": None,
                "model_version": None,
                "ranking_direction_label": '上位' if is_top else '下位',
                "ranking_rows": [],
            },
        )

    cache_key = (
        f'rankings_xgb_forecast_rate:{direction}:{latest_trade_date}:{latest_horizon}:{latest_model_version}:'
        f'market:{selected_market or "all"}'
    )
    cached_context = cache.get(cache_key)
    if cached_context is not None:
        return render(request, "rankings_xgb_forecast.html", cached_context)

    with connection.cursor() as cursor:
        base_sql = """
            SELECT
                x.code,
                l.name,
                l.market,
                x.predicted_close,
                x.actual_close,
                x.predicted_return,
                x.actual_return,
                (x.predicted_return * 100) AS forecast_rate
            FROM stock_prices_daily_xgb_forecast x
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
            ) l ON l.code = x.code
            WHERE x.trade_date = %s
              AND x.horizon = %s
              AND x.model_version = %s
              AND x.predicted_return IS NOT NULL
        """

        rise_params = [latest_trade_date, latest_horizon, latest_model_version]
        fall_params = [latest_trade_date, latest_horizon, latest_model_version]
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
    for code, name, market, predicted_close, actual_close, predicted_return, actual_return, forecast_rate in ranking_rows_raw:
        ranking_rows.append(
            {
                "code": code,
                "name": name or "-",
                "market": market or "-",
                "predicted_close": float(predicted_close) if predicted_close is not None else None,
                "actual_close": float(actual_close) if actual_close is not None else None,
                "predicted_return": float(predicted_return) if predicted_return is not None else None,
                "actual_return": float(actual_return) if actual_return is not None else None,
                "forecast_rate": float(forecast_rate),
                "score": float(forecast_rate),
            }
        )

    context = {
        "ranking_rows": ranking_rows,
        "ranking_direction": direction,
        "ranking_direction_label": '上位' if is_top else '下位',
        "markets": markets,
        "selected_market": selected_market,
        "trade_date": latest_trade_date,
        "horizon": latest_horizon,
        "model_version": latest_model_version,
    }
    cache.set(cache_key, context, 300)

    return render(request, "rankings_xgb_forecast.html", context)


def rankings_xgb_forecast_rate_top(request):
    return _rankings_xgb_forecast_rate(request, 'top')


def rankings_xgb_forecast_rate_bottom(request):
    return _rankings_xgb_forecast_rate(request, 'bottom')


def rankings_xgb_forecast_rate(request):
    return rankings_xgb_forecast_rate_top(request)
