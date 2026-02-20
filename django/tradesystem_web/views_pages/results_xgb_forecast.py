from django.core.cache import cache
from django.core.paginator import Paginator
from django.db import connection
from django.shortcuts import render


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
            SELECT trade_date, horizon, model_version
            FROM stock_prices_daily_xgb_forecast
            ORDER BY trade_date DESC, updated_at DESC
            LIMIT 1
            """
        )
        latest_row = cursor.fetchone()

    latest_trade_date = latest_row[0] if latest_row else None
    latest_horizon = latest_row[1] if latest_row else None
    latest_model_version = latest_row[2] if latest_row else None

    cache_key = (
        f'results_xgb_forecast:{latest_trade_date or "none"}:{latest_horizon or "none"}:'
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
                        %s AS trade_date,
                        %s AS horizon,
                        %s AS model_version,
                        MAX(x.trained_end_date) AS trained_end_date,
                        MAX(x.base_close) AS base_close,
                        MAX(x.predicted_close) AS predicted_close,
                        MAX(x.actual_close) AS actual_close,
                        MAX(x.predicted_return) AS predicted_return,
                        MAX(x.actual_return) AS actual_return
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
                     AND x.horizon = %s
                     AND x.model_version = %s
                    WHERE 1 = 1
                """
                params = [
                    latest_trade_date,
                    latest_horizon,
                    latest_model_version,
                    latest_trade_date,
                    latest_horizon,
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
                        NULL AS trade_date,
                        NULL AS horizon,
                        NULL AS model_version,
                        NULL AS trained_end_date,
                        NULL AS base_close,
                        NULL AS predicted_close,
                        NULL AS actual_close,
                        NULL AS predicted_return,
                        NULL AS actual_return
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
            trade_date,
            horizon,
            model_version,
            trained_end_date,
            base_close,
            predicted_close,
            actual_close,
            predicted_return,
            actual_return,
        ) in rows:
            result_rows.append(
                {
                    "code": code,
                    "name": name or "-",
                    "market": market or "-",
                    "trade_date": trade_date,
                    "horizon": horizon,
                    "model_version": model_version,
                    "trained_end_date": trained_end_date,
                    "base_close": float(base_close) if base_close is not None else None,
                    "predicted_close": float(predicted_close) if predicted_close is not None else None,
                    "actual_close": float(actual_close) if actual_close is not None else None,
                    "predicted_return": float(predicted_return) if predicted_return is not None else None,
                    "actual_return": float(actual_return) if actual_return is not None else None,
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
        "horizon": latest_horizon,
        "model_version": latest_model_version,
    }
    return render(request, "results_xgb_forecast.html", context)
