from django.core.cache import cache
from django.db import connection
from django.shortcuts import render


def _rankings_ma_estimate(request, direction):
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

        cursor.execute("SELECT MAX(trade_date) FROM stock_prices_daily_ma")
        latest_trade_date_row = cursor.fetchone()

    latest_trade_date = latest_trade_date_row[0] if latest_trade_date_row else None
    if latest_trade_date is None:
        return render(
            request,
            'rankings_ma_estimate.html',
            {
                'rise_top10': [],
                'fall_top10': [],
                'markets': markets,
                'selected_market': selected_market,
                'ranking_direction_label': '上位' if is_top else '下位',
                'ranking_rows': [],
            },
        )

    cache_key = (
        f'rankings_ma_estimate:{direction}:{latest_trade_date}:market:{selected_market or "all"}'
    )
    cached_context = cache.get(cache_key)
    if cached_context is not None:
        return render(request, 'rankings_ma_estimate.html', cached_context)

    with connection.cursor() as cursor:
        base_sql = """
            SELECT
                m.code,
                m.trade_date,
                m.ma5,
                m.ma25,
                ((m.ma5 - m.ma25) / m.ma25) * 100 AS estimate_rate,
                l.name,
                l.market
            FROM stock_prices_daily_ma m
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
            ) l ON l.code = m.code
            WHERE m.trade_date = %s
              AND m.ma5 IS NOT NULL
              AND m.ma25 IS NOT NULL
              AND m.ma25 <> 0
        """

        rise_params = [latest_trade_date]
        fall_params = [latest_trade_date]
        if selected_market:
            base_sql += " AND l.market = %s"
            rise_params.append(selected_market)
            fall_params.append(selected_market)

        rise_sql = base_sql + " ORDER BY estimate_rate DESC LIMIT 10"
        fall_sql = base_sql + " ORDER BY estimate_rate ASC LIMIT 10"

        ranking_sql = rise_sql if is_top else fall_sql
        ranking_params = rise_params if is_top else fall_params
        cursor.execute(ranking_sql, ranking_params)
        ranking_rows_raw = cursor.fetchall()

    ranking_rows = []
    for code, trade_date, ma5, ma25, estimate_rate, name, market in ranking_rows_raw:
        ranking_rows.append(
            {
                'code': code,
                'name': name or '-',
                'market': market or '-',
                'trade_date': trade_date,
                'ma5': float(ma5),
                'ma25': float(ma25),
                'estimate_rate': float(estimate_rate),
                'score': float(estimate_rate),
            }
        )

    context = {
        'ranking_rows': ranking_rows,
        'ranking_direction': direction,
        'ranking_direction_label': '上位' if is_top else '下位',
        'markets': markets,
        'selected_market': selected_market,
    }
    cache.set(cache_key, context, 300)

    return render(request, 'rankings_ma_estimate.html', context)


def rankings_ma_estimate_top(request):
    return _rankings_ma_estimate(request, 'top')


def rankings_ma_estimate_bottom(request):
    return _rankings_ma_estimate(request, 'bottom')


def rankings_ma_estimate(request):
    return rankings_ma_estimate_top(request)
