from django.core.cache import cache
from django.db import connection
from django.shortcuts import render


def _rankings_macd(request, direction):
    selected_market = (request.GET.get("market") or "").strip()
    is_top = direction == 'top'
    window_short = 12
    window_long = 26
    window_signal = 9

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
            SELECT MAX(trade_date)
            FROM stock_prices_daily_macd
            WHERE window_short = %s
              AND window_long = %s
              AND window_signal = %s
              AND macd IS NOT NULL
              AND `signal` IS NOT NULL
              AND histogram IS NOT NULL
            """,
            [window_short, window_long, window_signal],
        )
        latest_trade_date_row = cursor.fetchone()

    latest_trade_date = latest_trade_date_row[0] if latest_trade_date_row else None
    if latest_trade_date is None:
        return render(
            request,
            'rankings_macd.html',
            {
                'macd_top10': [],
                'macd_bottom10': [],
                'markets': markets,
                'selected_market': selected_market,
                'window_short': window_short,
                'window_long': window_long,
                'window_signal': window_signal,
                'trade_date': None,
                'ranking_direction_label': '上位' if is_top else '下位',
                'ranking_rows': [],
            },
        )

    cache_key = (
        f'rankings_macd:{direction}:{latest_trade_date}:ws:{window_short}:wl:{window_long}:wsg:{window_signal}:market:{selected_market or "all"}'
    )
    cached_context = cache.get(cache_key)
    if cached_context is not None:
        return render(request, 'rankings_macd.html', cached_context)

    with connection.cursor() as cursor:
        base_sql = """
            SELECT
                m.code,
                m.trade_date,
                m.macd,
                m.`signal`,
                m.histogram,
                l.name,
                l.market
            FROM stock_prices_daily_macd m
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
              AND m.window_short = %s
              AND m.window_long = %s
              AND m.window_signal = %s
              AND m.macd IS NOT NULL
              AND m.`signal` IS NOT NULL
              AND m.histogram IS NOT NULL
        """

        top_params = [latest_trade_date, window_short, window_long, window_signal]
        bottom_params = [latest_trade_date, window_short, window_long, window_signal]
        if selected_market:
            base_sql += " AND l.market = %s"
            top_params.append(selected_market)
            bottom_params.append(selected_market)

        top_sql = base_sql + " ORDER BY m.histogram DESC LIMIT 10"
        bottom_sql = base_sql + " ORDER BY m.histogram ASC LIMIT 10"

        ranking_sql = top_sql if is_top else bottom_sql
        ranking_params = top_params if is_top else bottom_params
        cursor.execute(ranking_sql, ranking_params)
        ranking_rows_raw = cursor.fetchall()

    ranking_rows = []
    for code, trade_date, macd, signal_v, histogram, name, market in ranking_rows_raw:
        ranking_rows.append(
            {
                'code': code,
                'name': name or '-',
                'market': market or '-',
                'trade_date': trade_date,
                'macd': float(macd),
                'signal': float(signal_v),
                'histogram': float(histogram),
                'score': float(histogram),
            }
        )

    context = {
        'ranking_rows': ranking_rows,
        'ranking_direction': direction,
        'ranking_direction_label': '上位' if is_top else '下位',
        'markets': markets,
        'selected_market': selected_market,
        'window_short': window_short,
        'window_long': window_long,
        'window_signal': window_signal,
        'trade_date': latest_trade_date,
    }
    cache.set(cache_key, context, 300)

    return render(request, 'rankings_macd.html', context)


def rankings_macd_top(request):
    return _rankings_macd(request, 'top')


def rankings_macd_bottom(request):
    return _rankings_macd(request, 'bottom')


def rankings_macd(request):
    return rankings_macd_top(request)
