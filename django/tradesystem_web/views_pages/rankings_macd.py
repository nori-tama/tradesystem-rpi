from django.core.cache import cache
from django.db import connection
from django.shortcuts import render


def rankings_macd(request):
    selected_market = (request.GET.get("market") or "").strip()
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
            },
        )

    cache_key = (
        f'rankings_macd:{latest_trade_date}:ws:{window_short}:wl:{window_long}:wsg:{window_signal}:market:{selected_market or "all"}'
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

        cursor.execute(top_sql, top_params)
        top_rows = cursor.fetchall()

        cursor.execute(bottom_sql, bottom_params)
        bottom_rows = cursor.fetchall()

    macd_top10 = []
    for code, trade_date, macd, signal_v, histogram, name, market in top_rows:
        macd_top10.append(
            {
                'code': code,
                'name': name or '-',
                'market': market or '-',
                'trade_date': trade_date,
                'macd': float(macd),
                'signal': float(signal_v),
                'histogram': float(histogram),
            }
        )

    macd_bottom10 = []
    for code, trade_date, macd, signal_v, histogram, name, market in bottom_rows:
        macd_bottom10.append(
            {
                'code': code,
                'name': name or '-',
                'market': market or '-',
                'trade_date': trade_date,
                'macd': float(macd),
                'signal': float(signal_v),
                'histogram': float(histogram),
            }
        )

    context = {
        'macd_top10': macd_top10,
        'macd_bottom10': macd_bottom10,
        'markets': markets,
        'selected_market': selected_market,
        'window_short': window_short,
        'window_long': window_long,
        'window_signal': window_signal,
        'trade_date': latest_trade_date,
    }
    cache.set(cache_key, context, 300)

    return render(request, 'rankings_macd.html', context)
