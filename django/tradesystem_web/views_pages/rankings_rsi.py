from django.core.cache import cache
from django.db import connection
from django.shortcuts import render


def rankings_rsi(request):
    selected_market = (request.GET.get("market") or "").strip()
    window = 14

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
            FROM stock_prices_daily_rsi
            WHERE `window` = %s
            """,
            [window],
        )
        latest_trade_date_row = cursor.fetchone()

    latest_trade_date = latest_trade_date_row[0] if latest_trade_date_row else None
    if latest_trade_date is None:
        return render(
            request,
            'rankings_rsi.html',
            {
                'rsi_top10': [],
                'rsi_bottom10': [],
                'markets': markets,
                'selected_market': selected_market,
                'window': window,
            },
        )

    cache_key = f'rankings_rsi:{latest_trade_date}:window:{window}:market:{selected_market or "all"}'
    cached_context = cache.get(cache_key)
    if cached_context is not None:
        return render(request, 'rankings_rsi.html', cached_context)

    with connection.cursor() as cursor:
        base_sql = """
            SELECT
                r.code,
                r.trade_date,
                r.rsi,
                l.name,
                l.market
            FROM stock_prices_daily_rsi r
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
            ) l ON l.code = r.code
            WHERE r.trade_date = %s
              AND r.`window` = %s
              AND r.rsi IS NOT NULL
        """

        top_params = [latest_trade_date, window]
        bottom_params = [latest_trade_date, window]
        if selected_market:
            base_sql += " AND l.market = %s"
            top_params.append(selected_market)
            bottom_params.append(selected_market)

        top_sql = base_sql + " ORDER BY r.rsi DESC LIMIT 10"
        bottom_sql = base_sql + " ORDER BY r.rsi ASC LIMIT 10"

        cursor.execute(top_sql, top_params)
        top_rows = cursor.fetchall()

        cursor.execute(bottom_sql, bottom_params)
        bottom_rows = cursor.fetchall()

    rsi_top10 = []
    for code, trade_date, rsi, name, market in top_rows:
        rsi_top10.append(
            {
                'code': code,
                'name': name or '-',
                'market': market or '-',
                'trade_date': trade_date,
                'rsi': float(rsi),
            }
        )

    rsi_bottom10 = []
    for code, trade_date, rsi, name, market in bottom_rows:
        rsi_bottom10.append(
            {
                'code': code,
                'name': name or '-',
                'market': market or '-',
                'trade_date': trade_date,
                'rsi': float(rsi),
            }
        )

    context = {
        'rsi_top10': rsi_top10,
        'rsi_bottom10': rsi_bottom10,
        'markets': markets,
        'selected_market': selected_market,
        'window': window,
    }
    cache.set(cache_key, context, 300)

    return render(request, 'rankings_rsi.html', context)
