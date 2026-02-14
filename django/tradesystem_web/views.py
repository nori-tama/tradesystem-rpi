import sys
from datetime import timedelta
from pathlib import Path

from django.core.cache import cache
from django.core.paginator import Paginator
from django.db import connection
from django.shortcuts import render

COMMON_DIR = Path(__file__).resolve().parents[2] / 'scripts' / 'common'
if str(COMMON_DIR) not in sys.path:
    sys.path.append(str(COMMON_DIR))

from exchange_calendar import calculate_exchange_business_days


def tse_listings_list(request):
    selected_market = request.GET.get("market")
    selected_sector33 = request.GET.get("sector33")
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
            SELECT DISTINCT sector33_name
            FROM tse_listings
            WHERE sector33_name IS NOT NULL AND sector33_name <> ''
            ORDER BY sector33_name
            """
        )
        sector33_names = [row[0] for row in cursor.fetchall()]

        params = []
        where_clauses = []
        if selected_market:
            where_clauses.append("market = %s")
            params.append(selected_market)
        if selected_sector33:
            where_clauses.append("sector33_name = %s")
            params.append(selected_sector33)
        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)
        cursor.execute(
            f"""
            SELECT
                listing_date,
                code,
                name,
                market,
                sector33_code,
                sector33_name,
                sector17_code,
                sector17_name,
                scale_code,
                scale_name,
                updated_at
            FROM tse_listings
            {where_sql}
            ORDER BY code
            """,
            params,
        )
        columns = [col[0] for col in cursor.description]
        listings = [dict(zip(columns, row)) for row in cursor.fetchall()]

    paginator = Paginator(listings, 15)
    page_number = request.GET.get("page")
    page_obj = paginator.get_page(page_number)

    return render(
        request,
        'tse_listings_list.html',
        {
            'listings': page_obj,
            'page_obj': page_obj,
            'markets': markets,
            'selected_market': selected_market or "",
            'sector33_names': sector33_names,
            'selected_sector33': selected_sector33 or "",
        },
    )


def stock_price_chart(request, code):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT code, name, market
            FROM tse_listings
            WHERE code = %s
            LIMIT 1
            """,
            [code],
        )
        listing_row = cursor.fetchone()

        cursor.execute(
            """
            SELECT
                t.trade_date,
                t.open,
                t.high,
                t.low,
                t.close,
                t.ma5,
                t.ma25
            FROM (
                SELECT
                    d.trade_date,
                    d.open,
                    d.high,
                    d.low,
                    d.close,
                    m.ma5,
                    m.ma25
                FROM stock_prices_daily d
                LEFT JOIN stock_prices_daily_ma m
                  ON m.code = d.code
                 AND m.trade_date = d.trade_date
                WHERE d.code = %s
                ORDER BY d.trade_date DESC
                LIMIT 100
            ) t
            ORDER BY t.trade_date
            """,
            [code],
        )
        rows = cursor.fetchall()

    listing = {
        'code': code,
        'name': '',
        'market': '',
    }
    if listing_row:
        listing = {
            'code': listing_row[0],
            'name': listing_row[1],
            'market': listing_row[2],
        }

    chart_labels = []
    open_values = []
    high_values = []
    low_values = []
    close_values = []
    ma5_values = []
    ma25_values = []
    monday_tick_labels = []
    non_business_day_labels = []
    missing_plot_day_labels = []
    business_day_set = set()
    filtered_rows = rows

    if rows:
        start_date = rows[0][0]
        end_date = rows[-1][0]
        business_days = calculate_exchange_business_days(start_date, end_date)
        business_day_set = set(business_days)
        filtered_rows = [row for row in rows if row[0] in business_day_set]

    for trade_date, open_price, high_price, low_price, close_price, ma5, ma25 in filtered_rows:
        chart_labels.append(trade_date.strftime('%Y-%m-%d'))
        open_values.append(float(open_price) if open_price is not None else None)
        high_values.append(float(high_price) if high_price is not None else None)
        low_values.append(float(low_price) if low_price is not None else None)
        close_values.append(float(close_price) if close_price is not None else None)
        ma5_values.append(float(ma5) if ma5 is not None else None)
        ma25_values.append(float(ma25) if ma25 is not None else None)

    if filtered_rows:
        start_date = filtered_rows[0][0]
        end_date = filtered_rows[-1][0]
        chart_day_set = {row[0] for row in filtered_rows}

        week_start = start_date - timedelta(days=start_date.weekday())
        while week_start <= end_date:
            week_end = week_start + timedelta(days=6)
            label_day = None

            current = week_start
            while current <= week_end and current <= end_date:
                if current >= start_date and current in business_day_set and current in chart_day_set:
                    label_day = current
                    break
                current += timedelta(days=1)

            if label_day is None:
                current = week_start
                while current <= week_end and current <= end_date:
                    if current >= start_date and current in business_day_set:
                        label_day = current
                        break
                    current += timedelta(days=1)

            if label_day is not None:
                monday_tick_labels.append(label_day.strftime('%Y-%m-%d'))

            week_start += timedelta(days=7)

        current = start_date
        while current <= end_date:
            if current not in business_day_set:
                non_business_day_labels.append(current.strftime('%Y-%m-%d'))
            elif current not in chart_day_set:
                missing_plot_day_labels.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)

    return render(
        request,
        'stock_price_chart.html',
        {
            'listing': listing,
            'chart_labels': chart_labels,
            'open_values': open_values,
            'high_values': high_values,
            'low_values': low_values,
            'close_values': close_values,
            'ma5_values': ma5_values,
            'ma25_values': ma25_values,
            'monday_tick_labels': monday_tick_labels,
            'non_business_day_labels': non_business_day_labels,
            'missing_plot_day_labels': missing_plot_day_labels,
        },
    )


def ma_estimate_rankings(request):
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

        cursor.execute("SELECT MAX(trade_date) FROM stock_prices_daily_ma")
        latest_trade_date_row = cursor.fetchone()

    latest_trade_date = latest_trade_date_row[0] if latest_trade_date_row else None
    if latest_trade_date is None:
        return render(
            request,
            'ma_estimate_rankings.html',
            {
                'rise_top10': [],
                'fall_top10': [],
                'markets': markets,
                'selected_market': selected_market,
            },
        )

    cache_key = f'ma_estimate_rankings:{latest_trade_date}:market:{selected_market or "all"}'
    cached_context = cache.get(cache_key)
    if cached_context is not None:
        return render(request, 'ma_estimate_rankings.html', cached_context)

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

        cursor.execute(rise_sql, rise_params)
        rise_rows = cursor.fetchall()

        cursor.execute(fall_sql, fall_params)
        fall_rows = cursor.fetchall()

    rise_top10 = []
    for code, trade_date, ma5, ma25, estimate_rate, name, market in rise_rows:
        rise_top10.append(
            {
                'code': code,
                'name': name or '-',
                'market': market or '-',
                'trade_date': trade_date,
                'ma5': float(ma5),
                'ma25': float(ma25),
                'estimate_rate': float(estimate_rate),
            }
        )

    fall_top10 = []
    for code, trade_date, ma5, ma25, estimate_rate, name, market in fall_rows:
        fall_top10.append(
            {
                'code': code,
                'name': name or '-',
                'market': market or '-',
                'trade_date': trade_date,
                'ma5': float(ma5),
                'ma25': float(ma25),
                'estimate_rate': float(estimate_rate),
            }
        )

    context = {
        'rise_top10': rise_top10,
        'fall_top10': fall_top10,
        'markets': markets,
        'selected_market': selected_market,
    }
    cache.set(cache_key, context, 300)

    return render(request, 'ma_estimate_rankings.html', context)
