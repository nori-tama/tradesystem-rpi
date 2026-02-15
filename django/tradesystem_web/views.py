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

from exchange_calendar import calculate_exchange_business_days, shift_exchange_business_day


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


def stock_rsi_chart(request, code):
    window = request.GET.get("window", "14")
    try:
        rsi_window = int(window)
    except (TypeError, ValueError):
        rsi_window = 14

    if rsi_window <= 0:
        rsi_window = 14

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
                t.close
            FROM (
                SELECT
                    d.trade_date,
                    d.open,
                    d.high,
                    d.low,
                    d.close
                FROM stock_prices_daily d
                WHERE d.code = %s
                ORDER BY d.trade_date DESC
                LIMIT 100
            ) t
            ORDER BY t.trade_date
            """,
            [code],
        )
        price_rows = cursor.fetchall()

        cursor.execute(
            """
            SELECT t.trade_date, t.rsi
            FROM (
                SELECT trade_date, rsi
                FROM stock_prices_daily_rsi
                WHERE code = %s
                  AND `window` = %s
                ORDER BY trade_date DESC
                LIMIT 100
            ) t
            ORDER BY t.trade_date
            """,
            [code, rsi_window],
        )
        rsi_rows = cursor.fetchall()

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

    price_chart_labels = []
    open_values = []
    high_values = []
    low_values = []
    close_values = []
    monday_tick_labels = []
    non_business_day_labels = []
    missing_plot_day_labels = []
    business_day_set = set()
    filtered_price_rows = price_rows

    if price_rows:
        start_date = price_rows[0][0]
        end_date = price_rows[-1][0]
        business_days = calculate_exchange_business_days(start_date, end_date)
        business_day_set = set(business_days)
        filtered_price_rows = [row for row in price_rows if row[0] in business_day_set]

    for trade_date, open_price, high_price, low_price, close_price in filtered_price_rows:
        price_chart_labels.append(trade_date.strftime('%Y-%m-%d'))
        open_values.append(float(open_price) if open_price is not None else None)
        high_values.append(float(high_price) if high_price is not None else None)
        low_values.append(float(low_price) if low_price is not None else None)
        close_values.append(float(close_price) if close_price is not None else None)

    if filtered_price_rows:
        start_date = filtered_price_rows[0][0]
        end_date = filtered_price_rows[-1][0]
        chart_day_set = {row[0] for row in filtered_price_rows}

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

    chart_labels = []
    rsi_values = []
    overbought_values = []
    oversold_values = []

    for trade_date, rsi in rsi_rows:
        chart_labels.append(trade_date.strftime('%Y-%m-%d'))
        rsi_values.append(float(rsi) if rsi is not None else None)
        overbought_values.append(70.0)
        oversold_values.append(30.0)

    return render(
        request,
        'stock_rsi_chart.html',
        {
            'listing': listing,
            'rsi_window': rsi_window,
            'price_chart_labels': price_chart_labels,
            'open_values': open_values,
            'high_values': high_values,
            'low_values': low_values,
            'close_values': close_values,
            'monday_tick_labels': monday_tick_labels,
            'non_business_day_labels': non_business_day_labels,
            'missing_plot_day_labels': missing_plot_day_labels,
            'chart_labels': chart_labels,
            'rsi_values': rsi_values,
            'overbought_values': overbought_values,
            'oversold_values': oversold_values,
        },
    )


def rankings_ma_estimate(request):
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
            'rankings_ma_estimate.html',
            {
                'rise_top10': [],
                'fall_top10': [],
                'markets': markets,
                'selected_market': selected_market,
            },
        )

    cache_key = f'rankings_ma_estimate:{latest_trade_date}:market:{selected_market or "all"}'
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

    return render(request, 'rankings_ma_estimate.html', context)


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
