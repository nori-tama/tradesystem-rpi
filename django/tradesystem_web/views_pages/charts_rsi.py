from datetime import timedelta

from django.db import connection
from django.shortcuts import render

from .common import calculate_exchange_business_days


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
