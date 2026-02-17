from datetime import timedelta

from django.db import connection
from django.shortcuts import render

from .common import calculate_exchange_business_days


def stock_macd_chart(request, code):
    window_short = 12
    window_long = 26
    window_signal = 9

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
            SELECT t.trade_date, t.macd, t.`signal`, t.histogram
            FROM (
                SELECT trade_date, macd, `signal`, histogram
                FROM stock_prices_daily_macd
                WHERE code = %s
                  AND window_short = %s
                  AND window_long = %s
                  AND window_signal = %s
                ORDER BY trade_date DESC
                LIMIT 100
            ) t
            ORDER BY t.trade_date
            """,
            [code, window_short, window_long, window_signal],
        )
        macd_rows = cursor.fetchall()

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
    macd_values = []
    signal_values = []
    histogram_values = []

    for trade_date, macd, signal_v, histogram in macd_rows:
        chart_labels.append(trade_date.strftime('%Y-%m-%d'))
        macd_values.append(float(macd) if macd is not None else None)
        signal_values.append(float(signal_v) if signal_v is not None else None)
        histogram_values.append(float(histogram) if histogram is not None else None)

    return render(
        request,
        'chart_macd.html',
        {
            'listing': listing,
            'window_short': window_short,
            'window_long': window_long,
            'window_signal': window_signal,
            'price_chart_labels': price_chart_labels,
            'open_values': open_values,
            'high_values': high_values,
            'low_values': low_values,
            'close_values': close_values,
            'monday_tick_labels': monday_tick_labels,
            'non_business_day_labels': non_business_day_labels,
            'missing_plot_day_labels': missing_plot_day_labels,
            'chart_labels': chart_labels,
            'macd_values': macd_values,
            'signal_values': signal_values,
            'histogram_values': histogram_values,
        },
    )
