from datetime import timedelta

from django.db import connection
from django.shortcuts import render

from .common import calculate_exchange_business_days


def stock_arima_forecast_chart(request, code):
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
        price_rows = cursor.fetchall()

        cursor.execute(
            """
            SELECT MAX(forecast_base_date)
            FROM stock_prices_daily_arima_forecast
            WHERE code = %s
            """,
            [code],
        )
        latest_base_date_row = cursor.fetchone()

        latest_base_date = latest_base_date_row[0] if latest_base_date_row else None

        forecast_rows = []
        base_close = None
        if latest_base_date is not None:
            cursor.execute(
                """
                SELECT `close`
                FROM stock_prices_daily
                WHERE code = %s
                  AND trade_date = %s
                LIMIT 1
                """,
                [code, latest_base_date],
            )
            base_close_row = cursor.fetchone()
            if base_close_row is not None and base_close_row[0] is not None:
                base_close = float(base_close_row[0])

            cursor.execute(
                """
                SELECT target_trade_date, predicted_close, horizon
                FROM stock_prices_daily_arima_forecast
                WHERE code = %s
                  AND forecast_base_date = %s
                  AND predicted_close IS NOT NULL
                ORDER BY horizon
                """,
                [code, latest_base_date],
            )
            forecast_rows = cursor.fetchall()

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

    price_labels = []
    open_values = []
    high_values = []
    low_values = []
    close_values = []
    ma5_values = []
    ma25_values = []

    filtered_price_rows = price_rows
    price_business_day_set = set()
    if price_rows:
        start_date = price_rows[0][0]
        end_date = price_rows[-1][0]
        business_days = calculate_exchange_business_days(start_date, end_date)
        price_business_day_set = set(business_days)
        filtered_price_rows = [row for row in price_rows if row[0] in price_business_day_set]

    for trade_date, open_price, high_price, low_price, close_price, ma5, ma25 in filtered_price_rows:
        price_labels.append(trade_date.strftime('%Y-%m-%d'))
        open_values.append(float(open_price) if open_price is not None else None)
        high_values.append(float(high_price) if high_price is not None else None)
        low_values.append(float(low_price) if low_price is not None else None)
        close_values.append(float(close_price) if close_price is not None else None)
        ma5_values.append(float(ma5) if ma5 is not None else None)
        ma25_values.append(float(ma25) if ma25 is not None else None)

    forecast_labels = []
    forecast_values = []
    forecast_base_date_label = None
    if latest_base_date is not None:
        forecast_base_date_label = latest_base_date.strftime('%Y-%m-%d')

    if latest_base_date is not None and base_close is not None:
        forecast_labels.append(latest_base_date.strftime('%Y-%m-%d'))
        forecast_values.append(base_close)

    for target_trade_date, predicted_close, _horizon in forecast_rows:
        if target_trade_date is None or predicted_close is None:
            continue
        forecast_labels.append(target_trade_date.strftime('%Y-%m-%d'))
        forecast_values.append(float(predicted_close))

    monday_tick_labels = []
    non_business_day_labels = []
    missing_plot_day_labels = []

    timeline_start = None
    timeline_end = None

    if filtered_price_rows:
        timeline_start = filtered_price_rows[0][0]
        timeline_end = filtered_price_rows[-1][0]

    if forecast_rows:
        forecast_end = max(row[0] for row in forecast_rows if row[0] is not None)
        if forecast_end is not None:
            if timeline_end is None or forecast_end > timeline_end:
                timeline_end = forecast_end

    if latest_base_date is not None:
        if timeline_start is None or latest_base_date < timeline_start:
            timeline_start = latest_base_date
        if timeline_end is None or latest_base_date > timeline_end:
            timeline_end = latest_base_date

    if timeline_start is not None and timeline_end is not None:
        business_days = calculate_exchange_business_days(timeline_start, timeline_end)
        business_day_set = set(business_days)

        price_day_set = {row[0] for row in filtered_price_rows}
        forecast_day_set = set()
        if latest_base_date is not None:
            forecast_day_set.add(latest_base_date)
        for target_trade_date, _predicted_close, _horizon in forecast_rows:
            if target_trade_date is not None:
                forecast_day_set.add(target_trade_date)

        visible_day_set = price_day_set | forecast_day_set

        week_start = timeline_start - timedelta(days=timeline_start.weekday())
        while week_start <= timeline_end:
            week_end = week_start + timedelta(days=6)
            label_day = None

            current = week_start
            while current <= week_end and current <= timeline_end:
                if current >= timeline_start and current in business_day_set and current in visible_day_set:
                    label_day = current
                    break
                current += timedelta(days=1)

            if label_day is None:
                current = week_start
                while current <= week_end and current <= timeline_end:
                    if current >= timeline_start and current in business_day_set:
                        label_day = current
                        break
                    current += timedelta(days=1)

            if label_day is not None:
                monday_tick_labels.append(label_day.strftime('%Y-%m-%d'))

            week_start += timedelta(days=7)

        current = timeline_start
        while current <= timeline_end:
            if current not in business_day_set:
                non_business_day_labels.append(current.strftime('%Y-%m-%d'))
            elif current not in visible_day_set:
                missing_plot_day_labels.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)

    return render(
        request,
        'stock_arima_forecast_chart.html',
        {
            'listing': listing,
            'price_labels': price_labels,
            'open_values': open_values,
            'high_values': high_values,
            'low_values': low_values,
            'close_values': close_values,
            'ma5_values': ma5_values,
            'ma25_values': ma25_values,
            'forecast_labels': forecast_labels,
            'forecast_values': forecast_values,
            'forecast_base_date': latest_base_date,
            'forecast_base_date_label': forecast_base_date_label,
            'monday_tick_labels': monday_tick_labels,
            'non_business_day_labels': non_business_day_labels,
            'missing_plot_day_labels': missing_plot_day_labels,
        },
    )
