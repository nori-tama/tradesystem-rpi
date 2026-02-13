from django.core.paginator import Paginator
from django.db import connection
from django.shortcuts import render


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

    for trade_date, open_price, high_price, low_price, close_price, ma5, ma25 in rows:
        chart_labels.append(trade_date.strftime('%Y-%m-%d'))
        open_values.append(float(open_price) if open_price is not None else None)
        high_values.append(float(high_price) if high_price is not None else None)
        low_values.append(float(low_price) if low_price is not None else None)
        close_values.append(float(close_price) if close_price is not None else None)
        ma5_values.append(float(ma5) if ma5 is not None else None)
        ma25_values.append(float(ma25) if ma25 is not None else None)

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
        },
    )
