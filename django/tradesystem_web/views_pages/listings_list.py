from django.core.paginator import Paginator
from django.db import connection
from django.shortcuts import render


def tse_listings_list(request):
    selected_code = request.GET.get("code")
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
        if selected_code:
            where_clauses.append("code LIKE %s")
            params.append(f"%{selected_code}%")
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
            'selected_code': selected_code or "",
            'markets': markets,
            'selected_market': selected_market or "",
            'sector33_names': sector33_names,
            'selected_sector33': selected_sector33 or "",
        },
    )
