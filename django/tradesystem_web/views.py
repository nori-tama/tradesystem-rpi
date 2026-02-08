from django.db import connection
from django.shortcuts import render


def tse_listings_list(request):
    with connection.cursor() as cursor:
        cursor.execute(
            """
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
            ORDER BY code
            """
        )
        columns = [col[0] for col in cursor.description]
        listings = [dict(zip(columns, row)) for row in cursor.fetchall()]

    return render(
        request,
        'tse_listings_list.html',
        {'listings': listings},
    )
