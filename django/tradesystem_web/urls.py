"""
URL configuration for tradesystem_web project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from django.views.generic.base import RedirectView

from . import views

urlpatterns = [
    path('admin/', admin.site.urls),
    path('tse_listings/', views.tse_listings_list, name='tse_listings_list'),
    path('stocks/<str:code>/chart/', views.stock_price_chart, name='stock_price_chart'),
    path('stocks/<str:code>/rsi/', views.stock_rsi_chart, name='stock_rsi_chart'),
    path('stocks/<str:code>/macd/', views.stock_macd_chart, name='stock_macd_chart'),
    path('stocks/<str:code>/arima-forecast/', views.stock_arima_forecast_chart, name='stock_arima_forecast_chart'),
    path('stocks/<str:code>/xgb-forecast/', views.stock_xgb_forecast_chart, name='stock_xgb_forecast_chart'),
    path(
        'rankings/ma-estimate/',
        RedirectView.as_view(pattern_name='rankings_ma_estimate_top', permanent=True, query_string=True),
        name='rankings_ma_estimate',
    ),
    path('rankings/ma-estimate/top/', views.rankings_ma_estimate_top, name='rankings_ma_estimate_top'),
    path('rankings/ma-estimate/bottom/', views.rankings_ma_estimate_bottom, name='rankings_ma_estimate_bottom'),
    path(
        'rankings/rsi/',
        RedirectView.as_view(pattern_name='rankings_rsi_top', permanent=True, query_string=True),
        name='rankings_rsi',
    ),
    path('rankings/rsi/top/', views.rankings_rsi_top, name='rankings_rsi_top'),
    path('rankings/rsi/bottom/', views.rankings_rsi_bottom, name='rankings_rsi_bottom'),
    path(
        'rankings/macd/',
        RedirectView.as_view(pattern_name='rankings_macd_top', permanent=True, query_string=True),
        name='rankings_macd',
    ),
    path('rankings/macd/top/', views.rankings_macd_top, name='rankings_macd_top'),
    path('rankings/macd/bottom/', views.rankings_macd_bottom, name='rankings_macd_bottom'),
    path(
        'rankings/arima-forecast-rate/',
        RedirectView.as_view(pattern_name='rankings_arima_forecast_rate_top', permanent=True, query_string=True),
        name='rankings_arima_forecast_rate',
    ),
    path('rankings/arima-forecast-rate/top/', views.rankings_arima_forecast_rate_top, name='rankings_arima_forecast_rate_top'),
    path('rankings/arima-forecast-rate/bottom/', views.rankings_arima_forecast_rate_bottom, name='rankings_arima_forecast_rate_bottom'),
    path(
        'rankings/xgb-forecast-rate/',
        RedirectView.as_view(pattern_name='rankings_xgb_forecast_rate_top', permanent=True, query_string=True),
        name='rankings_xgb_forecast_rate',
    ),
    path('rankings/xgb-forecast-rate/top/', views.rankings_xgb_forecast_rate_top, name='rankings_xgb_forecast_rate_top'),
    path('rankings/xgb-forecast-rate/bottom/', views.rankings_xgb_forecast_rate_bottom, name='rankings_xgb_forecast_rate_bottom'),
    path('results/arima-forecast/', views.results_arima_forecast, name='results_arima_forecast'),
    path('results/xgb-forecast/', views.results_xgb_forecast, name='results_xgb_forecast'),
]
