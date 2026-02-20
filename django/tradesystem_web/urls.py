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

from . import views

urlpatterns = [
    path('admin/', admin.site.urls),
    path('tse_listings/', views.tse_listings_list, name='tse_listings_list'),
    path('stocks/<str:code>/chart/', views.stock_price_chart, name='stock_price_chart'),
    path('stocks/<str:code>/rsi/', views.stock_rsi_chart, name='stock_rsi_chart'),
    path('stocks/<str:code>/macd/', views.stock_macd_chart, name='stock_macd_chart'),
    path('stocks/<str:code>/arima-forecast/', views.stock_arima_forecast_chart, name='stock_arima_forecast_chart'),
    path('rankings/ma-estimate/', views.rankings_ma_estimate, name='rankings_ma_estimate'),
    path('rankings/rsi/', views.rankings_rsi, name='rankings_rsi'),
    path('rankings/macd/', views.rankings_macd, name='rankings_macd'),
    path('rankings/arima-forecast-rate/', views.rankings_arima_forecast_rate, name='rankings_arima_forecast_rate'),
    path('rankings/xgb-signal-rate/', views.rankings_xgb_signal_rate, name='rankings_xgb_signal_rate'),
    path('results/arima-forecast/', views.results_arima_forecast, name='results_arima_forecast'),
    path('results/xgb-signal/', views.results_xgb_signal, name='results_xgb_signal'),
]
