"""
URL patterns for the tenants app.
"""
from django.urls import path
from . import price_views

app_name = 'tenants'

urlpatterns = [
    # Price-sensing endpoints
    path('tenants/<uuid:tenant_id>/products/<uuid:product_id>/price-event/', price_views.PriceEventAPIView.as_view(), name='price_event'),
    path('tenants/<uuid:tenant_id>/products/<uuid:product_id>/price-anomalies/', price_views.PriceAnomaliesAPIView.as_view(), name='price_anomalies'),
]
