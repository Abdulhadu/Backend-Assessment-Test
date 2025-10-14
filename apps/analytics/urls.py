# apps/analytics/urls.py
from django.urls import path
from .views.metrics import SalesMetricsAPIView
from .views.price_sensing import PriceEventAPIView,PriceAnomaliesAPIView
from .views.export import ExportAPIView, ExportDownloadAPIView

urlpatterns = [
    path("tenants/<uuid:tenant_id>/metrics/sales", SalesMetricsAPIView.as_view(), name="sales_metrics"),
    path('tenants/<uuid:tenant_id>/products/<uuid:product_id>/price-event/', PriceEventAPIView.as_view(), name='price_event'),
    path('tenants/<uuid:tenant_id>/products/<uuid:product_id>/price-anomalies/', PriceAnomaliesAPIView.as_view(), name='price_anomalies'),
    path('tenants/<uuid:tenant_id>/reports/export', ExportAPIView.as_view(), name='export_create'),
    path('tenants/<uuid:tenant_id>/reports/export/<uuid:export_id>/', ExportAPIView.as_view(), name='export_status'),
    path('tenants/<uuid:tenant_id>/reports/export/<uuid:export_id>/download', ExportDownloadAPIView.as_view(), name='export_download'),
]
