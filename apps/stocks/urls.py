from django.urls import path
from apps.stocks.views import BulkStockUpdateAPIView

urlpatterns = [
    path('tenants/<str:tenant_id>/stock/bulk_update', BulkStockUpdateAPIView.as_view()),
]