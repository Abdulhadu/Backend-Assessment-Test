from django.urls import path
from .views import OrdersStreamingSearchAPIView


urlpatterns = [
    path("tenants/<uuid:tenant_id>/orders/search", OrdersStreamingSearchAPIView.as_view(), name="orders_streaming_search"),
]


