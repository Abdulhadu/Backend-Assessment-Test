"""
URL patterns for the ingestions app.
"""
from django.urls import path, include
from . import views, comprehensive_views
from apps.orders.urls import urlpatterns as orders_urlpatterns
from apps.analytics.urls import urlpatterns as core_urlpatterns

app_name = 'ingestions'

urlpatterns = [
    # Comprehensive bulk ingestion endpoints (recommended)
    path('api/v1/ingest/comprehensive/', comprehensive_views.ComprehensiveBulkIngestionAPIView.as_view(), name='comprehensive_bulk_ingest'),
    
    # Legacy bulk ingestion endpoints (for backward compatibility)
    path('api/v1/ingest/orders/', views.BulkIngestionAPIView.as_view(), name='bulk_ingest_orders'),
    
    # Upload session management endpoints
    path('api/v1/ingest/sessions/', views.create_upload_session, name='create_upload_session'),
    path('api/v1/ingest/sessions/<str:upload_token>/status/', views.get_upload_status, name='get_upload_status'),
    path('api/v1/ingest/sessions/<str:upload_token>/resume/', views.resume_upload, name='resume_upload'),
]

