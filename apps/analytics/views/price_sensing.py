"""
Price-sensing API endpoints for real-time price monitoring and anomaly detection.
"""
import hashlib
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from decimal import Decimal

from django.core.cache import cache
from django.db import transaction
from django.http import StreamingHttpResponse
from django.utils import timezone
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiResponse, OpenApiExample
from drf_spectacular.types import OpenApiTypes

from apps.tenants.models import Tenant
from apps.products.models import Product
from apps.analytics.models import PriceEvent, IdempotencyKey

logger = logging.getLogger(__name__)


class PriceEventAPIView(APIView):
    """
    Webhook-style endpoint for price update events with rate limiting and anomaly detection.
    """
    permission_classes = [AllowAny]

    @method_decorator(csrf_exempt)
    @extend_schema(
        tags=["price-sensing"],
        summary="Submit price event (webhook-style)",
        description="Accepts price update events with rate limiting, anomaly detection, and idempotency support.",
        parameters=[
            OpenApiParameter("tenant_id", OpenApiTypes.STR, OpenApiParameter.PATH, required=True),
            OpenApiParameter("product_id", OpenApiTypes.STR, OpenApiParameter.PATH, required=True),
            OpenApiParameter("X-API-Key", OpenApiTypes.STR, OpenApiParameter.HEADER, required=True),
            OpenApiParameter("Idempotency-Key", OpenApiTypes.STR, OpenApiParameter.HEADER, required=True),
        ],
        request={
            'application/json': {
                'type': 'object',
                'properties': {
                    'old_price': {'type': 'number', 'description': 'Previous price'},
                    'new_price': {'type': 'number', 'description': 'New price'},
                    'source': {'type': 'string', 'description': 'Price source (e.g., "manual", "api", "scraper")'},
                    'metadata': {'type': 'object', 'description': 'Additional event metadata'},
                },
                'required': ['old_price', 'new_price']
            }
        },
        responses={
            201: OpenApiResponse(
                response=OpenApiTypes.OBJECT,
                description='Price event processed',
                examples=[OpenApiExample(
                    'SuccessResponse',
                    summary='Event Processed',
                    value={
                        'event_id': '123e4567-e89b-12d3-a456-426614174000',
                        'anomaly_detected': True,
                        'anomaly_reason': 'Price increased by 25.5%',
                        'status': 'processed',
                        'processed_at': '2025-01-15T10:30:00Z'
                    }
                )]
            ),
            400: OpenApiResponse(response=OpenApiTypes.OBJECT, description='Bad request'),
            401: OpenApiResponse(response=OpenApiTypes.OBJECT, description='Unauthorized'),
            429: OpenApiResponse(response=OpenApiTypes.OBJECT, description='Rate limited'),
        }
    )
    def post(self, request, tenant_id: str, product_id: str):
        """Process price update event with rate limiting and anomaly detection."""
        try:
            # Validate tenant and product
            tenant = self._validate_tenant(request.headers.get('X-API-Key'))
            if not tenant:
                return Response(
                    {'error': 'Invalid API key'}, 
                    status=status.HTTP_401_UNAUTHORIZED
                )

            # if str(tenant.tenant_id) != tenant_id:
            #     return Response(
            #         {'error': 'Tenant ID mismatch'}, 
            #         status=status.HTTP_400_BAD_REQUEST
            #     )

            product = self._get_product(tenant, product_id)
            if not product:
                return Response(
                    {'error': 'Product not found'}, 
                    status=status.HTTP_404_NOT_FOUND
                )

            # Check idempotency
            idempotency_key = request.headers.get('Idempotency-Key')
            if not idempotency_key:
                return Response(
                    {'error': 'Idempotency-Key header required'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )

            idempotency_result = self._check_idempotency(tenant, idempotency_key)
            if idempotency_result:
                return Response(idempotency_result, status=status.HTTP_200_OK)

            # Rate limiting
            if not self._check_rate_limit(tenant, product):
                return Response(
                    {'error': 'Rate limit exceeded', 'retry_after': 60}, 
                    status=status.HTTP_429_TOO_MANY_REQUESTS
                )

            # Validate request data
            data = request.data
            old_price = Decimal(str(data.get('old_price', 0)))
            new_price = Decimal(str(data.get('new_price', 0)))
            source = data.get('source', 'api')
            metadata = data.get('metadata', {})

            if old_price <= 0 or new_price <= 0:
                return Response(
                    {'error': 'Prices must be positive'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Calculate percentage change
            if old_price > 0:
                pct_change = ((new_price - old_price) / old_price) * 100
            else:
                pct_change = 0

            # Anomaly detection
            anomaly_detected, anomaly_reason = self._detect_anomaly(pct_change, old_price, new_price)

            # Create price event
            with transaction.atomic():
                price_event = PriceEvent.objects.create(
                    tenant=tenant,
                    product=product,
                    old_price=old_price,
                    new_price=new_price,
                    pct_change=pct_change,
                    anomaly_flag=anomaly_detected,
                    meta={
                        'source': source,
                        'anomaly_reason': anomaly_reason,
                        'metadata': metadata,
                        'processed_at': timezone.now().isoformat()
                    }
                )

                # Record idempotency
                IdempotencyKey.objects.create(
                    tenant=tenant,
                    idempotency_key=idempotency_key,
                    request_hash=hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest(),
                    response_summary={
                        'event_id': str(price_event.event_id),
                        'anomaly_detected': anomaly_detected,
                        'anomaly_reason': anomaly_reason,
                        'status': 'processed'
                    },
                    status='completed',
                    expires_at=timezone.now() + timedelta(hours=24)
                )

            logger.info(f"Price event processed: {price_event.event_id}, anomaly: {anomaly_detected}")

            return Response({
                'event_id': str(price_event.event_id),
                'anomaly_detected': anomaly_detected,
                'anomaly_reason': anomaly_reason,
                'pct_change': float(pct_change),
                'status': 'processed',
                'processed_at': price_event.received_at.isoformat()
            }, status=status.HTTP_201_CREATED)

        except Exception as e:
            logger.error(f"Price event processing failed: {str(e)}", exc_info=True)
            return Response(
                {'error': 'Internal server error', 'details': str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def _validate_tenant(self, api_key: str) -> Optional[Tenant]:
        """Validate tenant API key."""
        try:
            # Accept either raw API key (hash it) or pre-hashed value
            candidate_hashes = {api_key}
            try:
                candidate_hashes.add(hashlib.sha256(api_key.encode()).hexdigest())
            except Exception:
                pass
            return Tenant.objects.get(api_key_hash__in=list(candidate_hashes))
        except Tenant.DoesNotExist:
            return None
  

    def _get_product(self, tenant: Tenant, product_id: str) -> Optional[Product]:
        """Get product for tenant."""
        try:
            return Product.objects.get(tenant=tenant, product_id=product_id)
        except Product.DoesNotExist:
            return None

    def _check_idempotency(self, tenant: Tenant, idempotency_key: str) -> Optional[Dict]:
        """Check if request is idempotent."""
        try:
            idempotency_record = IdempotencyKey.objects.get(
                tenant=tenant,
                idempotency_key=idempotency_key
            )
            
            if idempotency_record.status == 'completed':
                return {
                    'event_id': str(idempotency_record.response_summary.get('event_id')),
                    'anomaly_detected': idempotency_record.response_summary.get('anomaly_detected', False),
                    'anomaly_reason': idempotency_record.response_summary.get('anomaly_reason'),
                    'status': 'already_processed',
                    'processed_at': idempotency_record.created_at.isoformat()
                }
            
            return None
            
        except IdempotencyKey.DoesNotExist:
            return None

    def _check_rate_limit(self, tenant: Tenant, product: Product) -> bool:
        """Check rate limiting per tenant and per product."""
        now = timezone.now()
        
        # Per-tenant rate limit: 100 requests per minute
        tenant_key = f"price_event_rate_limit:tenant:{tenant.tenant_id}"
        tenant_count = cache.get(tenant_key, 0)
        if tenant_count >= 100:
            return False
        
        # Per-product rate limit: 10 requests per minute
        product_key = f"price_event_rate_limit:product:{product.product_id}"
        product_count = cache.get(product_key, 0)
        if product_count >= 10:
            return False
        
        # Increment counters
        cache.set(tenant_key, tenant_count + 1, 60)
        cache.set(product_key, product_count + 1, 60)
        
        return True

    def _detect_anomaly(self, pct_change: Decimal, old_price: Decimal, new_price: Decimal) -> tuple[bool, str]:
        """Detect price anomalies based on percentage change and thresholds."""
        abs_change = abs(pct_change)
        
        # Define anomaly thresholds
        if abs_change >= 50:  # 50% or more change
            return True, f"Extreme price change: {pct_change:.1f}%"
        elif abs_change >= 25:  # 25% or more change
            return True, f"Significant price change: {pct_change:.1f}%"
        elif abs_change >= 10 and old_price > 100:  # 10% change for expensive items
            return True, f"Notable price change for high-value item: {pct_change:.1f}%"
        
        return False, None


class PriceAnomaliesAPIView(APIView):
    """
    Streaming endpoint for recent price anomalies.
    """
    permission_classes = [AllowAny]

    @extend_schema(
        tags=["price-sensing"],
        summary="Stream price anomalies",
        description="Stream recent price anomalies as NDJSON for real-time monitoring.",
        parameters=[
            OpenApiParameter("tenant_id", OpenApiTypes.STR, OpenApiParameter.PATH, required=True),
            OpenApiParameter("product_id", OpenApiTypes.STR, OpenApiParameter.PATH, required=True),
            OpenApiParameter("X-API-Key", OpenApiTypes.STR, OpenApiParameter.HEADER, required=True),
            OpenApiParameter("hours", OpenApiTypes.INT, OpenApiParameter.QUERY, required=False,
                             description="Hours to look back (default: 24)"),
            OpenApiParameter("limit", OpenApiTypes.INT, OpenApiParameter.QUERY, required=False,
                             description="Max anomalies to return (default: 100)"),
        ],
        responses={
            200: OpenApiResponse(
                response=OpenApiTypes.STR,
                description="NDJSON stream of price anomalies",
                examples=[OpenApiExample(
                    "NDJSON",
                    value=(
                        '{"event_id": "...", "product_id": "...", "old_price": 100.0, "new_price": 125.0, "pct_change": 25.0, "anomaly_reason": "Significant price change: 25.0%", "received_at": "2025-01-15T10:30:00Z"}\n'
                        '{"event_id": "...", "product_id": "...", "old_price": 50.0, "new_price": 75.0, "pct_change": 50.0, "anomaly_reason": "Extreme price change: 50.0%", "received_at": "2025-01-15T09:15:00Z"}\n'
                    ),
                )],
            ),
            401: OpenApiResponse(response=OpenApiTypes.OBJECT, description='Unauthorized'),
            404: OpenApiResponse(response=OpenApiTypes.OBJECT, description='Product not found'),
        },
    )
    def get(self, request, tenant_id: str, product_id: str):
        """Stream recent price anomalies for a product."""
        try:
            # Validate tenant and product
            tenant = self._validate_tenant(request.headers.get('X-API-Key'))
            if not tenant:
                return Response(
                    {'error': 'Invalid API key'}, 
                    status=status.HTTP_401_UNAUTHORIZED
                )

            # if str(tenant.tenant_id) != tenant_id:
            #     return Response(
            #         {'error': 'Tenant ID mismatch'}, 
            #         status=status.HTTP_400_BAD_REQUEST
            #     )

            product = self._get_product(tenant, product_id)
            if not product:
                return Response(
                    {'error': 'Product not found'}, 
                    status=status.HTTP_404_NOT_FOUND
                )

            # Parse query parameters
            hours = int(request.query_params.get('hours', 24))
            limit = min(int(request.query_params.get('limit', 100)), 1000)
            
            # Calculate time range
            since = timezone.now() - timedelta(hours=hours)
            
            # Get anomalies
            anomalies = PriceEvent.objects.filter(
                tenant=tenant,
                product=product,
                anomaly_flag=True,
                received_at__gte=since
            ).order_by('-received_at')[:limit]

            def stream_anomalies():
                """Stream anomalies as NDJSON."""
                count = 0
                for anomaly in anomalies.iterator(chunk_size=100):
                    anomaly_data = {
                        'event_id': str(anomaly.event_id),
                        'product_id': str(anomaly.product.product_id),
                        'product_name': anomaly.product.name,
                        'old_price': float(anomaly.old_price),
                        'new_price': float(anomaly.new_price),
                        'pct_change': float(anomaly.pct_change),
                        'anomaly_reason': anomaly.meta.get('anomaly_reason'),
                        'source': anomaly.meta.get('source'),
                        'received_at': anomaly.received_at.isoformat(),
                        'metadata': anomaly.meta.get('metadata', {})
                    }
                    yield json.dumps(anomaly_data) + '\n'
                    count += 1
                
                # Send summary
                summary = {
                    '_meta': {
                        'total_anomalies': count,
                        'time_range_hours': hours,
                        'product_id': str(product.product_id),
                        'streamed_at': timezone.now().isoformat()
                    }
                }
                yield json.dumps(summary) + '\n'

            return StreamingHttpResponse(
                stream_anomalies(),
                content_type='application/x-ndjson',
                headers={
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive',
                }
            )

        except Exception as e:
            logger.error(f"Anomaly streaming failed: {str(e)}", exc_info=True)
            return Response(
                {'error': 'Internal server error', 'details': str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def _validate_tenant(self, api_key: str) -> Optional[Tenant]:
        """Validate tenant API key."""
        if not api_key:
            return None
        try:
            candidate_hashes = {api_key}
            try:
                candidate_hashes.add(hashlib.sha256(api_key.encode()).hexdigest())
            except Exception:
                pass
            return Tenant.objects.get(api_key_hash__in=list(candidate_hashes))
        except Tenant.DoesNotExist:
            return None

    def _get_product(self, tenant: Tenant, product_id: str) -> Optional[Product]:
        """Get product for tenant."""
        try:
            return Product.objects.get(tenant=tenant, product_id=product_id)
        except Product.DoesNotExist:
            return None
