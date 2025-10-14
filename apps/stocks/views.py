from typing import List, Dict, Any
import json
import logging

from django.db import transaction
from django.http import JsonResponse
from django.utils.timezone import now
from django.utils.dateparse import parse_datetime
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework.parsers import MultiPartParser, FormParser
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiResponse
from drf_spectacular.types import OpenApiTypes

from apps.stocks.models import StockLevel, StockEvent
from apps.products.models import Product

logger = logging.getLogger(__name__)


class BulkStockUpdateAPIView(APIView):
    permission_classes = [AllowAny]
    parser_classes = [MultiPartParser, FormParser]

    @extend_schema(
        tags=["stocks"],
        summary="Bulk stock update from file upload (NDJSON)",
        parameters=[
            OpenApiParameter("tenant_id", OpenApiTypes.STR, OpenApiParameter.PATH, required=True),
            OpenApiParameter("X-API-Key", OpenApiTypes.STR, OpenApiParameter.HEADER, required=True),
        ],
        request={
            "multipart/form-data": {
                "type": "object",
                "properties": {
                    "file": {
                        "type": "string",
                        "format": "binary",
                        "description": "NDJSON file with stock events"
                    }
                }
            }
        },
        responses={
            200: OpenApiResponse(OpenApiTypes.OBJECT, description="Success or partial_success"),
            400: OpenApiResponse(OpenApiTypes.OBJECT, description="Bad request"),
        },
    )
    def post(self, request, tenant_id: str):
        """
        Process NDJSON file upload with stock events.
        Applies updates transactionally per product with conflict detection.
        """
        from apps.core.auth import authenticate_tenant
        
        tenant = authenticate_tenant(request, tenant_id)
        if isinstance(tenant, JsonResponse): 
            return tenant
            
        if 'file' not in request.FILES:
            return Response({"error": "file required"}, status=status.HTTP_400_BAD_REQUEST)

        uploaded_file = request.FILES['file']
        if not uploaded_file.name.endswith('.ndjson'):
            return Response({"error": "file must be .ndjson"}, status=status.HTTP_400_BAD_REQUEST)

        # Parse NDJSON file
        try:
            events = []
            for line_num, line in enumerate(uploaded_file, 1):
                line = line.decode('utf-8').strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                    events.append(event)
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON on line {line_num}: {e}")
                    continue
        except Exception as e:
            return Response({"error": f"file parsing failed: {str(e)}"}, status=status.HTTP_400_BAD_REQUEST)

        if not events:
            return Response({"error": "no valid events found"}, status=status.HTTP_400_BAD_REQUEST)

        # Group events by product for transactional processing
        product_events = {}
        for event in events:
            product_id = event.get('product_id')
            if not product_id:
                continue
            if product_id not in product_events:
                product_events[product_id] = []
            product_events[product_id].append(event)

        conflicts: List[Dict[str, Any]] = []
        applied: List[Dict[str, Any]] = []
        total_events_processed = 0

        # Process each product's events transactionally
        for product_id, product_event_list in product_events.items():
            try:
                with transaction.atomic():
                    # Validate product exists
                    try:
                        product = Product.objects.get(pk=product_id, tenant_id=tenant_id)
                    except Product.DoesNotExist:
                        conflicts.append({"product_id": product_id, "reason": "product not found"})
                        continue

                    # Lock stock level for this product
                    stock_level, _ = StockLevel.objects.select_for_update().get_or_create(
                        product=product,
                        defaults={"tenant_id": tenant_id, "available": 0},
                    )

                    # Process all events for this product
                    for event in product_event_list:
                        delta = int(event.get("delta", 0))
                        raw_time = event.get("event_time")
                        dt = parse_datetime(str(raw_time)) if raw_time else None
                        event_time = dt or now()
                        source = event.get("source") or "system"
                        meta = event.get("meta") or {}

                        if delta == 0:
                            continue  # Skip zero-delta events

                        new_level = max(0, int(stock_level.available) + int(delta))
                        stock_level.available = new_level
                        stock_level.save(update_fields=["available"])

                        StockEvent.objects.create(
                            tenant_id=tenant_id,
                            product=product,
                            delta=delta,
                            resulting_level=new_level,
                            event_time=event_time,
                            source=source,
                            meta=meta,
                        )

                        total_events_processed += 1

                    applied.append({
                        "product_id": str(product_id), 
                        "events_processed": len(product_event_list),
                        "final_level": stock_level.available
                    })

            except Exception as e:
                logger.error(f"Failed to process product {product_id}: {str(e)}")
                conflicts.append({"product_id": product_id, "reason": f"processing error: {str(e)}"})

        if conflicts:
            return Response({
                "status": "partial_success", 
                "conflicts": conflicts,
                "applied": applied,
                "total_events_processed": total_events_processed
            }, status=status.HTTP_200_OK)

        return Response({
            "status": "success", 
            "applied": applied,
            "total_events_processed": total_events_processed
        }, status=status.HTTP_200_OK)