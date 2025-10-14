import base64
import json
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

from django.db.models import Q
from django.http import StreamingHttpResponse
from django.utils.dateparse import parse_datetime
from rest_framework.views import APIView
from rest_framework.permissions import AllowAny
from rest_framework.request import Request
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiResponse, OpenApiExample
from drf_spectacular.types import OpenApiTypes

from .models import Order, OrderItem


def _b64encode(data: Dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(data, separators=(",", ":")).encode()).decode()


def _b64decode(token: str) -> Dict:
    return json.loads(base64.urlsafe_b64decode(token.encode()).decode())


def _parse_uuid(value: str):
    import uuid
    return uuid.UUID(str(value))


class OrdersStreamingSearchAPIView(APIView):
    permission_classes = [AllowAny]

    @extend_schema(
        tags=["orders"],
        summary="Search orders (cursor + projection, streaming)",
        description=(
            "High-throughput search with cursor-based pagination, complex filters, "
            "and column projection. Streams results as NDJSON to minimize memory usage."
        ),
        parameters=[
            OpenApiParameter("tenant_id", OpenApiTypes.STR, OpenApiParameter.PATH, required=True),
            OpenApiParameter("cursor", OpenApiTypes.STR, OpenApiParameter.QUERY, required=False,
                             description="Opaque continuation token returned by previous page"),
            OpenApiParameter("limit", OpenApiTypes.INT, OpenApiParameter.QUERY, required=False,
                             description="Max rows to return (<= 100000). Default 10000"),
            OpenApiParameter("fields", OpenApiTypes.STR, OpenApiParameter.QUERY, required=False,
                             description="Comma-separated list of fields to include (projection)"),
            OpenApiParameter("q", OpenApiTypes.STR, OpenApiParameter.QUERY, required=False,
                             description="Search text (matches customer name/email)"),
            OpenApiParameter("order_status", OpenApiTypes.STR, OpenApiParameter.QUERY, required=False,
                             description="Filter by order_status (exact; comma-separated for multiple)"),
            OpenApiParameter("product_ids", OpenApiTypes.STR, OpenApiParameter.QUERY, required=False,
                             description="Comma-separated product UUIDs to filter orders having those products"),
            OpenApiParameter("min_price", OpenApiTypes.NUMBER, OpenApiParameter.QUERY, required=False),
            OpenApiParameter("max_price", OpenApiTypes.NUMBER, OpenApiParameter.QUERY, required=False),
            OpenApiParameter("start_date", OpenApiTypes.DATETIME, OpenApiParameter.QUERY, required=False),
            OpenApiParameter("end_date", OpenApiTypes.DATETIME, OpenApiParameter.QUERY, required=False),
        ],
        responses={
            200: OpenApiResponse(
                response=OpenApiTypes.STR,
                description="NDJSON stream of orders. Each line is a JSON object.",
                examples=[OpenApiExample(
                    "NDJSON",
                    value=(
                        "{\"order_id\": \"...\", \"external_order_id\": \"...\"}\n"
                        "{\"order_id\": \"...\", \"external_order_id\": \"...\"}\n"
                    ),
                )],
            ),
        },
    )
    def get(self, request: Request, tenant_id: str):
        limit = min(int(request.query_params.get("limit") or 10000), 100000)
        fields_param = request.query_params.get("fields")
        requested_fields: Optional[List[str]] = [f.strip() for f in fields_param.split(",") if f.strip()] if fields_param else None

        # Always include sort keys for cursor
        mandatory_fields = {"order_id", "order_date"}
        if requested_fields:
            projection = list(set(requested_fields).union(mandatory_fields))
        else:
            # default projection
            projection = [
                "order_id",
                "external_order_id",
                "customer",
                "customer_name_snapshot",
                "customer_email_snapshot",
                "total_amount",
                "currency",
                "order_status",
                "order_date",
            ]

        base_qs = Order.objects.filter(tenant_id=tenant_id)

        # Text search on customer name/email (simulate FTS)
        q_text = request.query_params.get("q")
        if q_text:
            base_qs = base_qs.filter(
                Q(customer_name_snapshot__icontains=q_text) | Q(customer_email_snapshot__icontains=q_text)
            )

        # order_status filter (supports comma-separated)
        status_param = request.query_params.get("order_status")
        if status_param:
            statuses = [s.strip() for s in status_param.split(",") if s.strip()]
            base_qs = base_qs.filter(order_status__in=statuses)

        # price range
        min_price = request.query_params.get("min_price")
        max_price = request.query_params.get("max_price")
        if min_price is not None:
            base_qs = base_qs.filter(total_amount__gte=min_price)
        if max_price is not None:
            base_qs = base_qs.filter(total_amount__lte=max_price)

        # date range
        start_date = request.query_params.get("start_date")
        end_date = request.query_params.get("end_date")
        if start_date:
            dt = parse_datetime(start_date)
            if dt:
                base_qs = base_qs.filter(order_date__gte=dt)
        if end_date:
            dt = parse_datetime(end_date)
            if dt:
                base_qs = base_qs.filter(order_date__lte=dt)

        # Filter by product_ids (orders having these products)
        product_ids_param = request.query_params.get("product_ids")
        if product_ids_param:
            try:
                product_ids = [_parse_uuid(pid.strip()) for pid in product_ids_param.split(",") if pid.strip()]
                order_ids = (
                    OrderItem.objects.filter(tenant_id=tenant_id, product__in=product_ids)
                    .values_list("order", flat=True)
                    .distinct()
                )
                base_qs = base_qs.filter(order_id__in=order_ids)
            except Exception:
                # If product UUIDs invalid, return empty stream
                return StreamingHttpResponse((chunk for chunk in ()), content_type="application/x-ndjson")

        # Sorting: newest first (order_date desc, order_id desc)
        base_qs = base_qs.order_by("-order_date", "-order_id")

        # Apply cursor
        cursor_token = request.query_params.get("cursor")
        if cursor_token:
            try:
                cursor_data = _b64decode(cursor_token)
                last_date_str = cursor_data.get("order_date")
                last_id = _parse_uuid(cursor_data.get("order_id"))
                last_date = parse_datetime(last_date_str)
                if last_date and last_id:
                    base_qs = base_qs.filter(
                        Q(order_date__lt=last_date) | (Q(order_date=last_date) & Q(order_id__lt=last_id))
                    )
            except Exception:
                # Invalid cursor -> treat as start
                pass

        # Projection and iterator for low memory
        qs = base_qs.values(*projection).iterator(chunk_size=2000)

        def stream_rows() -> Iterable[bytes]:
            count = 0
            last_row: Optional[Dict] = None
            for row in qs:
                count += 1
                last_row = row
                # If client provided projection, send exactly that subset
                if requested_fields:
                    payload = {k: row.get(k) for k in requested_fields}
                else:
                    payload = row
                yield (json.dumps(payload, default=str) + "\n").encode()
                if count >= limit:
                    break

            # Trailer with next cursor (commented JSON line so parsers ignoring comments can skip)
            if last_row and count >= limit:
                next_cursor = _b64encode({
                    "order_date": (last_row.get("order_date") or "").isoformat() if isinstance(last_row.get("order_date"), datetime) else str(last_row.get("order_date")),
                    "order_id": str(last_row.get("order_id")),
                })
                meta = {"next_cursor": next_cursor}
                yield (json.dumps({"_meta": meta}) + "\n").encode()

        return StreamingHttpResponse(stream_rows(), content_type="application/x-ndjson")

from django.shortcuts import render

# Create your views here.
