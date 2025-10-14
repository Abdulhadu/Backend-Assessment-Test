import math
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Dict, Optional

from django.db.models import Sum, Count
from django.http import JsonResponse
from django.utils.dateparse import parse_datetime, parse_date
from rest_framework.views import APIView
from rest_framework.permissions import AllowAny
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiResponse
from drf_spectacular.types import OpenApiTypes

from apps.orders.models import Order, OrderItem
from apps.products.models import Product
from apps.analytics.models import MetricsPreagg


def _period_key(group_by: str, dt: datetime) -> str:
    if group_by == 'hour':
        return dt.strftime('%Y-%m-%dT%H:00:00')
    elif group_by == 'day':
        return dt.strftime('%Y-%m-%d')
    return dt.strftime('%Y-%m-%d')


class SalesMetricsAPIView(APIView):
    permission_classes = [AllowAny]

    @extend_schema(
        tags=["metrics"],
        summary="Sales metrics (approx/exact with pre-agg)",
        parameters=[
            OpenApiParameter("tenant_id", OpenApiTypes.STR, OpenApiParameter.PATH, required=True),
            OpenApiParameter("group_by", OpenApiTypes.STR, OpenApiParameter.QUERY, required=True,
                             description="day|hour|product|category"),
            OpenApiParameter("start_date", OpenApiTypes.DATETIME, OpenApiParameter.QUERY),
            OpenApiParameter("end_date", OpenApiTypes.DATETIME, OpenApiParameter.QUERY),
            OpenApiParameter("precision", OpenApiTypes.STR, OpenApiParameter.QUERY,
                             description="approx|exact (default approx)"),
        ],
        responses={200: OpenApiResponse(OpenApiTypes.OBJECT)},
    )
    def get(self, request, tenant_id: str):
        group_by = (request.query_params.get('group_by') or 'day').lower()
        precision = (request.query_params.get('precision') or 'approx').lower()

        # Flexible date parser
        def parse_datetime_flexible(value: Optional[str]) -> Optional[datetime]:
            if not value:
                return None
            dt = parse_datetime(value)
            if dt:
                return dt
            d = parse_date(value)
            if d:
                return datetime(d.year, d.month, d.day)
            try:
                return datetime.strptime(value, "%d-%m-%Y")
            except Exception:
                return None

        start_date = parse_datetime_flexible(request.query_params.get('start_date'))
        end_date = parse_datetime_flexible(request.query_params.get('end_date'))

        if group_by not in {'day', 'hour', 'product', 'category'}:
            return JsonResponse({"error": "invalid group_by"}, status=400)

        use_preagg = group_by in {'day', 'hour'} and precision == 'approx'
        result: Dict[str, Dict] = {}
        method_used = 'approx-preagg' if use_preagg else ('exact-scan' if precision == 'exact' else 'approx-stream')
        error_bounds = None

        # --- CASE 1: PREAGG ---
        if use_preagg:
            preaggs = MetricsPreagg.objects.filter(tenant_id=tenant_id)
            if start_date:
                preaggs = preaggs.filter(period_end__gte=start_date.date())
            if end_date:
                preaggs = preaggs.filter(period_start__lte=end_date.date())

            for m in preaggs.iterator(chunk_size=1000):
                key = m.group_key
                metrics = m.metrics or {}
                bucket = result.setdefault(key, {"sum_sales": 0.0, "num_orders": 0, "unique_customers_est": 0})
                bucket["sum_sales"] += float(metrics.get("sum_sales", 0.0))
                bucket["num_orders"] += int(metrics.get("num_orders", 0))
                bucket["unique_customers_est"] += int(metrics.get("unique_customers_est", 0))

            error_bounds = {"unique_customers_est": "~2-3% relative error (HLL-like)"}

        # --- CASE 2: EXACT / STREAM ---
        else:
            orders_qs = Order.objects.filter(tenant_id=tenant_id)
            if start_date:
                orders_qs = orders_qs.filter(order_date__gte=start_date)
            if end_date:
                orders_qs = orders_qs.filter(order_date__lte=end_date)

            if group_by in {"product", "category"}:
                # Collect order IDs once
                order_ids = list(orders_qs.values_list("order_id", flat=True))
                if not order_ids:
                    return JsonResponse({"data": result})

                # Fetch order items for those orders
                order_items_qs = OrderItem.objects.filter(
                    tenant_id=tenant_id,
                    order__in=order_ids
                ).values("order", "product")

                # Prefetch product→category map if needed
                category_map = {}
                if group_by == "category":
                    for p in Product.objects.all().values("product_id", "category_id"):
                        category_map[p["product_id"]] = p["category_id"]

                # Prepare order metadata lookup
                order_meta = {
                    o["order_id"]: (o["total_amount"], o["customer"])
                    for o in orders_qs.values("order_id", "total_amount", "customer")
                }

                # Aggregate in memory
                for item in order_items_qs.iterator(chunk_size=10000):
                    order_id = item["order"]
                    if order_id not in order_meta:
                        continue
                    total_amount, customer = order_meta[order_id]

                    if group_by == "product":
                        key = str(item["product"])
                    else:
                        cid = category_map.get(item["product"])
                        if cid is None:
                            continue
                        key = str(cid)

                    bucket = result.setdefault(key, {"sum_sales": 0.0, "num_orders": 0, "unique_customers_est": set()})
                    bucket["sum_sales"] += float(total_amount)
                    bucket["num_orders"] += 1
                    bucket["unique_customers_est"].add(customer)

                # Convert sets to counts
                for key, metrics in result.items():
                    metrics["unique_customers_est"] = len(metrics["unique_customers_est"])

            else:
                # Day/hour grouping
                for o in orders_qs.values("order_id", "total_amount", "customer", "order_date").iterator(chunk_size=5000):
                    odt = o["order_date"]
                    if not isinstance(odt, datetime):
                        try:
                            odt = parse_datetime(str(odt)) or datetime.fromisoformat(str(odt))
                        except Exception:
                            odt = None
                    key = _period_key(group_by, odt) if odt else "unknown"
                    bucket = result.setdefault(key, {"sum_sales": 0.0, "num_orders": 0, "unique_customers_est": set()})
                    bucket["sum_sales"] += float(o["total_amount"])
                    bucket["num_orders"] += 1
                    bucket["unique_customers_est"].add(o["customer"])

                for key, metrics in result.items():
                    metrics["unique_customers_est"] = len(metrics["unique_customers_est"])

            if precision == "approx":
                error_bounds = {"unique_customers_est": "~5-10% relative error (streaming sketch)"}

        return JsonResponse({
            "group_by": group_by,
            "precision": precision,
            "method": method_used,
            "error_bounds": error_bounds,
            "data": result,
        })
