import uuid
from typing import Any, Dict
import os

from django.http import JsonResponse, HttpResponse
from django.utils.timezone import now
from django.db import transaction
from rest_framework.permissions import AllowAny
from rest_framework.views import APIView
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiResponse
from drf_spectacular.types import OpenApiTypes

from apps.analytics.models import ExportJob, ExportChunk
from apps.tenants.models import Tenant
from apps.orders.models import Order
from apps.core.export_utils import get_export_file_path, write_csv_gzip_incremental, write_parquet_file
from apps.core.auth import authenticate_tenant


class ExportAPIView(APIView):
    permission_classes = [AllowAny]

    @extend_schema(
        tags=["export"],
        summary="Create an export job",
        parameters=[
            OpenApiParameter("tenant_id", OpenApiTypes.STR, OpenApiParameter.PATH, required=True),
            OpenApiParameter("X-API-Key", OpenApiTypes.STR, OpenApiParameter.HEADER, required=True),
        ],
        request={
            "application/json": OpenApiTypes.OBJECT,
            "example": {
                "format": "csv",
                "filters": {"status": "completed"}
            }
        },
        responses={201: OpenApiResponse(OpenApiTypes.OBJECT)},
    )
    def post(self, request, tenant_id: str):
        """Create a new export job for the specified tenant."""
        fmt = (request.data.get("format") or "csv").lower()
        filters: Dict[str, Any] = request.data.get("filters") or {}

        tenant = authenticate_tenant(request, tenant_id)
        if isinstance(tenant, JsonResponse):  # if authentication failed, tenant is actually a JsonResponse
            return tenant

        with transaction.atomic():
            job = ExportJob.objects.create(
                tenant=tenant,
                format=fmt,
                filters=filters,
                status="processing",
                manifest={"requested_at": now().isoformat(), "rows": 0},
            )

        # Build streaming export synchronously (CSV path); Parquet placeholder
        file_path = get_export_file_path(str(tenant.tenant_id), str(job.export_id), fmt)
        header = ("order_id", "customer", "total_amount", "order_date")

        def row_iter():
            qs = Order.objects.filter(tenant=tenant).values("order_id", "customer", "total_amount", "order_date").iterator(chunk_size=5000)
            for o in qs:
                yield {
                    "order_id": str(o["order_id"]),
                    "customer": str(o["customer"]),
                    "total_amount": str(o["total_amount"]),
                    "order_date": o["order_date"].isoformat() if o["order_date"] else "",
                }

        if fmt == "csv":
            bytes_delta, rows_delta = write_csv_gzip_incremental(file_path, row_iter(), header=header, resume=True)
        elif fmt == "parquet":
            bytes_delta, rows_delta = write_parquet_file(file_path, row_iter())
        else:
            return JsonResponse({"error": "unsupported format"}, status=400)

        with transaction.atomic():
            chunk = ExportChunk.objects.create(
                export_job=job,
                chunk_index=0,
                storage_path=file_path,
                bytes=bytes_delta,
                status="completed",
            )

            job.status = "completed"
            job.manifest.update({"chunks": 1, "rows": rows_delta, "path": file_path})
            job.save(update_fields=["status", "manifest"])

        return JsonResponse(
            {
                "export_id": str(job.export_id),
                "status": job.status,
                "format": job.format,
                "chunks": [
                    {
                        "chunk_id": str(chunk.chunk_id),
                        "chunk_index": chunk.chunk_index,
                        "status": chunk.status,
                    }
                ],
            },
            status=201,
        )

    @extend_schema(
        tags=["export"],
        summary="Get export job status",
        parameters=[
            OpenApiParameter("tenant_id", OpenApiTypes.STR, OpenApiParameter.PATH, required=True),
            OpenApiParameter("export_id", OpenApiTypes.STR, OpenApiParameter.PATH, required=True),
            OpenApiParameter("X-API-Key", OpenApiTypes.STR, OpenApiParameter.HEADER, required=True),
        ],
        responses={200: OpenApiResponse(OpenApiTypes.OBJECT)},
    )
    def get(self, request, tenant_id: str, export_id: str):
        """Retrieve the status and details of an existing export job."""

        tenant = authenticate_tenant(request, tenant_id)
        if isinstance(tenant, JsonResponse):  # if authentication failed, tenant is actually a JsonResponse
            return tenant

        try:
            job = ExportJob.objects.get(export_id=export_id, tenant_id=tenant_id)
        except ExportJob.DoesNotExist:
            return JsonResponse({"error": "export not found"}, status=404)

        chunks = [
            {
                "chunk_id": str(c.chunk_id),
                "chunk_index": c.chunk_index,
                "status": c.status,
                "bytes": c.bytes,
            }
            for c in job.chunks.order_by("chunk_index")
        ]
        return JsonResponse(
            {
                "export_id": str(job.export_id),
                "status": job.status,
                "format": job.format,
                "manifest": job.manifest,
                "chunks": chunks,
            }
        )


class ExportDownloadAPIView(APIView):
    permission_classes = [AllowAny]

    @extend_schema(
        tags=["export"],
        summary="Download export file (supports Range and gzip)",
        parameters=[
            OpenApiParameter("tenant_id", OpenApiTypes.STR, OpenApiParameter.PATH, required=True),
            OpenApiParameter("export_id", OpenApiTypes.STR, OpenApiParameter.PATH, required=True),
            OpenApiParameter("X-API-Key", OpenApiTypes.STR, OpenApiParameter.HEADER, required=True),
        ],
        responses={200: OpenApiResponse(OpenApiTypes.BINARY)},
    )
    def get(self, request, tenant_id: str, export_id: str):
        """Download the export file with support for byte-range requests."""
       
        tenant = authenticate_tenant(request, tenant_id)
        if isinstance(tenant, JsonResponse):  # if authentication failed, tenant is actually a JsonResponse
            return tenant

        try:
            job = ExportJob.objects.get(export_id=export_id, tenant_id=tenant_id)
        except ExportJob.DoesNotExist:
            return JsonResponse({"error": "export not found"}, status=404)

        file_path = job.manifest.get("path")
        if not file_path:
            return JsonResponse({"error": "file not ready"}, status=404)

        try:
            file_size = os.path.getsize(file_path)
        except FileNotFoundError:
            return JsonResponse({"error": "file missing"}, status=404)

        range_header = request.headers.get("Range")
        start = 0
        end = file_size - 1
        status_code = 200
        if range_header and range_header.startswith("bytes="):
            try:
                val = range_header.split("=", 1)[1]
                s, e = val.split("-", 1)
                if s:
                    start = int(s)
                if e:
                    end = int(e)
                status_code = 206
            except Exception:
                pass

        length = max(0, end - start + 1)

        def file_iterator(path, start_pos, length, chunk_size=64 * 1024):
            with open(path, "rb") as f:
                f.seek(start_pos)
                remaining = length
                while remaining > 0:
                    data = f.read(min(chunk_size, remaining))
                    if not data:
                        break
                    remaining -= len(data)
                    yield data

        # Set content-type by extension
        if file_path.endswith('.csv.gz'):
            content_type = "application/gzip"
        elif file_path.endswith('.parquet'):
            content_type = "application/octet-stream"
        else:
            content_type = "application/octet-stream"

        resp = HttpResponse(file_iterator(file_path, start, length), content_type=content_type)
        resp["Content-Length"] = str(length)
        resp["Accept-Ranges"] = "bytes"
        filename = os.path.basename(file_path)
        resp["Content-Disposition"] = f"attachment; filename=\"{filename}\""
        if status_code == 206:
            resp.status_code = 206
            resp["Content-Range"] = f"bytes {start}-{end}/{file_size}"
        return resp
