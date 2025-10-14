"""
Comprehensive bulk ingestion API views for handling all data types.
"""
import hashlib
import json
import logging
import tempfile
import time
import uuid
from typing import Dict, List, Optional, Tuple
from django.conf import settings
from django.core.files.storage import default_storage
from django.db import transaction
from django.db.utils import IntegrityError
from django.http import JsonResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.authentication import BasicAuthentication, SessionAuthentication
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser, FormParser
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiResponse, OpenApiExample
from drf_spectacular.types import OpenApiTypes
import gzip
import csv
import io
import os

from apps.analytics.models import IdempotencyKey
from apps.ingestions.models import IngestUpload, OrderIngestChunk
from apps.tenants.models import Tenant
from apps.core.tasks.ingestion import process_comprehensive_ingestion

logger = logging.getLogger(__name__)


class ComprehensiveBulkIngestionAPIView(APIView):
    """
    Handles comprehensive bulk ingestion of all data types with proper dependency handling.
    """
    
    parser_classes = [MultiPartParser, FormParser]
    # Disable default session auth to avoid CSRF for Swagger/file uploads; we authenticate via header
    authentication_classes = []
    permission_classes = [AllowAny]

    @extend_schema(
        tags=["ingest"],
        summary="Comprehensive bulk ingestion",
        description="Upload customers_, products_, orders_, or order_items_ NDJSON/CSV files. Processes in dependency order.",
        request={
            'multipart/form-data': {
                'type': 'object',
                'properties': {
                    'file': {'type': 'string', 'format': 'binary'}
                },
                'required': ['file']
            }
        },
        parameters=[
            OpenApiParameter(name='X-API-Key', location=OpenApiParameter.HEADER, required=True, type=str),
            OpenApiParameter(name='Idempotency-Key', location=OpenApiParameter.HEADER, required=True, type=str),
            OpenApiParameter(name='Upload-Token', location=OpenApiParameter.HEADER, required=False, type=str),
        ],
        responses={
            201: OpenApiResponse(
                response=OpenApiTypes.OBJECT,
                description='Upload queued',
                examples=[OpenApiExample(
                    'QueuedResponse',
                    summary='Queued',
                    value={
                        'upload_id': '9c6f3a2e-9e8a-4c2c-9d1b-7bb3c2f8e1a1',
                        'chunk_id': '1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d',
                        'upload_token': 'e7c1f2b4-7d6a-4f1c-9b2a-3e4f5a6b7c8d',
                        'data_type': 'orders',
                        'rows_received': 0,
                        'rows_inserted': 0,
                        'rows_failed': 0,
                        'status': 'queued',
                        'idempotency_key': 'abc123'
                    }
                )]
            ),
            400: OpenApiResponse(response=OpenApiTypes.OBJECT, description='Bad request'),
            401: OpenApiResponse(response=OpenApiTypes.OBJECT, description='Unauthorized')
        }
    )
    def post(self, request):
        """
        Handle comprehensive bulk data ingestion.
        
        Headers:
        - X-API-Key: Tenant API key
        - Idempotency-Key: Unique key for idempotent requests
        - Upload-Token: Token for resumable uploads (optional)
        - Content-Type: application/x-ndjson, text/csv, or application/octet-stream
        
        Body: File upload with data (customers, products, orders, or order_items)
        """
        start_time = time.time()
        
        try:
            # Extract headers
            api_key = request.headers.get('X-API-Key')
            idempotency_key = request.headers.get('Idempotency-Key')
            upload_token = request.headers.get('Upload-Token')
            
            if not api_key:
                return Response(
                    {'error': 'X-API-Key header required'},
                    status=status.HTTP_401_UNAUTHORIZED
                )
            
            if not idempotency_key:
                return Response(
                    {'error': 'Idempotency-Key header required'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Validate tenant
            tenant = self._validate_tenant(api_key)
            if not tenant:
                return Response(
                    {'error': 'Invalid API key'},
                    status=status.HTTP_401_UNAUTHORIZED
                )
            
            # Handle file upload
            if 'file' not in request.FILES:
                return Response(
                    {'error': 'No file provided'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            uploaded_file = request.FILES['file']
            content_type = request.content_type
            
            # Determine data type from filename
            data_type = self._determine_data_type(uploaded_file.name)
            if not data_type:
                return Response(
                    {'error': 'Could not determine data type from filename. Expected: customers_, products_, orders_, or order_items_'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Process the upload
            result = self._process_upload(
                tenant=tenant,
                file=uploaded_file,
                content_type=content_type,
                idempotency_key=idempotency_key,
                upload_token=upload_token,
                data_type=data_type
            )
            
            processing_time = time.time() - start_time
            result['processing_time'] = round(processing_time, 3)
            result['data_type'] = data_type
            
            return Response(result, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            logger.error(f"Comprehensive bulk ingestion error: {str(e)}", exc_info=True)
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
    
    def _determine_data_type(self, filename: str) -> Optional[str]:
        """Determine data type from filename."""
        if filename.startswith('customers_'):
            return 'customers'
        elif filename.startswith('products_'):
            return 'products'
        elif filename.startswith('orders_'):
            return 'orders'
        elif filename.startswith('order_items_'):
            return 'order_items'
        else:
            return None
    
    def _process_upload(self, tenant: Tenant, file, content_type: str, 
                       idempotency_key: str, upload_token: str = None, data_type: str = None) -> Dict:
        """Process the uploaded file."""
        
        # Check idempotency
        idempotency_result = self._check_idempotency(tenant, idempotency_key)
        if idempotency_result:
            return idempotency_result
        
        # Create or get upload session
        upload_session = self._get_or_create_upload_session(
            tenant, upload_token, idempotency_key
        )
        
        # Save file to temporary storage
        file_path = self._save_uploaded_file(file, upload_session.upload_id)
        
        # Create chunk record
        chunk = self._create_chunk_record(
            upload_session, file_path, file.size, content_type
        )

        logger.info(f"Chunk created: Chunk {chunk.chunk_index} of Upload {upload_session.upload_id} ({data_type})")
        
        # Queue comprehensive background processing
        process_comprehensive_ingestion.delay(str(upload_session.upload_id))
        
        return {
            'upload_id': str(upload_session.upload_id),
            'chunk_id': str(chunk.chunk_id),
            'upload_token': upload_session.upload_token,
            'data_type': data_type,
            'rows_received': 0,  # Will be updated by background task
            'rows_inserted': 0,
            'rows_failed': 0,
            'status': 'queued',
            'idempotency_key': idempotency_key
        }
    
    def _check_idempotency(self, tenant: Tenant, idempotency_key: str) -> Optional[Dict]:
        """Check if request is idempotent."""
        try:
            idempotency_record = IdempotencyKey.objects.get(
                tenant=tenant,
                idempotency_key=idempotency_key
            )
            
            if idempotency_record.status == 'completed':
                return {
                    'upload_id': str(idempotency_record.response_summary.get('upload_id')),
                    'rows_received': idempotency_record.response_summary.get('rows_received', 0),
                    'rows_inserted': idempotency_record.response_summary.get('rows_inserted', 0),
                    'rows_failed': idempotency_record.response_summary.get('rows_failed', 0),
                    'status': 'completed',
                    'idempotency_key': idempotency_key,
                    'message': 'Request already processed'
                }
            
            return None
            
        except IdempotencyKey.DoesNotExist:
            return None
    
    def _get_or_create_upload_session(self, tenant: Tenant, upload_token: str, 
                                    idempotency_key: str) -> IngestUpload:
        """Get existing upload session or create new one."""
        if upload_token:
            try:
                return IngestUpload.objects.get(
                    tenant=tenant,
                    upload_token=upload_token
                )
            except IngestUpload.DoesNotExist:
                pass
        
        # Create new upload session
        upload_session = IngestUpload.objects.create(
            tenant=tenant,
            upload_token=str(uuid.uuid4()),
            status='pending',
            manifest={
                'idempotency_key': idempotency_key,
                'created_at': time.time()
            }
        )
        
        return upload_session
    
    def _save_uploaded_file(self, file, upload_id: uuid.UUID) -> str:
        """Save uploaded file to temporary storage."""
        # Create unique filename
        filename = f"uploads/{upload_id}/{file.name}"
        
        # Save file
        file_path = default_storage.save(filename, file)
        
        return file_path
    
    def _create_chunk_record(self, upload_session: IngestUpload, file_path: str, 
                            file_size: int, content_type: str) -> OrderIngestChunk:
        """Create chunk record for tracking."""
        
        # Calculate checksum
        file_content = default_storage.open(file_path).read()
        checksum = hashlib.sha256(file_content).hexdigest()
        
        # Get next chunk index using atomic operation to avoid race conditions
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with transaction.atomic():
                    # Use select_for_update to prevent race conditions
                    last_chunk = OrderIngestChunk.objects.filter(
                        upload=upload_session
                    ).select_for_update().order_by('-chunk_index').first()
                    
                    next_index = (last_chunk.chunk_index + 1) if last_chunk else 0
                    
                    chunk = OrderIngestChunk.objects.create(
                        upload=upload_session,
                        chunk_index=next_index,
                        file_path=file_path,
                        content_type=content_type,
                        checksum=checksum,
                        status='pending',
                        rows=0,  # Will be updated by background task
                        errors_sample=[]
                    )
                
                return chunk
                
            except IntegrityError as e:
                # Handle Postgres unique violation on (upload, chunk_index)
                if ("duplicate key value violates unique constraint" in str(e)) or ("unique_upload_chunk_index" in str(e)):
                    # If a chunk with the same checksum already exists for this upload, return it (idempotent)
                    existing = OrderIngestChunk.objects.filter(upload=upload_session, checksum=checksum).first()
                    if existing:
                        logger.info("Duplicate chunk detected for upload %s with same checksum; returning existing chunk %s", upload_session.upload_id, existing.chunk_id)
                        return existing
                    if attempt < max_retries - 1:
                        logger.warning("Chunk creation hit unique constraint, retrying (attempt %s/%s)", attempt + 1, max_retries)
                        time.sleep(0.1 * (attempt + 1))
                        continue
                # Re-raise if not a unique-constraint race or retries exhausted
                raise


# Legacy views for backward compatibility
@api_view(['POST'])
@permission_classes([AllowAny])
def create_upload_session(request):
    """
    Create a new upload session for resumable uploads.
    
    Headers:
    - X-API-Key: Tenant API key
    
    Body:
    - manifest: Optional metadata about the upload
    """
    try:
        api_key = request.headers.get('X-API-Key')
        if not api_key:
            return Response(
                {'error': 'X-API-Key header required'},
                status=status.HTTP_401_UNAUTHORIZED
            )
        
        # Validate tenant
        api_key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        try:
            tenant = Tenant.objects.get(api_key_hash=api_key_hash)
        except Tenant.DoesNotExist:
            return Response(
                {'error': 'Invalid API key'},
                status=status.HTTP_401_UNAUTHORIZED
            )
        
        # Create upload session
        upload_session = IngestUpload.objects.create(
            tenant=tenant,
            upload_token=str(uuid.uuid4()),
            status='pending',
            manifest=request.data.get('manifest', {})
        )
        
        return Response({
            'upload_token': upload_session.upload_token,
            'upload_id': str(upload_session.upload_id),
            'status': 'created'
        }, status=status.HTTP_201_CREATED)
        
    except Exception as e:
        logger.error(f"Create upload session error: {str(e)}", exc_info=True)
        return Response(
            {'error': 'Internal server error'},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
@permission_classes([AllowAny])
def get_upload_status(request, upload_token):
    """
    Get the status of an upload session.
    
    Headers:
    - X-API-Key: Tenant API key
    """
    try:
        api_key = request.headers.get('X-API-Key')
        if not api_key:
            return Response(
                {'error': 'X-API-Key header required'},
                status=status.HTTP_401_UNAUTHORIZED
            )
        
        # Validate tenant
        api_key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        try:
            tenant = Tenant.objects.get(api_key_hash=api_key_hash)
        except Tenant.DoesNotExist:
            return Response(
                {'error': 'Invalid API key'},
                status=status.HTTP_401_UNAUTHORIZED
            )
        
        # Get upload session
        try:
            upload_session = IngestUpload.objects.get(
                tenant=tenant,
                upload_token=upload_token
            )
        except IngestUpload.DoesNotExist:
            return Response(
                {'error': 'Upload session not found'},
                status=status.HTTP_404_NOT_FOUND
            )
        
        # Get chunk statistics
        chunks = OrderIngestChunk.objects.filter(upload=upload_session)
        total_rows = sum(chunk.rows for chunk in chunks)
        total_errors = sum(len(chunk.errors_sample) for chunk in chunks)
        
        return Response({
            'upload_id': str(upload_session.upload_id),
            'upload_token': upload_session.upload_token,
            'status': upload_session.status,
            'total_chunks': chunks.count(),
            'completed_chunks': chunks.filter(status='completed').count(),
            'failed_chunks': chunks.filter(status='failed').count(),
            'total_rows': total_rows,
            'total_errors': total_errors,
            'created_at': upload_session.created_at.isoformat(),
            'last_activity': upload_session.last_activity.isoformat()
        })
        
    except Exception as e:
        logger.error(f"Get upload status error: {str(e)}", exc_info=True)
        return Response(
            {'error': 'Internal server error'},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['POST'])
@permission_classes([AllowAny])
def resume_upload(request, upload_token):
    """
    Resume a failed upload by retrying failed chunks.
    
    Headers:
    - X-API-Key: Tenant API key
    
    Body:
    - chunk_indices: List of chunk indices to retry (optional)
    """
    try:
        api_key = request.headers.get('X-API-Key')
        if not api_key:
            return Response(
                {'error': 'X-API-Key header required'},
                status=status.HTTP_401_UNAUTHORIZED
            )
        
        # Validate tenant
        api_key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        try:
            tenant = Tenant.objects.get(api_key_hash=api_key_hash)
        except Tenant.DoesNotExist:
            return Response(
                {'error': 'Invalid API key'},
                status=status.HTTP_401_UNAUTHORIZED
            )
        
        # Get upload session
        try:
            upload_session = IngestUpload.objects.get(
                tenant=tenant,
                upload_token=upload_token
            )
        except IngestUpload.DoesNotExist:
            return Response(
                {'error': 'Upload session not found'},
                status=status.HTTP_404_NOT_FOUND
            )
        
        # Get chunks to retry
        chunk_indices = request.data.get('chunk_indices', [])
        if chunk_indices:
            chunks_to_retry = OrderIngestChunk.objects.filter(
                upload=upload_session,
                chunk_index__in=chunk_indices
            )
        else:
            chunks_to_retry = OrderIngestChunk.objects.filter(
                upload=upload_session,
                status='failed'
            )
        
        # Reset chunk status
        chunks_to_retry.update(status='pending')
        
        # Queue comprehensive background processing
        # schedule the celery task only after the DB transaction commits
        transaction.on_commit(lambda: process_comprehensive_ingestion.delay(str(upload_session.upload_id)))
        
        return Response({
            'upload_id': str(upload_session.upload_id),
            'upload_token': upload_session.upload_token,
            'chunks_queued': chunks_to_retry.count(),
            'status': 'queued'
        })
        
    except Exception as e:
        logger.error(f"Resume upload error: {str(e)}", exc_info=True)
        return Response(
            {'error': 'Internal server error'},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
