"""
Bulk ingestion API views for handling large-scale data uploads.
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
from django.http import JsonResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView
import gzip
import csv
import io

from apps.analytics.models import IdempotencyKey
from apps.ingestions.models import IngestUpload, OrderIngestChunk
from apps.tenants.models import Tenant
from apps.core.tasks.ingestion import process_order_ingestion

logger = logging.getLogger(__name__)


class BulkIngestionAPIView(APIView):
    """
    Handles bulk ingestion of order data with chunked uploads,
    idempotency, and resumable upload tokens.
    """
    
    def post(self, request):
        """
        Handle bulk order ingestion.
        
        Headers:
        - X-API-Key: Tenant API key
        - Idempotency-Key: Unique key for idempotent requests
        - Upload-Token: Token for resumable uploads (optional)
        - Content-Type: application/x-ndjson, text/csv, or application/octet-stream
        
        Body: File upload with order data
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
            
            # Process the upload
            result = self._process_upload(
                tenant=tenant,
                file=uploaded_file,
                content_type=content_type,
                idempotency_key=idempotency_key,
                upload_token=upload_token
            )
            
            processing_time = time.time() - start_time
            result['processing_time'] = round(processing_time, 3)
            
            return Response(result, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            logger.error(f"Bulk ingestion error: {str(e)}", exc_info=True)
            return Response(
                {'error': 'Internal server error', 'details': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def _validate_tenant(self, api_key: str) -> Optional[Tenant]:
        """Validate tenant API key."""
        try:
            api_key_hash = hashlib.sha256(api_key.encode()).hexdigest()
            return Tenant.objects.get(api_key_hash=api_key_hash)
        except Tenant.DoesNotExist:
            return None
    
    def _process_upload(self, tenant: Tenant, file, content_type: str, 
                       idempotency_key: str, upload_token: str = None) -> Dict:
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

        logger.error(f"Chunk created: {chunk}")
        
        # Queue background processing
        process_order_ingestion.delay(str(upload_session.upload_id))
        
        return {
            'upload_id': str(upload_session.upload_id),
            'chunk_id': str(chunk.chunk_id),
            'upload_token': upload_session.upload_token,
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
                
            except Exception as e:
                if "UNIQUE constraint failed" in str(e) and attempt < max_retries - 1:
                    logger.warning("Chunk creation failed due to race condition, retrying (attempt %s/%s)", 
                                 attempt + 1, max_retries)
                    time.sleep(0.1 * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    raise


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
        
        # Queue background processing
        # schedule the celery task only after the DB transaction commits
        transaction.on_commit(lambda: process_order_ingestion.delay(str(upload_session.upload_id)))

        
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