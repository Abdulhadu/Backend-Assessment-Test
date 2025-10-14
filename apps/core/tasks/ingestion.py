"""
Comprehensive Celery tasks for processing all data types with proper dependencies.
"""
from celery import shared_task
from django.utils import timezone
from datetime import timedelta
import logging
import json
import os
from pathlib import Path

logger = logging.getLogger(__name__)


def determine_data_type(file_path):
    """Determine data type from file path."""
    filename = os.path.basename(file_path)
    
    if filename.startswith('customers_'):
        return 'customers'
    elif filename.startswith('products_'):
        return 'products'
    elif filename.startswith('orders_') and not filename.startswith('order_items_'):
        return 'orders'
    elif filename.startswith('order_items_'):
        return 'order_items'
    else:
        return None


@shared_task(bind=True, time_limit=600, soft_time_limit=540)
def process_comprehensive_ingestion(self, upload_id):
    """
    Process comprehensive data ingestion with proper dependency handling.
    Processes: customers -> products -> orders -> order_items
    """
    from apps.ingestions.models import IngestUpload, OrderIngestChunk
    from apps.ingestions.comprehensive_data_processor import ComprehensiveDataProcessor, ComprehensiveStagingTableManager
    from django.core.files.storage import default_storage
    import time
    
    try:
        upload = IngestUpload.objects.get(upload_id=upload_id)
        upload.status = 'processing'
        upload.save()
        
        # Initialize comprehensive data processor
        processor = ComprehensiveDataProcessor()
        staging_manager = ComprehensiveStagingTableManager()
        
        # Create staging tables for all data types
        staging_manager.create_staging_tables()
        
        # Process chunks in dependency order
        chunks = OrderIngestChunk.objects.filter(
            upload=upload, 
            status__in=['pending', 'failed']
        ).order_by('chunk_index')
        
        total_rows_received = 0
        total_rows_inserted = 0
        total_rows_failed = 0
        all_errors = []
        
        # Process each chunk
        for chunk in chunks:
            # Check if we're approaching the soft time limit
            if self.request.get('time_remaining', 600) < 30:
                logger.warning(f"Approaching time limit, stopping processing for upload {upload_id}")
                break
                
            try:
                chunk.status = 'processing'
                chunk.save()
                
                # Determine data type from file path
                file_path = chunk.file_path
                data_type = determine_data_type(file_path)
                
                if not data_type:
                    logger.warning(f"Could not determine data type for file: {file_path}")
                    chunk.status = 'failed'
                    chunk.errors_sample = [{'error': 'Unknown data type'}]
                    chunk.save()
                    continue
                
                # Process the file
                result = processor.process_file(
                    file_path=file_path,
                    content_type=chunk.content_type or 'application/x-ndjson',
                    tenant_id=str(upload.tenant.tenant_id),
                    chunk_id=str(chunk.chunk_id),
                    data_type=data_type
                )
                
                # Update chunk with results
                chunk.rows = result['rows_received']
                chunk.errors_sample = result['errors'][:5]  # Store sample errors
                chunk.status = 'completed' if result['rows_failed'] == 0 else 'completed_with_errors'
                chunk.save()
                
                # Promote staging data to production
                promote_result = processor.promote_staging_data(
                    tenant_id=str(upload.tenant.tenant_id),
                    chunk_id=str(chunk.chunk_id),
                    data_type=data_type
                )

                try:
                    if file_path and default_storage.exists(file_path):
                        default_storage.delete(file_path)
                        logger.info(f"Successfully deleted processed file: {file_path}")
                except Exception as e:
                    # Log an error but don't fail the task if deletion fails.
                    logger.error(f"Could not delete file {file_path}. Reason: {e}")
                
                total_rows_received += result['rows_received']
                total_rows_inserted += result['rows_inserted']
                total_rows_failed += result['rows_failed']
                all_errors.extend(result['errors'])
                
                logger.info(f"Processed chunk {chunk.chunk_index} ({data_type}): "
                          f"{result['rows_received']} rows, "
                          f"{result['rows_inserted']} inserted, "
                          f"{result['rows_failed']} failed")
                
            except Exception as e:
                chunk.status = 'failed'
                # Ensure error message is JSON serializable
                error_msg = str(e)
                try:
                    # Try to serialize to ensure it's safe
                    json.dumps(error_msg)
                except (TypeError, ValueError):
                    # If it contains non-serializable objects, convert to string
                    error_msg = repr(e)
                
                chunk.errors_sample = [{'error': error_msg}]
                chunk.save()
                
                logger.error(f"Failed to process chunk {chunk.chunk_id}: {error_msg}")
                all_errors.append({'chunk_id': str(chunk.chunk_id), 'error': error_msg})
        
        # Update upload status
        failed_chunks = chunks.filter(status='failed').count()
        if failed_chunks == 0:
            upload.status = 'completed'
        elif failed_chunks == chunks.count():
            upload.status = 'failed'
        else:
            upload.status = 'completed_with_errors'
        
        upload.save()
        
        # Cleanup staging tables
        staging_manager.cleanup_staging_tables()
        
        logger.info(f"Comprehensive ingestion completed for upload {upload_id}: "
                   f"{total_rows_received} total rows, "
                   f"{total_rows_inserted} inserted, "
                   f"{total_rows_failed} failed")
        
        return {
            'upload_id': str(upload_id),
            'rows_received': total_rows_received,
            'rows_inserted': total_rows_inserted,
            'rows_failed': total_rows_failed,
            'status': upload.status,
            'errors_count': len(all_errors)
        }
        
    except IngestUpload.DoesNotExist:
        logger.error(f"Upload {upload_id} not found")
        raise
    except Exception as e:
        logger.error(f"Comprehensive ingestion failed for upload {upload_id}: {str(e)}")
        raise


@shared_task(bind=True, time_limit=300, soft_time_limit=240)
def process_order_ingestion(self, upload_id):
    """
    Legacy order ingestion task - now redirects to comprehensive processing.
    """
    logger.info(f"Redirecting legacy order ingestion to comprehensive processing for upload {upload_id}")
    return process_comprehensive_ingestion.delay(upload_id)


