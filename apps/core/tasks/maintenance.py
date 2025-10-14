from celery import shared_task
from django.utils import timezone
from datetime import timedelta
import logging
import json
import os
from pathlib import Path

logger = logging.getLogger(__name__)


@shared_task
def cleanup_expired_data():
    """
    Clean up expired data to maintain database performance.
    """
    from apps.core.sql_utils import DatabaseMaintenance
    
    try:
        result = DatabaseMaintenance.cleanup_expired_data(days=90)
        logger.info(f"Cleanup completed: {result}")
        return result
    except Exception as e:
        logger.error(f"Cleanup failed: {str(e)}")
        raise


