"""
Celery tasks for the stocks app.
"""
from celery import shared_task
import logging

logger = logging.getLogger(__name__)


@shared_task
def update_stock_levels():
    """
    Update stock levels based on recent stock events.
    """
    from apps.stocks.models import StockEvent, StockLevel
    from apps.products.models import Product
    from django.utils import timezone
    from datetime import timedelta
    
    try:
        # Get products with recent stock events
        recent_time = timezone.now() - timedelta(hours=1)
        recent_events = StockEvent.objects.filter(event_time__gte=recent_time)
        
        updated_count = 0
        for event in recent_events:
            stock_level, created = StockLevel.objects.get_or_create(
                product=event.product,
                defaults={
                    'tenant': event.tenant,
                    'available': event.resulting_level or 0,
                }
            )
            
            if not created:
                stock_level.available = event.resulting_level or stock_level.available
                stock_level.save()
            
            updated_count += 1
        
        logger.info(f"Updated {updated_count} stock levels")
        return f"Updated {updated_count} stock levels"
    except Exception as e:
        logger.error(f"Stock level update failed: {str(e)}")
        raise


@shared_task
def process_stock_event(stock_event_id):
    """
    Process a single stock event and update related data.
    """
    from apps.stocks.models import StockEvent, StockLevel
    
    try:
        event = StockEvent.objects.get(stock_event_id=stock_event_id)
        
        # Update stock level
        stock_level, created = StockLevel.objects.get_or_create(
            product=event.product,
            defaults={
                'tenant': event.tenant,
                'available': event.resulting_level or 0,
            }
        )
        
        if not created:
            stock_level.available = event.resulting_level or stock_level.available
            stock_level.save()
        
        logger.info(f"Processed stock event {stock_event_id}")
        return f"Stock event {stock_event_id} processed"
    except StockEvent.DoesNotExist:
        logger.error(f"Stock event {stock_event_id} not found")
        raise
    except Exception as e:
        logger.error(f"Failed to process stock event {stock_event_id}: {str(e)}")
        raise
