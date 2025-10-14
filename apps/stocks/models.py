import uuid
from django.db import models
from django.core.validators import MinValueValidator


class StockEvent(models.Model):
    """
    StockEvent model representing inventory changes.
    Each event tracks stock level changes for products.
    """
    SOURCE_CHOICES = [
        ('manual', 'Manual Adjustment'),
        ('order', 'Order Fulfillment'),
        ('receipt', 'Stock Receipt'),
        ('return', 'Return Processing'),
        ('adjustment', 'Inventory Adjustment'),
        ('system', 'System Generated'),
    ]
    
    stock_event_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey(
        'tenants.Tenant', 
        on_delete=models.CASCADE, 
        db_column='tenant_id',
        related_name='stock_events'
    )
    product = models.ForeignKey(
        'products.Product', 
        on_delete=models.CASCADE, 
        db_column='product_id',
        related_name='stock_events'
    )
    delta = models.IntegerField(help_text="Stock level change (positive for increase, negative for decrease)")
    resulting_level = models.PositiveIntegerField(
        null=True, 
        blank=True, 
        help_text="Stock level after this event"
    )
    event_time = models.DateTimeField(help_text="When the stock event occurred")
    source = models.CharField(max_length=50, choices=SOURCE_CHOICES, help_text="Source of the stock event")
    meta = models.JSONField(default=dict, blank=True, help_text="Additional event metadata")
    
    class Meta:
        db_table = 'stock_events'
        verbose_name = 'Stock Event'
        verbose_name_plural = 'Stock Events'
        indexes = [
            models.Index(fields=['product', 'event_time'], name='stock_events_product_time_idx'),
            models.Index(fields=['tenant', 'event_time'], name='stock_events_tenant_time_idx'),
            models.Index(fields=['source'], name='stock_events_source_idx'),
            models.Index(fields=['event_time'], name='stock_events_event_time_idx'),
        ]
    
    def __str__(self):
        return f"Stock Event: {self.product.name} ({self.delta:+d}) at {self.event_time}"


class StockLevel(models.Model):
    """
    StockLevel model representing current inventory levels.
    This is a denormalized view of current stock for fast queries.
    """
    product = models.OneToOneField(
        'products.Product', 
        on_delete=models.CASCADE, 
        primary_key=True,
        db_column='product_id',
        related_name='stock_level'
    )
    tenant = models.ForeignKey(
        'tenants.Tenant', 
        on_delete=models.CASCADE, 
        db_column='tenant_id',
        related_name='stock_levels'
    )
    available = models.PositiveIntegerField(
        default=0, 
        validators=[MinValueValidator(0)],
        help_text="Current available stock level"
    )
    last_updated = models.DateTimeField(auto_now=True, help_text="Last stock level update")
    
    class Meta:
        db_table = 'stock_levels'
        verbose_name = 'Stock Level'
        verbose_name_plural = 'Stock Levels'
        indexes = [
            models.Index(fields=['tenant'], name='stock_levels_tenant_idx'),
            models.Index(fields=['available'], name='stock_levels_available_idx'),
            models.Index(fields=['last_updated'], name='stock_levels_last_updated_idx'),
        ]
    
    def __str__(self):
        return f"{self.product.name}: {self.available} units"
