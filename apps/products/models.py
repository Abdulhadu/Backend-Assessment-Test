import uuid
from django.db import models
from django.core.validators import MinValueValidator
from decimal import Decimal


class Product(models.Model):
    """
    Product model representing items that can be ordered.
    Each product belongs to a tenant and has pricing information.
    """
    product_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey(
        'tenants.Tenant', 
        on_delete=models.CASCADE, 
        db_column='tenant_id',
        related_name='products'
    )
    sku = models.TextField(max_length=100, help_text="Stock Keeping Unit identifier")
    name = models.TextField(max_length=255, help_text="Product name")
    price = models.DecimalField(
        max_digits=10, 
        decimal_places=2, 
        validators=[MinValueValidator(Decimal('0.01'))],
        help_text="Current product price"
    )
    category_id = models.UUIDField(null=True, blank=True, help_text="Product category reference")
    active = models.BooleanField(default=True, help_text="Whether product is active")
    created_at = models.DateTimeField(auto_now_add=True, help_text="Product creation timestamp")
    
    class Meta:
        db_table = 'products'
        verbose_name = 'Product'
        verbose_name_plural = 'Products'
        indexes = [
            models.Index(fields=['tenant', 'product_id'], name='products_tenant_product_idx'),
            models.Index(fields=['tenant', 'sku'], name='products_tenant_sku_idx'),
            models.Index(fields=['tenant', 'active'], name='products_tenant_active_idx'),
            models.Index(fields=['category_id'], name='products_category_idx'),
            models.Index(fields=['created_at'], name='products_created_at_idx'),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=['tenant', 'sku'], 
                name='unique_tenant_sku'
            ),
        ]
    
    def __str__(self):
        return f"{self.name} ({self.sku})"
