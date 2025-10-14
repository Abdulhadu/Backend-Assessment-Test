import uuid
from django.db import models


class Customer(models.Model):
    """
    Customer model representing end users who place orders.
    Each customer belongs to a tenant and has searchable metadata.
    """
    customer_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey(
        'tenants.Tenant', 
        on_delete=models.CASCADE, 
        db_column='tenant_id',
        related_name='customers'
    )
    name = models.TextField(max_length=255, help_text="Customer name")
    email = models.EmailField(max_length=255, help_text="Customer email address")
    metadata = models.JSONField(default=dict, blank=True, help_text="Additional customer metadata")
    search_vector = models.TextField(null=True, blank=True, help_text="Full-text search vector (simplified for SQLite)")
    created_at = models.DateTimeField(auto_now_add=True, help_text="Customer creation timestamp")
    
    class Meta:
        db_table = 'customers'
        verbose_name = 'Customer'
        verbose_name_plural = 'Customers'
        indexes = [
            models.Index(fields=['search_vector'], name='customers_search_vector_idx'),
            models.Index(fields=['tenant', 'email'], name='customers_tenant_email_idx'),
            models.Index(fields=['tenant', 'name'], name='customers_tenant_name_idx'),
            models.Index(fields=['created_at'], name='customers_created_at_idx'),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=['tenant', 'email'], 
                name='unique_tenant_customer_email'
            ),
        ]
    
    def __str__(self):
        return f"{self.name} ({self.email})"
