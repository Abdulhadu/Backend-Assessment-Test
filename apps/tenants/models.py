import uuid
from django.db import models
from django.utils import timezone
from decimal import Decimal


class Tenant(models.Model):
    """
    Tenant model representing multi-tenant architecture.
    Each tenant owns their own data and has isolated access.
    """
    tenant_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.TextField(max_length=255, help_text="Tenant name")
    api_key_hash = models.TextField(max_length=255, help_text="Hashed API key for authentication")
    created_at = models.DateTimeField(auto_now_add=True, help_text="Tenant creation timestamp")
    
    class Meta:
        db_table = 'tenants'
        verbose_name = 'Tenant'
        verbose_name_plural = 'Tenants'
        indexes = [
            models.Index(fields=['created_at'], name='tenants_created_at_idx'),
            models.Index(fields=['name'], name='tenants_name_idx'),
        ]
    
    def __str__(self):
        return f"{self.name} ({self.tenant_id})"
