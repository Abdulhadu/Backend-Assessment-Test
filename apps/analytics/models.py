import uuid
from django.db import models
from django.core.validators import MinValueValidator
from decimal import Decimal
from datetime import date


class PriceHistory(models.Model):
    """
    PriceHistory model representing historical pricing data.
    Tracks price changes over time with effective date ranges.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey(
        'tenants.Tenant', 
        on_delete=models.CASCADE, 
        db_column='tenant_id',
        related_name='price_history'
    )
    product = models.ForeignKey(
        'products.Product', 
        on_delete=models.CASCADE, 
        db_column='product_id',
        related_name='price_history'
    )
    price = models.DecimalField(
        max_digits=10, 
        decimal_places=2, 
        validators=[MinValueValidator(Decimal('0.01'))],
        help_text="Price at this point in time"
    )
    effective_from = models.DateTimeField(help_text="When this price became effective")
    effective_to = models.DateTimeField(null=True, blank=True, help_text="When this price stopped being effective")
    
    class Meta:
        db_table = 'price_history'
        verbose_name = 'Price History'
        verbose_name_plural = 'Price History'
        indexes = [
            models.Index(fields=['product', 'effective_from'], name='price_history_product_from_idx'),
            models.Index(fields=['tenant', 'effective_from'], name='price_history_tenant_from_idx'),
            models.Index(fields=['effective_from'], name='price_history_eff_from_idx'),
        ]
    
    def __str__(self):
        return f"{self.product.name}: ${self.price} ({self.effective_from})"


class PriceEvent(models.Model):
    """
    PriceEvent model representing price change events.
    Tracks price changes with anomaly detection and metadata.
    """
    event_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey(
        'tenants.Tenant', 
        on_delete=models.CASCADE, 
        db_column='tenant_id',
        related_name='price_events'
    )
    product = models.ForeignKey(
        'products.Product', 
        on_delete=models.CASCADE, 
        db_column='product_id',
        related_name='price_events'
    )
    old_price = models.DecimalField(
        max_digits=10, 
        decimal_places=2, 
        validators=[MinValueValidator(Decimal('0.01'))],
        help_text="Previous price"
    )
    new_price = models.DecimalField(
        max_digits=10, 
        decimal_places=2, 
        validators=[MinValueValidator(Decimal('0.01'))],
        help_text="New price"
    )
    pct_change = models.DecimalField(
        max_digits=8, 
        decimal_places=4,
        help_text="Percentage change in price"
    )
    anomaly_flag = models.BooleanField(default=False, help_text="Whether this price change is anomalous")
    meta = models.JSONField(default=dict, blank=True, help_text="Additional event metadata")
    received_at = models.DateTimeField(auto_now_add=True, help_text="Event reception timestamp")
    
    class Meta:
        db_table = 'price_events'
        verbose_name = 'Price Event'
        verbose_name_plural = 'Price Events'
        indexes = [
            models.Index(fields=['tenant', 'product', 'received_at'], name='pe_tpt_idx'),
            models.Index(fields=['product', 'received_at'], name='pe_prd_time_idx'),
            models.Index(fields=['anomaly_flag'], name='pe_anom_idx'),
            models.Index(fields=['received_at'], name='pe_recv_idx'),
        ]

    
    def __str__(self):
        return f"Price Event: {self.product.name} ${self.old_price} → ${self.new_price}"


class IdempotencyKey(models.Model):
    """
    IdempotencyKey model for preventing duplicate API requests.
    Ensures operations are idempotent by tracking request hashes.
    """
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey(
        'tenants.Tenant', 
        on_delete=models.CASCADE, 
        db_column='tenant_id',
        related_name='idempotency_keys'
    )
    idempotency_key = models.TextField(max_length=255, help_text="Unique idempotency key")
    request_hash = models.TextField(max_length=64, help_text="Hash of the request payload")
    response_summary = models.JSONField(default=dict, blank=True, help_text="Summary of the response")
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    created_at = models.DateTimeField(auto_now_add=True, help_text="Key creation timestamp")
    expires_at = models.DateTimeField(help_text="When this key expires")
    
    class Meta:
        db_table = 'idempotency_keys'
        verbose_name = 'Idempotency Key'
        verbose_name_plural = 'Idempotency Keys'
        indexes = [
            models.Index(fields=['tenant', 'idempotency_key'], name='idempotency_tenant_key_idx'),
            models.Index(fields=['expires_at'], name='idempotency_expires_at_idx'),
            models.Index(fields=['status'], name='idempotency_status_idx'),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=['tenant', 'idempotency_key'], 
                name='unique_tenant_idempotency_key'
            ),
        ]
    
    def __str__(self):
        return f"Idempotency Key: {self.idempotency_key} ({self.status})"


class ExportJob(models.Model):
    """
    ExportJob model representing data export operations.
    Tracks export requests and their status.
    """
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
        ('cancelled', 'Cancelled'),
    ]
    
    FORMAT_CHOICES = [
        ('csv', 'CSV'),
        ('json', 'JSON'),
        ('xlsx', 'Excel'),
        ('parquet', 'Parquet'),
    ]
    
    export_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey(
        'tenants.Tenant', 
        on_delete=models.CASCADE, 
        db_column='tenant_id',
        related_name='export_jobs'
    )
    format = models.CharField(max_length=20, choices=FORMAT_CHOICES, help_text="Export file format")
    filters = models.JSONField(default=dict, blank=True, help_text="Export filters and criteria")
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    manifest = models.JSONField(default=dict, blank=True, help_text="Export metadata and configuration")
    created_at = models.DateTimeField(auto_now_add=True, help_text="Export creation timestamp")
    
    class Meta:
        db_table = 'export_jobs'
        verbose_name = 'Export Job'
        verbose_name_plural = 'Export Jobs'
        indexes = [
            models.Index(fields=['tenant', 'status'], name='export_jobs_tenant_status_idx'),
            models.Index(fields=['format'], name='export_jobs_format_idx'),
            models.Index(fields=['created_at'], name='export_jobs_created_at_idx'),
        ]
    
    def __str__(self):
        return f"Export Job {self.export_id} ({self.format})"


class ExportChunk(models.Model):
    """
    ExportChunk model representing individual chunks of exported data.
    Each chunk is part of a larger export job.
    """
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    chunk_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    export_job = models.ForeignKey(
        ExportJob, 
        on_delete=models.CASCADE, 
        db_column='export_id',
        related_name='chunks'
    )
    chunk_index = models.PositiveIntegerField(help_text="Order of this chunk in the export")
    storage_path = models.TextField(max_length=500, help_text="Path to the chunk file in storage")
    bytes = models.PositiveBigIntegerField(default=0, help_text="Size of the chunk in bytes")
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    
    class Meta:
        db_table = 'export_chunks'
        verbose_name = 'Export Chunk'
        verbose_name_plural = 'Export Chunks'
        indexes = [
            models.Index(fields=['export_job', 'chunk_index'], name='export_chunks_job_index_idx'),
            models.Index(fields=['status'], name='export_chunks_status_idx'),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=['export_job', 'chunk_index'], 
                name='unique_export_chunk_index'
            ),
        ]
    
    def __str__(self):
        return f"Export Chunk {self.chunk_index} of Job {self.export_job.export_id}"


class MetricsPreagg(models.Model):
    """
    MetricsPreagg model representing pre-aggregated metrics.
    Stores computed metrics for fast querying and reporting.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey(
        'tenants.Tenant', 
        on_delete=models.CASCADE, 
        db_column='tenant_id',
        related_name='metrics_preagg'
    )
    group_key = models.TextField(max_length=255, help_text="Grouping key for the metrics")
    period_start = models.DateField(help_text="Start of the metrics period")
    period_end = models.DateField(help_text="End of the metrics period")
    metrics = models.JSONField(default=dict, help_text="Pre-aggregated metrics data")
    last_updated = models.DateTimeField(auto_now=True, help_text="Last metrics update")
    
    class Meta:
        db_table = 'metrics_preagg'
        verbose_name = 'Pre-aggregated Metrics'
        verbose_name_plural = 'Pre-aggregated Metrics'
        indexes = [
            models.Index(fields=['tenant', 'period_start'], name='metrics_tenant_period_idx'),
            models.Index(fields=['group_key'], name='metrics_group_key_idx'),
            models.Index(fields=['period_start', 'period_end'], name='metrics_period_range_idx'),
        ]
    
    def __str__(self):
        return f"Metrics {self.group_key} ({self.period_start} - {self.period_end})"


class AuditLog(models.Model):
    """
    AuditLog model representing audit trail for data changes.
    Tracks all modifications to critical data for compliance.
    """
    ACTION_CHOICES = [
        ('create', 'Create'),
        ('update', 'Update'),
        ('delete', 'Delete'),
        ('restore', 'Restore'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey(
        'tenants.Tenant', 
        on_delete=models.CASCADE, 
        db_column='tenant_id',
        related_name='audit_logs'
    )
    source_table = models.TextField(max_length=100, help_text="Name of the table that was modified")
    source_id = models.UUIDField(help_text="ID of the record that was modified")
    action = models.CharField(max_length=20, choices=ACTION_CHOICES, help_text="Type of action performed")
    diff = models.JSONField(default=dict, blank=True, help_text="Changes made to the record")
    created_at = models.DateTimeField(auto_now_add=True, help_text="Audit log creation timestamp")
    
    class Meta:
        db_table = 'audit_logs'
        verbose_name = 'Audit Log'
        verbose_name_plural = 'Audit Logs'
        indexes = [
            models.Index(fields=['tenant', 'source_table'], name='audit_logs_tenant_table_idx'),
            models.Index(fields=['source_table', 'source_id'], name='audit_logs_table_id_idx'),
            models.Index(fields=['action'], name='audit_logs_action_idx'),
            models.Index(fields=['created_at'], name='audit_logs_created_at_idx'),
        ]
    
    def __str__(self):
        return f"Audit: {self.action} on {self.source_table}.{self.source_id}"
