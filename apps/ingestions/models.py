import uuid
from django.db import models


class IngestUpload(models.Model):
    """
    IngestUpload model representing bulk data uploads.
    Each upload contains multiple chunks of data to be processed.
    """
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
        ('cancelled', 'Cancelled'),
    ]
    
    upload_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey(
        'tenants.Tenant', 
        on_delete=models.CASCADE, 
        db_column='tenant_id',
        related_name='ingest_uploads'
    )
    upload_token = models.TextField(unique=True, max_length=255, help_text="Unique upload identifier")
    status = models.CharField(max_length=50, choices=STATUS_CHOICES, default='pending')
    manifest = models.JSONField(default=dict, blank=True, help_text="Upload metadata and configuration")
    created_at = models.DateTimeField(auto_now_add=True, help_text="Upload creation timestamp")
    last_activity = models.DateTimeField(auto_now=True, help_text="Last activity timestamp")
    
    class Meta:
        db_table = 'ingest_uploads'
        verbose_name = 'Ingest Upload'
        verbose_name_plural = 'Ingest Uploads'
        indexes = [
            models.Index(fields=['tenant', 'status'], name='ingest_uploads_tenant_stat_idx'),
            models.Index(fields=['upload_token'], name='ingest_uploads_token_idx'),
            models.Index(fields=['created_at'], name='ingest_uploads_created_at_idx'),
            models.Index(fields=['last_activity'], name='ingest_uploads_last_act_idx'),
        ]
    
    def __str__(self):
        return f"Upload {self.upload_token} ({self.status})"


class OrderIngestChunk(models.Model):
    """
    OrderIngestChunk model representing individual chunks of order data.
    Each chunk is part of a larger upload and contains processed order data.
    """
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
        ('skipped', 'Skipped'),
    ]
    
    chunk_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    upload = models.ForeignKey(
        IngestUpload, 
        on_delete=models.CASCADE, 
        db_column='upload_id',
        related_name='chunks'
    )
    chunk_index = models.PositiveIntegerField(help_text="Order of this chunk in the upload")
    file_path = models.TextField(null=True, blank=True, help_text="Path to the uploaded file")
    content_type = models.TextField(default='application/x-ndjson', null=True, blank=True, help_text="MIME type of the file")
    checksum = models.TextField(max_length=64, help_text="Data integrity checksum")
    status = models.CharField(max_length=50, choices=STATUS_CHOICES, default='pending')
    rows = models.PositiveIntegerField(default=0, help_text="Number of rows in this chunk")
    errors_sample = models.JSONField(default=list, blank=True, help_text="Sample of processing errors")
    received_at = models.DateTimeField(auto_now_add=True, help_text="Chunk reception timestamp")
    
    class Meta:
        db_table = 'order_ingest_chunks'
        verbose_name = 'Order Ingest Chunk'
        verbose_name_plural = 'Order Ingest Chunks'
        indexes = [
            models.Index(fields=['upload', 'chunk_index'], name='chunks_upload_index_idx'),
            models.Index(fields=['status'], name='chunks_status_idx'),
            models.Index(fields=['received_at'], name='chunks_received_at_idx'),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=['upload', 'chunk_index'], 
                name='unique_upload_chunk_index'
            ),
        ]
    
    def __str__(self):
        return f"Chunk {self.chunk_index} of Upload {self.upload.upload_token}"
