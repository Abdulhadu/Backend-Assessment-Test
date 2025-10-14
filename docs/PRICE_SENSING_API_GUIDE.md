# Price-Sensing API Implementation

## Overview

This implementation provides a complete real-time price-sensing API with anomaly detection, rate limiting, and streaming capabilities under the tenants app.

## Features Implemented

### 1. Celery Beat Scheduler Configuration

**Problem**: Management command approach was causing format errors and was unnecessarily complex.

**Solution**: Configured Celery Beat scheduler directly in `main/settings/base.py`:

```python
# Celery Beat Schedule Configuration
CELERY_BEAT_SCHEDULE = {
    'generate-daily-metrics': {
        'task': 'apps.core.tasks.metrics.generate_daily_metrics',
        'schedule': 86400.0,  # Run daily (24 hours)
        'options': {'queue': 'default'}
    },
    'generate-hourly-metrics': {
        'task': 'apps.core.tasks.metrics.generate_hourly_metrics',
        'schedule': 3600.0,  # Run hourly
        'options': {'queue': 'default'}
    },
    'cleanup-expired-data': {
        'task': 'apps.core.tasks.maintenance.cleanup_expired_data',
        'schedule': 86400.0,  # Run daily
        'options': {'queue': 'default'}
    },
}
```

**Benefits**:
- ✅ No management command needed
- ✅ Automatic task scheduling
- ✅ Database-backed scheduler
- ✅ Easy to modify schedules

### 2. Price-Sensing API Endpoints

#### POST `/api/v1/tenants/{tenant_id}/products/{product_id}/price-event/`

**Features**:
- ✅ **Rate Limiting**: 100 requests/minute per tenant, 10 requests/minute per product
- ✅ **Anomaly Detection**: Detects price changes ≥10%, ≥25%, ≥50%
- ✅ **Idempotency**: Webhook retry support with `Idempotency-Key` header
- ✅ **Change Stream**: Stores events in `price_events` table
- ✅ **Authentication**: API key validation
- ✅ **Swagger Documentation**: Complete OpenAPI specs

**Request Example**:
```bash
curl -X POST \
  'http://127.0.0.1:8000/api/v1/tenants/52fb8ddf-c975-4374-9655-fc4e5adc9cbc/products/123e4567-e89b-12d3-a456-426614174000/price-event/' \
  -H 'X-API-Key: your-api-key' \
  -H 'Idempotency-Key: unique-key-123' \
  -H 'Content-Type: application/json' \
  -d '{
    "old_price": 100.0,
    "new_price": 150.0,
    "source": "api",
    "metadata": {"user_id": "user123"}
  }'
```

**Response Example**:
```json
{
  "event_id": "123e4567-e89b-12d3-a456-426614174000",
  "anomaly_detected": true,
  "anomaly_reason": "Significant price change: 50.0%",
  "pct_change": 50.0,
  "status": "processed",
  "processed_at": "2025-01-15T10:30:00Z"
}
```

#### GET `/api/v1/tenants/{tenant_id}/products/{product_id}/price-anomalies/`

**Features**:
- ✅ **Streaming Response**: NDJSON format for real-time monitoring
- ✅ **Time-based Filtering**: `hours` parameter (default: 24)
- ✅ **Limit Control**: `limit` parameter (max: 1000)
- ✅ **Low Memory**: Uses Django iterator for large datasets
- ✅ **Metadata**: Includes product info and summary

**Request Example**:
```bash
curl -X GET \
  'http://127.0.0.1:8000/api/v1/tenants/52fb8ddf-c975-4374-9655-fc4e5adc9cbc/products/123e4567-e89b-12d3-a456-426614174000/price-anomalies/?hours=24&limit=10' \
  -H 'X-API-Key: your-api-key'
```

**Response Example** (NDJSON):
```
{"event_id": "...", "product_id": "...", "old_price": 100.0, "new_price": 150.0, "pct_change": 50.0, "anomaly_reason": "Significant price change: 50.0%", "received_at": "2025-01-15T10:30:00Z"}
{"event_id": "...", "product_id": "...", "old_price": 50.0, "new_price": 75.0, "pct_change": 50.0, "anomaly_reason": "Extreme price change: 50.0%", "received_at": "2025-01-15T09:15:00Z"}
{"_meta": {"total_anomalies": 2, "time_range_hours": 24, "product_id": "...", "streamed_at": "2025-01-15T10:35:00Z"}}
```

### 3. Database Models

#### PriceEvent Model
```python
class PriceEvent(models.Model):
    event_id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    tenant = models.ForeignKey('tenants.Tenant', on_delete=models.CASCADE)
    product = models.ForeignKey('products.Product', on_delete=models.CASCADE)
    old_price = models.DecimalField(max_digits=12, decimal_places=2)
    new_price = models.DecimalField(max_digits=12, decimal_places=2)
    pct_change = models.DecimalField(max_digits=8, decimal_places=2)
    anomaly_flag = models.BooleanField(default=False)
    meta = models.JSONField(default=dict)
    received_at = models.DateTimeField(auto_now_add=True)
```

#### IdempotencyKey Model
```python
class IdempotencyKey(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    tenant = models.ForeignKey('tenants.Tenant', on_delete=models.CASCADE)
    idempotency_key = models.CharField(max_length=255)
    request_hash = models.CharField(max_length=64)
    response_summary = models.JSONField(default=dict)
    status = models.CharField(max_length=20, choices=[...])
    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField()
```

### 4. Anomaly Detection Logic

**Thresholds**:
- **Extreme**: ≥50% change → "Extreme price change"
- **Significant**: ≥25% change → "Significant price change"  
- **Notable**: ≥10% change for items >$100 → "Notable price change for high-value item"

**Algorithm**:
```python
def _detect_anomaly(self, pct_change: Decimal, old_price: Decimal, new_price: Decimal):
    abs_change = abs(pct_change)
    
    if abs_change >= 50:
        return True, f"Extreme price change: {pct_change:.1f}%"
    elif abs_change >= 25:
        return True, f"Significant price change: {pct_change:.1f}%"
    elif abs_change >= 10 and old_price > 100:
        return True, f"Notable price change for high-value item: {pct_change:.1f}%"
    
    return False, None
```

### 5. Rate Limiting Implementation

**Per-Tenant**: 100 requests/minute
**Per-Product**: 10 requests/minute

**Implementation**:
```python
def _check_rate_limit(self, tenant: Tenant, product: Product) -> bool:
    # Per-tenant rate limit: 100 requests per minute
    tenant_key = f"price_event_rate_limit:tenant:{tenant.tenant_id}"
    tenant_count = cache.get(tenant_key, 0)
    if tenant_count >= 100:
        return False
    
    # Per-product rate limit: 10 requests per minute
    product_key = f"price_event_rate_limit:product:{product.product_id}"
    product_count = cache.get(product_key, 0)
    if product_count >= 10:
        return False
    
    # Increment counters
    cache.set(tenant_key, tenant_count + 1, 60)
    cache.set(product_key, product_count + 1, 60)
    
    return True
```

### 6. Idempotency Implementation

**Features**:
- ✅ **Request Hashing**: SHA256 hash of request body
- ✅ **Response Caching**: Stores response summary
- ✅ **Expiration**: 24-hour TTL
- ✅ **Status Tracking**: pending/completed/failed

**Flow**:
1. Check if `Idempotency-Key` exists for tenant
2. If exists and completed → return cached response
3. If not exists → process request and cache response
4. If exists but pending → wait or retry

### 7. Data Cleanup Task

**Automated Cleanup**:
- **Expired Idempotency Keys**: >24 hours old
- **Old Price Events**: >30 days old
- **Scheduled**: Daily via Celery Beat

## Usage Instructions

### 1. Database Migration
```bash
python manage.py makemigrations tenants
python manage.py migrate
```

### 2. Start Celery Beat
```bash
celery -A main beat --loglevel=info
```

### 3. Start Celery Worker
```bash
celery -A main worker --loglevel=info
```

### 4. Test API
```bash
python test_price_sensing.py
```

### 5. View Swagger Documentation
Visit: `http://127.0.0.1:8000/api/docs/`

## Architecture Benefits

### ✅ **Scalability**
- Streaming responses for large datasets
- Database-backed rate limiting
- Efficient indexing for fast queries

### ✅ **Reliability**
- Idempotency for webhook retries
- Atomic transactions for data consistency
- Comprehensive error handling

### ✅ **Performance**
- Low memory footprint with iterators
- Cached rate limiting
- Optimized database queries

### ✅ **Monitoring**
- Detailed logging
- Anomaly detection with reasons
- Real-time streaming for alerts

### ✅ **Security**
- API key authentication
- Tenant isolation
- Rate limiting protection

## Error Handling

- **400**: Bad request (invalid data, missing headers)
- **401**: Unauthorized (invalid API key)
- **404**: Not found (product doesn't exist)
- **429**: Rate limited (too many requests)
- **500**: Internal server error (with details)

## Future Enhancements

1. **Machine Learning**: Advanced anomaly detection
2. **Webhooks**: Push notifications for anomalies
3. **Analytics**: Price trend analysis
4. **Caching**: Redis for better performance
5. **Monitoring**: Prometheus metrics integration
