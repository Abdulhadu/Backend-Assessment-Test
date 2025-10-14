# Bulk Stock Update API

## Overview

The Bulk Stock Update API provides transactional, product-level atomic updates for inventory management in a multi-tenant e-commerce system. It processes stock events from NDJSON files with built-in conflict resolution and row-level locking.

## Features

- **Product-Level Atomicity**: Each product's events are processed in a single transaction
- **Row-Level Locking**: Prevents race conditions using `select_for_update()`
- **Conflict Detection**: Identifies and reports processing failures per product
- **Partial Success Handling**: Some products can succeed while others fail
- **Zero-Delta Filtering**: Automatically skips events with no stock change
- **Flexible Event Sources**: Supports multiple event sources (manual, order, receipt, return, etc.)
- **Metadata Support**: Allows custom metadata per stock event

## API Endpoint

### Bulk Stock Update

**Endpoint**: `PUT /api/v1/tenants/{tenant_id}/stock/bulk_update`

**Method**: `POST` (using PUT in URL path)

**Content-Type**: `multipart/form-data`

**Authentication**: Currently AllowAny (configure as needed)

#### Path Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `tenant_id` | string | Yes | Unique tenant identifier (UUID) |

#### Request Body

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `file` | file | Yes | NDJSON file containing stock events |

#### NDJSON Event Format

Each line in the NDJSON file must be a valid JSON object with the following structure:

```json
{
  "product_id": "uuid",
  "delta": -50,
  "event_time": "2024-01-15T10:30:00Z",
  "source": "order",
  "meta": {
    "order_id": "12345",
    "operator": "John Doe",
    "reason": "Customer order fulfillment"
  }
}
```

**Field Descriptions:**

- **product_id** (required): UUID of the product
- **delta** (required): Stock change amount (positive for additions, negative for reductions)
- **event_time** (optional): ISO 8601 timestamp of the event (defaults to current time)
- **source** (optional): Event source identifier (defaults to "system")
- **meta** (optional): Additional metadata as JSON object

#### Response Format

**Success Response (200 OK)**

```json
{
  "status": "success",
  "applied": [
    {
      "product_id": "550e8400-e29b-41d4-a716-446655440000",
      "events_processed": 15,
      "final_level": 245
    },
    {
      "product_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
      "events_processed": 8,
      "final_level": 102
    }
  ],
  "total_events_processed": 23
}
```

**Partial Success Response (200 OK)**

```json
{
  "status": "partial_success",
  "conflicts": [
    {
      "product_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
      "reason": "product not found"
    },
    {
      "product_id": "886313e1-3b8a-5372-9b90-0c9aee199e5d",
      "reason": "processing error: invalid delta value"
    }
  ],
  "applied": [
    {
      "product_id": "550e8400-e29b-41d4-a716-446655440000",
      "events_processed": 15,
      "final_level": 245
    }
  ],
  "total_events_processed": 15
}
```

**Error Response (400 Bad Request)**

```json
{
  "error": "file must be .ndjson"
}
```

### Response Field Explanations

#### `final_level`
The **final stock level** after all events for a product have been processed. This represents the resulting inventory quantity available for that product after applying all deltas in the batch.

**Example:**
- Initial stock: 200 units
- Event 1: delta = +50 (receipt)
- Event 2: delta = -30 (order)
- Event 3: delta = +25 (return)
- **final_level = 245 units**

#### `events_processed`
The **number of stock events** successfully applied for a specific product in the current batch. This count excludes zero-delta events which are automatically skipped.

**Example:**
- Batch contains 20 events for product A
- 3 events have delta = 0 (skipped)
- 2 events fail validation
- **events_processed = 15** (successfully applied)

## Conflict Resolution

### Current Strategy: Last Write Wins

The API currently implements a **last-write-wins** strategy with product-level locking:

1. **Row-Level Locking**: Uses Django's `select_for_update()` to lock the stock level row
2. **Sequential Processing**: Events for the same product are processed in order
3. **Transaction Isolation**: Each product's events are in a separate transaction
4. **Automatic Rollback**: If any event for a product fails, all events for that product are rolled back

### Concurrency Handling

When two requests try to update the same product simultaneously:

1. First request acquires row lock
2. Second request waits for lock release
3. Events are processed sequentially
4. Final state reflects both updates in order

### Conflict Detection

The API detects and reports conflicts:

- **Product Not Found**: Referenced product doesn't exist in tenant
- **Processing Errors**: Invalid data or constraint violations
- **Lock Timeouts**: Database lock acquisition failures

## Testing

### Prerequisites

1. **Running Django Server**
   ```bash
   python manage.py runserver
   ```

2. **Generated Test Data**
   ```bash
   # Generate stock events data
   python gen_dataset.py --preset small
   ```

3. **Valid Tenant ID**
   - Extract tenant_id from generated `tenants.csv`
   - Or create a tenant via API/admin

### Test Script: `test_stock_upload.py`

#### Installation

Save the test script as `test_stock_upload.py`:

```python
#!/usr/bin/env python3
"""
Test script for bulk stock events ingestion from generated_data files.
"""
import os
import sys
import requests
import argparse
from pathlib import Path


def test_stock_events_upload(tenant_id: str, file_path: str, api_url: str = "http://127.0.0.1:8000"):
    """Upload a stock events NDJSON file to the bulk update endpoint."""
    url = f"{api_url}/api/v1/tenants/{tenant_id}/stock/bulk_update"
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return False
    
    try:
        with open(file_path, 'rb') as f:
            files = {'file': (os.path.basename(file_path), f, 'application/x-ndjson')}
            
            print(f"📤 Uploading {file_path} to {url}")
            response = requests.post(url, files=files, timeout=60)
            
            print(f"📊 Status: {response.status_code}")
            print(f"📋 Response: {response.json()}")
            
            if response.status_code in [200, 207]:
                print("✅ Upload successful")
                return True
            else:
                print("❌ Upload failed")
                return False
                
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to API. Is the server running?")
        return False
    except Exception as e:
        print(f"❌ Upload error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Test stock events bulk upload")
    parser.add_argument("--tenant-id", required=True, help="Tenant ID")
    parser.add_argument("--file", help="Specific NDJSON file to upload")
    parser.add_argument("--data-dir", default="generated_data", help="Directory with generated data")
    parser.add_argument("--api-url", default="http://127.0.0.1:8000", help="API base URL")
    parser.add_argument("--limit", type=int, help="Limit number of files to process")
    
    args = parser.parse_args()
    
    if args.file:
        # Upload specific file
        success = test_stock_events_upload(args.tenant_id, args.file, args.api_url)
        sys.exit(0 if success else 1)
    
    # Find and upload stock events files for the tenant
    data_dir = Path(args.data_dir)
    pattern = f"stock_events_{args.tenant_id}_chunk_*.ndjson"
    files = list(data_dir.glob(pattern))
    
    if not files:
        print(f"❌ No stock events files found for tenant {args.tenant_id}")
        print(f"   Pattern: {pattern}")
        sys.exit(1)
    
    print(f"📁 Found {len(files)} stock events files")
    
    if args.limit:
        files = files[:args.limit]
        print(f"🔢 Limited to {len(files)} files")
    
    success_count = 0
    for file_path in files:
        print(f"\n{'='*60}")
        print(f"Processing: {file_path.name}")
        if test_stock_events_upload(args.tenant_id, str(file_path), args.api_url):
            success_count += 1
    
    print(f"\n{'='*60}")
    print(f"📈 Summary: {success_count}/{len(files)} files uploaded successfully")
    
    if success_count == len(files):
        print("🎉 All uploads successful!")
        sys.exit(0)
    else:
        print("⚠️  Some uploads failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
```

Make it executable:
```bash
chmod +x test_stock_upload.py
```

#### Usage Examples

**1. Upload All Stock Events for a Tenant**

```bash
python test_stock_upload.py --tenant-id <your-tenant-id>
```

**2. Upload a Specific File**

```bash
python test_stock_upload.py \
  --tenant-id <your-tenant-id> \
  --file generated_data/stock_events_<tenant-id>_chunk_0000.ndjson
```

**3. Test with Limited Files**

```bash
python test_stock_upload.py \
  --tenant-id <your-tenant-id> \
  --limit 3
```

**4. Custom API URL and Data Directory**

```bash
python test_stock_upload.py \
  --tenant-id <your-tenant-id> \
  --data-dir /path/to/data \
  --api-url http://localhost:9000
```

### Step-by-Step Testing Guide

#### Step 1: Generate Test Data

```bash
# Generate small dataset with stock events
python gen_dataset.py --preset small

# Check generated files
ls -lh generated_data/stock_events_*.ndjson
```

#### Step 2: Get Tenant ID

```bash
# Option 1: From generated tenants file
cat generated_data/tenants.csv

# Option 2: From Django shell
python manage.py shell
>>> from apps.tenants.models import Tenant
>>> tenant = Tenant.objects.first()
>>> print(tenant.tenant_id)
```

#### Step 3: Ensure Products Exist

The stock events reference products. Make sure products are loaded:

```bash
# Check if products exist for tenant
python manage.py shell
>>> from apps.products.models import Product
>>> Product.objects.filter(tenant_id='<your-tenant-id>').count()
```

If products don't exist, upload them first (requires separate product upload endpoint).

#### Step 4: Run the Test

```bash
# Upload stock events
python test_stock_upload.py --tenant-id <your-tenant-id>
```

#### Step 5: Verify Results

```bash
# Check stock levels in database
python manage.py shell
>>> from apps.stocks.models import StockLevel, StockEvent
>>> 
>>> # Check stock levels
>>> StockLevel.objects.filter(tenant_id='<tenant-id>').count()
>>> 
>>> # Check stock events
>>> StockEvent.objects.filter(tenant_id='<tenant-id>').count()
>>> 
>>> # Check a specific product's stock
>>> level = StockLevel.objects.get(product_id='<product-id>')
>>> print(f"Available: {level.available}")
>>> 
>>> # View recent events for a product
>>> events = StockEvent.objects.filter(
...     product_id='<product-id>'
... ).order_by('-event_time')[:10]
>>> for e in events:
...     print(f"{e.event_time}: {e.delta:+d} -> {e.resulting_level}")
```

### Expected Test Output

```
📁 Found 5 stock events files

============================================================
Processing: stock_events_550e8400-e29b-41d4-a716-446655440000_chunk_0000.ndjson
📤 Uploading generated_data/stock_events_550e8400-e29b-41d4-a716-446655440000_chunk_0000.ndjson to http://127.0.0.1:8000/api/v1/tenants/550e8400-e29b-41d4-a716-446655440000/stock/bulk_update
📊 Status: 200
📋 Response: {
  'status': 'success',
  'applied': [
    {'product_id': '6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'events_processed': 1000, 'final_level': 523},
    {'product_id': '7c9e6679-7425-40de-944b-e07fc1f90ae7', 'events_processed': 1000, 'final_level': 412}
  ],
  'total_events_processed': 2000
}
✅ Upload successful

============================================================
📈 Summary: 5/5 files uploaded successfully
🎉 All uploads successful!
```

## Performance Considerations

### Optimization Tips

1. **Batch Size**: Keep files under 10,000 events per product for optimal transaction size
2. **Chunking**: Use the data generator's chunking feature for large datasets
3. **Concurrent Uploads**: Can upload different tenants in parallel
4. **Product Grouping**: Events are automatically grouped by product internally

### Expected Performance

| Events per File | Products | Processing Time | Throughput |
|----------------|----------|-----------------|------------|
| 1,000 | 10 | ~2 seconds | 500 events/sec |
| 10,000 | 100 | ~15 seconds | 667 events/sec |
| 100,000 | 1,000 | ~3 minutes | 556 events/sec |

*Performance varies based on hardware, database, and concurrent load*

## Error Handling

### Common Errors

**1. File Not Found**
```json
{"error": "file required"}
```
**Solution**: Include `file` parameter in multipart form data

**2. Invalid File Format**
```json
{"error": "file must be .ndjson"}
```
**Solution**: Ensure file has `.ndjson` extension

**3. Empty File**
```json
{"error": "no valid events found"}
```
**Solution**: Check file contains valid JSON lines

**4. Product Not Found**
```json
{
  "status": "partial_success",
  "conflicts": [
    {"product_id": "uuid", "reason": "product not found"}
  ]
}
```
**Solution**: Ensure products exist before uploading stock events

### Debugging

Enable verbose logging:

```python
# settings.py or local_settings.py
LOGGING = {
    'version': 1,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
        },
    },
    'loggers': {
        'apps.stocks': {
            'handlers': ['console'],
            'level': 'DEBUG',
        },
    },
}
```

Check logs for detailed error messages:
```bash
tail -f logs/django.log
```

## Database Schema

### StockLevel Model

```python
class StockLevel:
    id: UUID (primary key)
    tenant_id: UUID (foreign key)
    product: ForeignKey(Product)
    available: Integer (current stock quantity)
    created_at: DateTime
    updated_at: DateTime
```

### StockEvent Model

```python
class StockEvent:
    stock_event_id: UUID (primary key)
    tenant_id: UUID (foreign key)
    product: ForeignKey(Product)
    delta: Integer (stock change amount)
    resulting_level: Integer (stock after change)
    event_time: DateTime
    source: String (event source)
    meta: JSONField (additional data)
    created_at: DateTime
```

## Future Enhancements

### Planned Features

1. **Conflict Resolution Options**
   - `last_write_wins` (current default)
   - `merge` (additive deltas)
   - `reject` (fail on conflicts)
   - `optimistic_locking` (version-based)

2. **Advanced Locking**
   - Application-level advisory locks
   - Distributed locks (Redis)
   - Optimistic concurrency control

3. **Async Processing**
   - Queue-based processing for large files
   - Background job status tracking
   - Webhook notifications on completion

4. **Validation Rules**
   - Minimum stock thresholds
   - Maximum stock limits
   - Business rule validation

5. **Audit Trail**
   - Complete event history
   - User attribution
   - Rollback capabilities

## Security Considerations

### Current Implementation

- **Authentication**: Currently `AllowAny` - update for production
- **Tenant Isolation**: All queries filtered by `tenant_id`
- **Input Validation**: JSON parsing with error handling
- **Transaction Safety**: ACID compliance via Django ORM

### Production Recommendations

1. **Enable Authentication**
   ```python
   permission_classes = [IsAuthenticated, IsTenantMember]
   ```

2. **Rate Limiting**
   ```python
   from rest_framework.throttling import UserRateThrottle
   throttle_classes = [UserRateThrottle]
   ```

3. **File Size Limits**
   ```python
   FILE_UPLOAD_MAX_MEMORY_SIZE = 10 * 1024 * 1024  # 10MB
   ```

4. **Virus Scanning**
   - Scan uploaded files before processing
   - Use ClamAV or similar

## Troubleshooting

### Issue: Transaction Deadlocks

**Symptoms**: Database deadlock errors in logs

**Solutions**:
- Reduce concurrent uploads for same tenant
- Increase database timeout settings
- Sort events by product_id before processing

### Issue: Memory Usage

**Symptoms**: Out of memory errors with large files

**Solutions**:
- Reduce chunk size in data generator
- Process files in streaming mode
- Increase server memory

### Issue: Slow Performance

**Symptoms**: Long processing times

**Solutions**:
- Add database indexes on `product_id` and `tenant_id`
- Increase database connection pool size
- Use database-level batch inserts
- Enable query optimization

## Support

For issues or questions:
1. Check server logs: `tail -f logs/django.log`
2. Verify database connection: `python manage.py dbshell`
3. Test with small dataset first
4. Review API documentation: `/api/schema/swagger-ui/`

## Appendix

### Complete Example Request

```bash
curl -X POST \
  'http://localhost:8000/api/v1/tenants/550e8400-e29b-41d4-a716-446655440000/stock/bulk_update' \
  -H 'Content-Type: multipart/form-data' \
  -F 'file=@generated_data/stock_events_550e8400-e29b-41d4-a716-446655440000_chunk_0000.ndjson'
```

### Sample NDJSON File Content

```ndjson
{"product_id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8","delta":50,"event_time":"2024-01-15T10:00:00Z","source":"receipt","meta":{"po_number":"PO-12345","supplier":"ACME Corp"}}
{"product_id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8","delta":-10,"event_time":"2024-01-15T14:30:00Z","source":"order","meta":{"order_id":"ORD-67890"}}
{"product_id":"7c9e6679-7425-40de-944b-e07fc1f90ae7","delta":100,"event_time":"2024-01-15T09:00:00Z","source":"manual","meta":{"reason":"Initial stock","operator":"John Doe"}}
```