# Complete Data Ingestion Guide

This guide explains how to perform bulk data ingestion after generating synthetic data with `gen_dataset.py`.

## Problem Overview

When you run `gen_dataset.py`, it generates:
1. **Tenant data** (saved to `tenants.csv`)
2. **Product, customer, order data** for each tenant (saved to various files)

However, the **tenants are not automatically saved to the database**, so you don't have their API keys to use for ingestion.

## Solution: Complete Ingestion Workflow

### Step 1: Generate Data (Already Done)

You've already run:
```bash
python gen_dataset.py --tenants 5 --products-per-tenant 100000 --orders-per-tenant 500000
```

This created:
- `generated_data/tenants.csv` - Tenant information
- `generated_data/products_<tenant_id>_chunk_*.ndjson` - Product data
- `generated_data/customers_<tenant_id>.ndjson` - Customer data  
- `generated_data/orders_<tenant_id>_chunk_*.ndjson` - Order data
- `generated_data/order_items_<tenant_id>_chunk_*.ndjson` - Order items
- And more...

### Step 2: Load Tenants into Database

**First, load the tenants into your database:**

```bash
python manage.py load_tenants --file generated_data/tenants.csv
```

This command will:
- Read the `tenants.csv` file
- Create `Tenant` records in the database
- Show you the API keys for each tenant

**Example output:**
```
Loading tenants from: generated_data/tenants.csv
Found 5 tenants in CSV file
Successfully processed tenants: 5 created, 0 updated, 0 skipped

================================================================================
TENANT API KEYS FOR INGESTION
================================================================================
Tenant: Acme Corp Inc
API Key: api_key_12345678-1234-1234-1234-123456789abc
--------------------------------------------------------------------------------
Tenant: Beta Solutions LLC
API Key: api_key_87654321-4321-4321-4321-cba987654321
--------------------------------------------------------------------------------
...
```

### Step 3: Perform Bulk Ingestion

**Now you can ingest all the data for each tenant:**

```bash
python bulk_ingest.py --data-dir generated_data --api-url http://localhost:8000
```

This script will:
- Read all generated data files
- Create upload sessions for each tenant
- Upload all files using the correct API keys
- Provide progress tracking and error reporting

**Options:**
```bash
# Dry run (see what would be uploaded)
python bulk_ingest.py --data-dir generated_data --dry-run

# Process specific tenant only
python bulk_ingest.py --data-dir generated_data --tenant-id <tenant-id>

# Wait for completion and show status
python bulk_ingest.py --data-dir generated_data --wait-for-completion

# Verbose output
python bulk_ingest.py --data-dir generated_data --verbose
```

## Alternative: Manual Ingestion

If you prefer to upload data manually for each tenant:

### 1. Get Tenant API Keys

The API key for each tenant follows the pattern: `api_key_<tenant_id>`

You can extract them from the `tenants.csv` file or use the management command output.

### 2. Upload Data for Each Tenant

For each tenant, upload their data files:

```bash
# Example for one tenant
TENANT_ID="12345678-1234-1234-1234-123456789abc"
API_KEY="api_key_12345678-1234-1234-1234-123456789abc"

# Upload products
curl -X POST http://localhost:8000/api/v1/ingest/orders/ \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: products-$(date +%s)" \
  -F "file=@generated_data/products_${TENANT_ID}_chunk_0000.ndjson"

# Upload customers  
curl -X POST http://localhost:8000/api/v1/ingest/orders/ \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: customers-$(date +%s)" \
  -F "file=@generated_data/customers_${TENANT_ID}.ndjson"

# Upload orders (repeat for each chunk)
curl -X POST http://localhost:8000/api/v1/ingest/orders/ \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: orders-chunk-0000-$(date +%s)" \
  -F "file=@generated_data/orders_${TENANT_ID}_chunk_0000.ndjson"

# Upload order items (repeat for each chunk)
curl -X POST http://localhost:8000/api/v1/ingest/orders/ \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: order-items-chunk-0000-$(date +%s)" \
  -F "file=@generated_data/order_items_${TENANT_ID}_chunk_0000.ndjson"
```

## Monitoring Upload Progress

### Check Upload Status

```bash
# Get upload session status
curl -X GET http://localhost:8000/api/v1/ingest/sessions/<upload_token>/status/ \
  -H "X-API-Key: <api_key>"
```

### Monitor with Celery

```bash
# Start Celery worker (if not already running)
celery -A main worker -l info

# Monitor Celery events
celery -A main events
```

## Troubleshooting

### Common Issues

1. **"API key not found" error**
   - Make sure you've loaded tenants into the database first
   - Verify the API key format: `api_key_<tenant_id>`

2. **"File not found" error**
   - Check that the data files exist in the `generated_data` directory
   - Verify the file paths in the script

3. **"Upload failed" error**
   - Check that the Django server is running
   - Verify that Celery workers are running
   - Check the Django logs for detailed error messages

4. **Memory issues**
   - The bulk ingestion script processes files in chunks
   - If you still have memory issues, reduce the number of concurrent uploads

### Debug Mode

Enable verbose logging:
```bash
python bulk_ingest.py --data-dir generated_data --verbose
```

Check Django logs:
```bash
tail -f logs/django.log
```

## Performance Tips

1. **Use the bulk ingestion script** - It's optimized for performance
2. **Run Celery workers** - Background processing is much faster
3. **Monitor system resources** - Large datasets can be memory-intensive
4. **Use chunked uploads** - The script automatically handles large files

## Expected Results

After successful ingestion, you should have:
- **5 tenants** in the database
- **500,000 products** per tenant (2.5M total)
- **500,000 orders** per tenant (2.5M total)  
- **Order items** for each order
- **Customer data** for each tenant
- **Price history** and **stock events**

## Next Steps

1. **Verify data** - Check that all data was ingested correctly
2. **Run queries** - Test your application with the new data
3. **Monitor performance** - Check database performance with large datasets
4. **Scale up** - Generate larger datasets if needed

## Files Created

This guide created the following files:
- `apps/tenants/management/commands/load_tenants.py` - Management command to load tenants
- `bulk_ingest.py` - Bulk ingestion script
- `INGESTION_GUIDE.md` - This guide

Use these tools to complete your data ingestion workflow!

