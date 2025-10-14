# Multi-Tenant E-Commerce Data Ingestion System

> A high-performance, scalable bulk data ingestion system for multi-tenant e-commerce platforms with real-time analytics, inventory management, and comprehensive API endpoints.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Django 4.2+](https://img.shields.io/badge/django-4.2+-green.svg)](https://www.djangoproject.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Running the Project](#running-the-project)
- [Performance Testing](#performance-testing)
- [Design Decisions](#design-decisions)
- [Optimizations](#optimizations)
- [Production Migration](#production-migration)
- [Hardware Requirements](#hardware-requirements)
- [Documentation](#documentation)
- [API Endpoints](#api-endpoints)
- [Troubleshooting](#troubleshooting)

## Overview

This system provides enterprise-grade bulk data ingestion capabilities for multi-tenant e-commerce platforms. It handles millions of records with features like:

- **Bulk Data Ingestion**: Process large datasets (2M+ orders, 500K+ products per tenant)
- **Real-time Price Sensing**: Anomaly detection and price change monitoring
- **Inventory Management**: Transactional stock updates with conflict resolution
- **Multi-Tenant Isolation**: Complete data separation and security
- **Comprehensive Analytics**: Pre-aggregated metrics and reporting
- **High Performance**: 2000+ rows/second ingestion throughput

## Features

### Core Capabilities

- ✅ **Chunked Uploads**: Handle datasets of any size with automatic chunking
- ✅ **Idempotency**: Safe retry of failed operations without data duplication
- ✅ **Resumable Uploads**: Continue interrupted uploads using upload tokens
- ✅ **Background Processing**: Asynchronous processing with Celery workers
- ✅ **Staging Tables**: Optimized data loading with staging-to-production promotion
- ✅ **Transaction Safety**: Product-level atomicity with row-level locking
- ✅ **Rate Limiting**: Protection against abuse (100 req/min tenant, 10 req/min product)
- ✅ **Anomaly Detection**: Real-time price change detection with streaming responses
- ✅ **Data Export**: Async CSV/JSON export with progress tracking

### Technical Features

- 🔒 **Multi-Tenant Security**: Complete tenant isolation at database level
- 📊 **Pre-aggregated Metrics**: Fast analytics without real-time computation
- 🔄 **Conflict Resolution**: Last-write-wins with product-level locking
- 📈 **Performance Monitoring**: Comprehensive metrics and throughput tracking
- 🚀 **Horizontal Scalability**: Designed for distributed deployment
- 💾 **Multiple Database Support**: SQLite (dev), PostgreSQL/MySQL (prod)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Client Layer                            │
│  (Data Generator, Test Scripts, External APIs)                  │
└─────────────────┬───────────────────────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────────────────────┐
│                      API Gateway (Django)                       │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐ │
│  │ Comprehensive│ Bulk Stock   │ Price Sensing│ Data Export  │ │
│  │ Ingestion    │ Update       │ API          │ API          │ │
│  └──────────────┴──────────────┴──────────────┴──────────────┘ │
└─────────────────┬───────────────────────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────────────────────┐
│                   Message Queue (Redis/Celery)                  │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐ │
│  │ Ingestion    │ Stock Update │ Metrics      │ Export       │ │
│  │ Tasks        │ Tasks        │ Generation   │ Tasks        │ │
│  └──────────────┴──────────────┴──────────────┴──────────────┘ │
└─────────────────┬───────────────────────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────────────────────┐
│                    Processing Layer (Celery Workers)            │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Staging Tables → Validation → Production Tables         │  │
│  │  customers_staging, products_staging, orders_staging     │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────┬───────────────────────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────────────────────┐
│                      Database Layer                             │
│  ┌────────────┬────────────┬────────────┬────────────┐         │
│  │ Multi-     │ Product    │ Inventory  │ Analytics  │         │
│  │ Tenant     │ Catalog    │ Management │ & Metrics  │         │
│  │ Management │            │            │            │         │
│  └────────────┴────────────┴────────────┴────────────┘         │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Python 3.10+
- Redis 7.0+ (for Celery)
- SQLite3 (included) or PostgreSQL 14+/MySQL 8.0+
- Docker & Docker Compose (optional, recommended)

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd Bacnkend Assessment Test

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run migrations
python manage.py migrate

# Create superuser (optional)
python manage.py createsuperuser
```

## Running the Project

### Option 1: Docker Compose (Recommended)

```bash
# Start all services
docker-compose up --build

# Access the application
# API: http://localhost:8000
# Swagger Docs: http://localhost:8000/api/schema/swagger-ui/
# Admin: http://localhost:8000/admin

# Stop services
docker-compose down
```
### Option 2: Local Development with PostgreSQL (Recomended)

```bash
# 1. Install PostgreSQL
sudo apt-get install postgresql postgresql-contrib  # Ubuntu/Debian
brew install postgresql                             # macOS

# 2. Create database and user
psql -U postgres
CREATE DATABASE localDB;
CREATE USER postgres WITH PASSWORD 'mechanlyze123@';
GRANT ALL PRIVILEGES ON DATABASE localDB TO postgres;
\q

# 3. Update environment variables (example for local Docker setup)
export DB_ENGINE="django.db.backends.postgresql"
export DB_NAME="localDB"
export DB_USER="postgres"
export DB_PASSWORD="mechanlyze123@"
export DB_HOST="host.docker.internal"   # or 127.0.0.1 if not using Docker
export DB_PORT="5432"

# 4. Apply database migrations
python manage.py migrate

# 5. Start services (e.g., Django + Celery + Redis)
docker-compose up

### Option 3: Local Development with MySQL/MSSQL

**MySQL:**
```bash
# 1. Install MySQL
sudo apt-get install mysql-server  # Ubuntu/Debian
brew install mysql                 # macOS

# 2. Create database and user
mysql -u root -p
CREATE DATABASE ecommerce_db;
CREATE USER 'ecommerce_user'@'localhost' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON ecommerce_db.* TO 'ecommerce_user'@'localhost';
FLUSH PRIVILEGES;
EXIT;

# 3. Install MySQL client for Django
pip install mysqlclient

# 4. Update environment variables
export DB_ENGINE="django.db.backends.mysql"
export DB_NAME="ecommerce_db"
export DB_USER="ecommerce_user"
export DB_PASSWORD="your_password"
export DB_HOST="127.0.0.1"
export DB_PORT="3306"

# 5. Apply database migrations
python manage.py migrate

```

**MSSQL:**
```bash
# 1. Install MSSQL Django driver
pip install mssql-django

# 2. (Optional) Install MSSQL locally or use a Docker image for MSSQL
# Example:
# docker run -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=YourStrong@Passw0rd' \
#    -p 1433:1433 --name mssql -d mcr.microsoft.com/mssql/server:2022-latest

# 3. Create database manually in MSSQL (e.g., using SQL Server Management Studio or `sqlcmd`)

# 4. Update environment variables
export DB_ENGINE="mssql"
export DB_NAME="ecommerce_db"
export DB_USER="sa"                      # or your created user
export DB_PASSWORD="YourStrong@Passw0rd" # or your password
export DB_HOST="127.0.0.1"
export DB_PORT="1433"

# 5. Apply database migrations
python manage.py migrate

```

## Performance Testing

### 1. Generate Test Data

```bash
# Small dataset (quick test)
python scripts/gen_dataset.py --preset small
# Output: 2 tenants, 1K products, 5K orders

# Medium dataset (realistic test)
python scripts/gen_dataset.py --preset medium
# Output: 5 tenants, 50K products, 100K orders

# Large dataset (stress test)
python scripts/gen_dataset.py --preset large
# Output: 10 tenants, 500K products, 2M orders

# Custom dataset
python scripts/gen_dataset.py \
  --tenants 20 \
  --products-per-tenant 1000000 \
  --orders-per-tenant 5000000 \
  --chunk-size 50000
```

### 2. Load Tenants into Database

```bash
# Load generated tenants
python manage.py load_tenants --file generated_data/tenants.csv

# Note the API keys displayed - you'll need them for testing
```

### 3. Run Comprehensive Ingestion Test

```bash
# Test complete data pipeline
python tests/test_comprehensive_ingestion.py

# Or in Docker
docker-compose exec web python tests/test_comprehensive_ingestion.py
```

### 4. Run Bulk Ingestion

```bash
# Ingest all data for all tenants
python scripts/bulk_ingest.py --data-dir generated_data --api-url http://localhost:8000

# Specific tenant only
python scripts/bulk_ingest.py \
  --data-dir generated_data \
  --tenant-id <tenant-id> \
  --api-url http://localhost:8000

# Dry run (no actual upload)
python scripts/bulk_ingest.py --data-dir generated_data --dry-run

# With progress monitoring
python scripts/bulk_ingest.py \
  --data-dir generated_data \
  --wait-for-completion \
  --verbose
```

### 5. Test Stock Update API

```bash
# Upload stock events for a tenant
python tests/test_stock_upload.py --tenant-id <tenant-id>

# Upload specific file
python tests/test_stock_upload.py \
  --tenant-id <tenant-id> \
  --file generated_data/stock_events_<tenant-id>_chunk_0000.ndjson

# Limit number of files
python tests/test_stock_upload.py --tenant-id <tenant-id> --limit 3
```

### 6. Test Price Sensing API

```bash
# Run price sensing tests
python tests/test_price_sensing.py

# Manual test
curl -X POST \
  'http://localhost:8000/api/v1/tenants/<tenant-id>/products/<product-id>/price-event/' \
  -H 'X-API-Key: <api-key>' \
  -H 'Idempotency-Key: test-123' \
  -H 'Content-Type: application/json' \
  -d '{
    "old_price": 100.0,
    "new_price": 150.0,
    "source": "api",
    "metadata": {"reason": "promotion"}
  }'
```

### 7. Monitor Performance

```bash
# Watch Celery worker logs
docker-compose logs -f celery_worker

# Watch Django logs
docker-compose logs -f web

# Monitor Redis
redis-cli MONITOR

# Check database stats
python manage.py dbshell
```

### 8. Benchmark Results

Expected performance metrics (hardware dependent):

| Operation | Throughput | Latency (p95) | Notes |
|-----------|------------|---------------|-------|
| Order Ingestion | 2000-5000 rows/sec | 50ms | With staging tables |
| Stock Updates | 500-1000 events/sec | 25ms | With row-level locking |
| Price Events | 100 req/sec | 10ms | Per tenant rate limit |
| Data Export | 10K-50K rows/sec | N/A | Streaming response |
| Metrics Generation | 1M rows/min | N/A | Background task |

## Design Decisions

### 1. Multi-Tenant Architecture

**Decision**: Shared database with tenant_id filtering (Row-Level Security)

**Rationale**:
- ✅ Cost-effective: Single database instance
- ✅ Easier maintenance: Single schema version
- ✅ Better resource utilization: Shared connection pool
- ✅ Simpler backup/restore: One database to manage
- ⚠️ Requires strict query filtering
- ⚠️ Potential noisy neighbor issues (mitigated with rate limiting)

**Alternative Considered**: Separate database per tenant
- ❌ Higher cost: Multiple database instances
- ❌ Complex migrations: Must migrate all tenant databases
- ✅ Better isolation: Complete data separation
- ✅ Easier to scale specific tenants

### 2. Staging Tables Pattern

**Decision**: Use staging tables for bulk operations

**Rationale**:
- ✅ **Performance**: Bulk inserts without constraints/indexes
- ✅ **Validation**: Validate before affecting production data
- ✅ **Rollback**: Easy to discard bad data
- ✅ **Partial Success**: Some data can succeed while others fail
- ✅ **Zero Downtime**: Production tables remain available

**Implementation**:
```sql
-- Staging table (no constraints, indexes)
CREATE TABLE customers_staging (
    customer_id TEXT,
    tenant_id TEXT,
    name TEXT,
    email TEXT,
    ...
);

-- Fast bulk insert
INSERT INTO customers_staging VALUES (...);

-- Validate and promote
INSERT INTO customers SELECT * FROM customers_staging WHERE valid;
```

### 3. Background Processing (Celery)

**Decision**: Asynchronous processing with Celery

**Rationale**:
- ✅ **Scalability**: Horizontal scaling with more workers
- ✅ **Resilience**: Automatic retry on failures
- ✅ **Monitoring**: Task status and progress tracking
- ✅ **Priority**: Queue prioritization for urgent tasks
- ✅ **Timeout Management**: Prevents long-running requests

**Task Distribution**:
- **High Priority**: Price events, stock updates
- **Normal Priority**: Data ingestion, exports
- **Low Priority**: Metrics generation, cleanup

### 4. Idempotency Keys

**Decision**: SHA256 hash-based idempotency

**Rationale**:
- ✅ **Webhook Support**: Safe retries from external systems
- ✅ **Duplicate Prevention**: Same request = same result
- ✅ **Response Caching**: Avoid reprocessing
- ✅ **24-hour TTL**: Balance between safety and storage

**Implementation**:
```python
request_hash = hashlib.sha256(request.body).hexdigest()
key, created = IdempotencyKey.objects.get_or_create(
    tenant=tenant,
    idempotency_key=idempotency_key,
    defaults={'request_hash': request_hash}
)
```

### 5. Product-Level Atomicity

**Decision**: Per-product transactions for stock updates

**Rationale**:
- ✅ **Granular Control**: Products succeed independently
- ✅ **Better Concurrency**: Locks only affected products
- ✅ **Partial Success**: Report per-product status
- ⚠️ **More Complex**: Requires careful error handling

**Alternative**: Single transaction for entire batch
- ❌ All-or-nothing: One failure aborts everything
- ❌ Poor concurrency: Large locks
- ✅ Simpler error handling

### 6. NDJSON Format

**Decision**: Newline Delimited JSON for bulk data

**Rationale**:
- ✅ **Streaming**: Process line-by-line, low memory
- ✅ **Partial Recovery**: Skip bad lines, continue processing
- ✅ **Human Readable**: Easy to debug and inspect
- ✅ **Widely Supported**: Standard format for big data

**Alternative**: CSV
- ✅ Smaller file size
- ❌ No nested structures
- ❌ Type ambiguity (everything is string)

### 7. UUID Primary Keys

**Decision**: UUID v4 for all primary keys

**Rationale**:
- ✅ **Distributed Systems**: Generate IDs anywhere
- ✅ **No Collisions**: Globally unique
- ✅ **Security**: Non-sequential IDs
- ✅ **Merge-Friendly**: Easy to merge datasets
- ⚠️ **Larger Storage**: 36 bytes vs 8 bytes
- ⚠️ **Slower Joins**: Compared to integers

### 8. Streaming Responses

**Decision**: NDJSON streaming for large result sets

**Rationale**:
- ✅ **Low Memory**: Constant memory usage
- ✅ **Early Results**: Client sees data immediately
- ✅ **Large Datasets**: No size limit
- ✅ **Real-time**: Monitor as data arrives

## Optimizations

### 1. Database Optimizations

#### Composite Indexes
```sql
-- Multi-column indexes for common queries
CREATE INDEX orders_tenant_date_status_idx 
ON orders (tenant_id, order_date DESC, order_status);

-- Benefit: Single index serves multiple query patterns
-- Impact: 10x faster filtered queries
```

#### Covering Indexes
```sql
-- Include frequently accessed columns
CREATE INDEX orders_summary_covering_idx 
ON orders (tenant_id, order_date, order_status) 
INCLUDE (total_amount, currency);

-- Benefit: Avoid table lookups (index-only scan)
-- Impact: 5x faster summary queries
```

#### Partial Indexes
```sql
-- Index only relevant subset
CREATE INDEX products_active_idx 
ON products (tenant_id, sku) 
WHERE active = TRUE;

-- Benefit: Smaller index, faster updates
-- Impact: 50% index size reduction
```

### 2. Query Optimizations

#### Batch Processing
```python
# Before: Row-by-row (slow)
for item in items:
    Order.objects.create(**item)

# After: Bulk insert (fast)
Order.objects.bulk_create([Order(**item) for item in items], batch_size=1000)

# Impact: 100x faster for large batches
```

#### Select Related / Prefetch Related
```python
# Before: N+1 queries
orders = Order.objects.filter(tenant_id=tenant_id)
for order in orders:
    print(order.customer.name)  # Query per order

# After: 2 queries total
orders = Order.objects.filter(tenant_id=tenant_id).select_related('customer')
for order in orders:
    print(order.customer.name)  # No extra query

# Impact: 50x faster for related data
```

#### Raw SQL for Complex Operations
```python
# When ORM is inefficient, use raw SQL
from django.db import connection

with connection.cursor() as cursor:
    cursor.execute("""
        INSERT INTO customers 
        SELECT * FROM customers_staging 
        WHERE is_valid(customer_id)
        ON CONFLICT (tenant_id, email) DO NOTHING
    """)

# Impact: 10x faster than ORM for bulk operations
```

### 3. Caching Strategy

#### Rate Limiting Cache
```python
# Redis-backed rate limiting
cache_key = f"rate_limit:tenant:{tenant_id}"
count = cache.get(cache_key, 0)
if count >= RATE_LIMIT:
    raise RateLimitExceeded
cache.set(cache_key, count + 1, timeout=60)

# Impact: O(1) rate limit checks
```

#### Query Result Cache
```python
# Cache expensive queries
cache_key = f"metrics:tenant:{tenant_id}:daily"
result = cache.get(cache_key)
if not result:
    result = calculate_metrics(tenant_id)
    cache.set(cache_key, result, timeout=3600)

# Impact: 1000x faster for repeated queries
```


### 4. Memory Optimization

#### Streaming Iterators
```python
# Before: Load everything into memory
orders = Order.objects.filter(tenant_id=tenant_id)  # Loads all
for order in orders:
    process(order)

# After: Stream from database
orders = Order.objects.filter(tenant_id=tenant_id).iterator(chunk_size=1000)
for order in orders:
    process(order)

# Impact: Constant memory usage vs O(n) memory
```

#### Generator-based Processing
```python
# Generate data on-the-fly
def generate_export_rows():
    queryset = Order.objects.filter(...).iterator()
    for order in queryset:
        yield format_row(order)

# Stream to response
response = StreamingHttpResponse(
    generate_export_rows(),
    content_type='text/csv'
)

# Impact: Can export millions of rows with <100MB memory
```

## Production Migration

### Phase 1: Pre-Migration (1-2 weeks)

#### 1. Database Selection: PostgreSQL (Recommended)

**Why PostgreSQL over MySQL?**

| Feature | PostgreSQL | MySQL | Winner |
|---------|------------|-------|--------|
| JSONB Support | ✅ Native, indexed | ⚠️ JSON (no indexing) | PostgreSQL |
| Full-Text Search | ✅ Built-in | ⚠️ Limited | PostgreSQL |
| Concurrent Writes | ✅ MVCC | ⚠️ Table locking | PostgreSQL |
| Partitioning | ✅ Native, flexible | ✅ Native | Tie |
| Replication | ✅ Built-in | ✅ Built-in | Tie |
| Performance | ✅ Complex queries | ✅ Simple queries | Context-dependent |
| Extensions | ✅ Rich ecosystem | ⚠️ Limited | PostgreSQL |

**Recommendation**: PostgreSQL 14+ for production

#### 2. Schema Migration Script

```sql
-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

-- Create tenants table
CREATE TABLE tenants (
    tenant_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    api_key_hash VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create products table with partitioning
CREATE TABLE products (
    product_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    sku VARCHAR(100) NOT NULL,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(12,2) NOT NULL CHECK (price >= 0),
    category_id UUID,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_tenant_sku UNIQUE (tenant_id, sku)
) PARTITION BY HASH (tenant_id);

-- Create 8 partitions for products (shard by tenant)
CREATE TABLE products_p0 PARTITION OF products FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE products_p1 PARTITION OF products FOR VALUES WITH (MODULUS 8, REMAINDER 1);
-- ... up to p7

-- Create orders table with time-based partitioning
CREATE TABLE orders (
    order_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    external_order_id VARCHAR(100) NOT NULL,
    customer_id UUID REFERENCES customers(customer_id) ON DELETE SET NULL,
    customer_name_snapshot VARCHAR(255),
    customer_email_snapshot VARCHAR(255),
    total_amount DECIMAL(12,2) NOT NULL CHECK (total_amount >= 0),
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    order_status VARCHAR(20) NOT NULL DEFAULT 'pending',
    order_date TIMESTAMPTZ NOT NULL,
    raw_payload JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_tenant_external_order UNIQUE (tenant_id, external_order_id)
) PARTITION BY RANGE (order_date);

-- Create monthly partitions for orders (last 2 years + future)
CREATE TABLE orders_2024_01 PARTITION OF orders
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
-- ... create partitions for each month

-- Create indexes
CREATE INDEX orders_tenant_date_status_idx ON orders (tenant_id, order_date DESC, order_status);
CREATE INDEX orders_customer_idx ON orders (customer_id) WHERE customer_id IS NOT NULL;
CREATE INDEX orders_raw_payload_gin_idx ON orders USING GIN (raw_payload);

-- Create materialized views for analytics
CREATE MATERIALIZED VIEW tenant_daily_metrics AS
SELECT 
    tenant_id,
    DATE(order_date) as metric_date,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    COUNT(DISTINCT customer_id) as unique_customers
FROM orders
WHERE order_status IN ('delivered', 'shipped')
GROUP BY tenant_id, DATE(order_date);

CREATE UNIQUE INDEX tenant_daily_metrics_uidx ON tenant_daily_metrics (tenant_id, metric_date);
```

#### 3. Data Migration Strategy

**A. Export from SQLite**
```python
# export_sqlite.py
import sqlite3
import json
from pathlib import Path

def export_table(db_path, table_name, output_dir):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(f"SELECT * FROM {table_name}")
    
    output_file = Path(output_dir) / f"{table_name}.ndjson"
    with open(output_file, 'w') as f:
        for row in cursor:
            f.write(json.dumps(dict(row)) + '\n')
    
    conn.close()
    print(f"Exported {table_name} to {output_file}")

# Export all tables
tables = ['tenants', 'products', 'customers', 'orders', 'order_items']
for table in tables:
    export_table('db.sqlite3', table, 'export/')
```

**B. Import to PostgreSQL**
```python
# import_postgres.py
import psycopg2
import json
from psycopg2.extras import execute_values

def import_table(conn, table_name, file_path):
    with open(file_path) as f:
        rows = [json.loads(line) for line in f]
    
    if not rows:
        return
    
    columns = list(rows[0].keys())
    values = [tuple(row[col] for col in columns) for row in rows]
    
    cursor = conn.cursor()
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
    execute_values(cursor, query, values, page_size=1000)
    conn.commit()
    print(f"Imported {len(rows)} rows into {table_name}")

# Import in dependency order
conn = psycopg2.connect("postgresql://user:pass@localhost/db")
tables = ['tenants', 'products', 'customers', 'orders', 'order_items']
for table in tables:
    import_table(conn, table, f'export/{table}.ndjson')
conn.close()
```
