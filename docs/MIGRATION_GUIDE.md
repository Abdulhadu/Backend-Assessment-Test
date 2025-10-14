# Database Migration Guide: SQLite to PostgreSQL/MySQL

This document outlines the migration strategy from SQLite to production databases (PostgreSQL/MySQL) and provides production-scale optimizations.

## Current SQLite Implementation

The current implementation uses SQLite with the following characteristics:
- Multi-tenant architecture with tenant isolation
- Comprehensive indexing strategy
- JSON fields for flexible metadata storage
- UUID primary keys for distributed systems
- Audit logging for compliance

## Migration Strategy

### Phase 1: Schema Migration

#### 1.1 Data Type Mapping

| SQLite Type | PostgreSQL | MySQL | Notes |
|-------------|------------|-------|-------|
| TEXT | TEXT/VARCHAR(255) | VARCHAR(255) | Use VARCHAR with appropriate length |
| INTEGER | BIGINT | BIGINT | For UUIDs and large numbers |
| REAL | DECIMAL(10,2) | DECIMAL(10,2) | For monetary values |
| JSON | JSONB | JSON | PostgreSQL JSONB preferred for indexing |

#### 1.2 Schema Conversion Script

```sql
-- PostgreSQL Migration Script
-- Convert SQLite schema to PostgreSQL

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create tenants table
CREATE TABLE tenants (
    tenant_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    api_key_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create products table with proper constraints
CREATE TABLE products (
    product_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    sku VARCHAR(100) NOT NULL,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) NOT NULL CHECK (price > 0),
    category_id UUID,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_tenant_sku UNIQUE (tenant_id, sku)
);

-- Create customers table with full-text search
CREATE TABLE customers (
    customer_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    metadata JSONB DEFAULT '{}',
    search_vector TSVECTOR,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_tenant_customer_email UNIQUE (tenant_id, email)
);

-- Create orders table with proper indexing
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
    CONSTRAINT unique_tenant_external_order UNIQUE (tenant_id, external_order_id),
    CONSTRAINT valid_currency CHECK (currency IN ('USD', 'EUR', 'GBP', 'CAD')),
    CONSTRAINT valid_order_status CHECK (order_status IN ('pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded'))
);
```

#### 1.3 Advanced PostgreSQL Features

```sql
-- Create GIN indexes for JSONB fields
CREATE INDEX customers_metadata_gin_idx ON customers USING GIN (metadata);
CREATE INDEX orders_raw_payload_gin_idx ON orders USING GIN (raw_payload);

-- Create full-text search index
CREATE INDEX customers_search_vector_gin_idx ON customers USING GIN (search_vector);

-- Create partial indexes for better performance
CREATE INDEX products_active_idx ON products (tenant_id, sku) WHERE active = TRUE;
CREATE INDEX orders_recent_idx ON orders (tenant_id, order_date DESC) WHERE order_date >= NOW() - INTERVAL '1 year';

-- Create covering indexes
CREATE INDEX orders_summary_covering_idx ON orders (tenant_id, order_date, order_status) INCLUDE (total_amount, currency);
```

### Phase 2: Data Migration

#### 2.1 Data Export from SQLite

```python
# Python script to export data from SQLite
import sqlite3
import json
import uuid
from datetime import datetime

def export_sqlite_data(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    
    tables = [
        'tenants', 'products', 'customers', 'orders', 
        'order_items', 'stock_events', 'stock_levels',
        'price_history', 'price_events', 'idempotency_keys',
        'ingest_uploads', 'order_ingest_chunks',
        'export_jobs', 'export_chunks', 'metrics_preagg', 'audit_logs'
    ]
    
    data = {}
    for table in tables:
        cursor = conn.execute(f"SELECT * FROM {table}")
        data[table] = [dict(row) for row in cursor.fetchall()]
    
    conn.close()
    return data
```

#### 2.2 Data Import to PostgreSQL

```python
# Python script to import data to PostgreSQL
import psycopg2
from psycopg2.extras import execute_values
import json

def import_to_postgresql(data, connection_params):
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    
    # Import in dependency order
    import_order = [
        'tenants', 'products', 'customers', 'orders',
        'order_items', 'stock_events', 'stock_levels',
        'price_history', 'price_events', 'idempotency_keys',
        'ingest_uploads', 'order_ingest_chunks',
        'export_jobs', 'export_chunks', 'metrics_preagg', 'audit_logs'
    ]
    
    for table in import_order:
        if table in data and data[table]:
            # Convert data types and handle special cases
            processed_data = process_table_data(table, data[table])
            
            # Bulk insert
            columns = list(processed_data[0].keys())
            values = [tuple(row[col] for col in columns) for row in processed_data]
            
            query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s"
            execute_values(cursor, query, values)
    
    conn.commit()
    conn.close()
```

### Phase 3: Production Optimizations

#### 3.1 PostgreSQL Production Configuration

```sql
-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Configure connection pooling
-- Use pgbouncer for connection management

-- Set up replication
-- Configure streaming replication for high availability

-- Configure partitioning for large tables
-- Partition orders table by date
CREATE TABLE orders_y2024m01 PARTITION OF orders
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE orders_y2024m02 PARTITION OF orders
FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

-- Create partitioned indexes
CREATE INDEX orders_partitioned_idx ON orders (tenant_id, order_date, order_status);
```

#### 3.2 MySQL Production Configuration

```sql
-- MySQL 8.0+ configuration
-- Enable JSON functions and indexing
-- Configure InnoDB for better performance

-- Create partitioned tables
CREATE TABLE orders (
    order_id CHAR(36) PRIMARY KEY,
    tenant_id CHAR(36) NOT NULL,
    external_order_id VARCHAR(100) NOT NULL,
    customer_id CHAR(36),
    customer_name_snapshot VARCHAR(255),
    customer_email_snapshot VARCHAR(255),
    total_amount DECIMAL(12,2) NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    order_status VARCHAR(20) NOT NULL DEFAULT 'pending',
    order_date DATETIME NOT NULL,
    raw_payload JSON,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_tenant_external_order (tenant_id, external_order_id),
    KEY idx_tenant_date_status (tenant_id, order_date, order_status),
    KEY idx_customer (customer_id),
    KEY idx_order_date (order_date)
) PARTITION BY RANGE (YEAR(order_date)) (
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p2025 VALUES LESS THAN (2026),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- Create JSON indexes (MySQL 8.0+)
CREATE INDEX idx_orders_raw_payload ON orders ((CAST(raw_payload->'$.source' AS CHAR(50))));
```

#### 3.3 Performance Monitoring

```sql
-- PostgreSQL performance monitoring
-- Query performance analysis
SELECT 
    query,
    calls,
    total_time,
    mean_time,
    rows
FROM pg_stat_statements 
ORDER BY total_time DESC 
LIMIT 10;

-- Index usage analysis
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes 
ORDER BY idx_scan DESC;

-- Table size analysis
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

### Phase 4: Production Scale Considerations

#### 4.1 Horizontal Scaling

1. **Read Replicas**: Set up read replicas for reporting and analytics
2. **Sharding**: Implement tenant-based sharding for large datasets
3. **Caching**: Use Redis for frequently accessed data
4. **CDN**: Implement CDN for static assets

#### 4.2 Vertical Scaling

1. **Connection Pooling**: Use pgbouncer or similar
2. **Query Optimization**: Implement query result caching
3. **Index Optimization**: Regular index maintenance and analysis
4. **Archival Strategy**: Move old data to cold storage

#### 4.3 Backup and Recovery

```bash
# PostgreSQL backup strategy
# Full backup
pg_dump -h localhost -U username -d database_name > full_backup.sql

# Incremental backup using WAL archiving
# Configure wal_level = replica
# Set up continuous archiving

# Point-in-time recovery
pg_basebackup -h localhost -U username -D /backup/location -Ft -z -P
```

#### 4.4 Security Considerations

1. **Encryption**: Enable SSL/TLS for database connections
2. **Access Control**: Implement role-based access control
3. **Audit Logging**: Enable comprehensive audit logging
4. **Data Masking**: Implement data masking for non-production environments

### Phase 5: Migration Checklist

#### Pre-Migration
- [ ] Backup current SQLite database
- [ ] Test migration on staging environment
- [ ] Validate data integrity
- [ ] Performance testing
- [ ] Rollback plan preparation

#### Migration Day
- [ ] Put application in maintenance mode
- [ ] Final data export
- [ ] Schema creation in target database
- [ ] Data import
- [ ] Index creation
- [ ] Data validation
- [ ] Application configuration update
- [ ] Smoke testing
- [ ] Go live

#### Post-Migration
- [ ] Monitor performance metrics
- [ ] Validate application functionality
- [ ] Update monitoring and alerting
- [ ] Document lessons learned
- [ ] Plan for future optimizations

## Performance Benchmarks

### Expected Performance Improvements

| Operation | SQLite | PostgreSQL | MySQL |
|-----------|--------|------------|-------|
| Concurrent Users | 1-10 | 100-1000+ | 100-1000+ |
| Query Performance | Good | Excellent | Good |
| JSON Operations | Limited | Excellent | Good |
| Full-text Search | Basic | Excellent | Good |
| Partitioning | Simulated | Native | Native |
| Replication | None | Native | Native |

### Monitoring Metrics

1. **Query Performance**: Average query execution time
2. **Connection Usage**: Active connections vs. pool size
3. **Index Usage**: Index hit ratio and efficiency
4. **Storage Growth**: Database size and growth rate
5. **Error Rates**: Database error frequency

## Conclusion

This migration strategy provides a comprehensive approach to moving from SQLite to production databases while maintaining data integrity and improving performance. The key is to plan thoroughly, test extensively, and monitor continuously throughout the migration process.
