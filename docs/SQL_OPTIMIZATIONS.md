# SQL Optimizations and Raw SQL Examples

This document demonstrates SQL optimizations implemented in the Django models and provides raw SQL examples for common operations.

## Database Indexes

### Composite Indexes for Multi-Tenant Queries
All models include tenant-specific indexes to optimize multi-tenant queries:

```sql
-- Products by tenant and SKU (most common lookup)
CREATE INDEX products_tenant_sku_idx ON products (tenant_id, sku);

-- Orders by tenant, date, and status (common filtering)
CREATE INDEX orders_tenant_date_status_idx ON orders (tenant_id, order_date DESC, order_status);

-- Stock events by product and time (time-series queries)
CREATE INDEX stock_events_product_time_idx ON stock_events (product_id, event_time DESC);
```

### Covering Indexes for Common Projections
```sql
-- Covering index for order summaries (avoids table lookups)
CREATE INDEX orders_summary_covering_idx ON orders (tenant_id, order_date, order_status, total_amount, currency);

-- Covering index for product listings
CREATE INDEX products_listing_covering_idx ON products (tenant_id, active, sku, name, price);
```

## Raw SQL Examples

### 1. Multi-Tenant Order Analytics
```sql
-- Get order statistics by tenant for the last 30 days
SELECT 
    t.name as tenant_name,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as total_revenue,
    AVG(o.total_amount) as avg_order_value,
    COUNT(DISTINCT o.customer_id) as unique_customers
FROM tenants t
LEFT JOIN orders o ON t.tenant_id = o.tenant_id 
    AND o.order_date >= datetime('now', '-30 days')
GROUP BY t.tenant_id, t.name
ORDER BY total_revenue DESC;
```

### 2. Product Performance Analysis
```sql
-- Top performing products by revenue
SELECT 
    p.sku,
    p.name,
    p.price,
    COUNT(oi.order_item_id) as times_ordered,
    SUM(oi.quantity) as total_quantity_sold,
    SUM(oi.line_total) as total_revenue
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
JOIN orders o ON oi.order_id = o.order_id
WHERE o.order_status IN ('delivered', 'shipped')
    AND o.order_date >= datetime('now', '-90 days')
GROUP BY p.product_id, p.sku, p.name, p.price
ORDER BY total_revenue DESC
LIMIT 20;
```

### 3. Stock Level Monitoring
```sql
-- Products with low stock levels
SELECT 
    p.sku,
    p.name,
    sl.available,
    sl.last_updated,
    CASE 
        WHEN sl.available = 0 THEN 'Out of Stock'
        WHEN sl.available <= 10 THEN 'Low Stock'
        ELSE 'In Stock'
    END as stock_status
FROM products p
JOIN stock_levels sl ON p.product_id = sl.product_id
WHERE p.active = 1
    AND sl.available <= 10
ORDER BY sl.available ASC, p.name;
```

### 4. Price Change Analysis
```sql
-- Products with significant price changes
SELECT 
    p.sku,
    p.name,
    pe.old_price,
    pe.new_price,
    pe.pct_change,
    pe.anomaly_flag,
    pe.received_at
FROM price_events pe
JOIN products p ON pe.product_id = p.product_id
WHERE ABS(pe.pct_change) > 10.0  -- More than 10% change
    AND pe.received_at >= datetime('now', '-7 days')
ORDER BY ABS(pe.pct_change) DESC;
```

### 5. Customer Segmentation
```sql
-- Customer value analysis
SELECT 
    c.customer_id,
    c.name,
    c.email,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as total_spent,
    AVG(o.total_amount) as avg_order_value,
    MAX(o.order_date) as last_order_date,
    CASE 
        WHEN SUM(o.total_amount) >= 1000 THEN 'High Value'
        WHEN SUM(o.total_amount) >= 500 THEN 'Medium Value'
        ELSE 'Low Value'
    END as customer_segment
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_status != 'cancelled' OR o.order_status IS NULL
GROUP BY c.customer_id, c.name, c.email
ORDER BY total_spent DESC;
```

### 6. Inventory Turnover Analysis
```sql
-- Inventory turnover calculation
WITH product_sales AS (
    SELECT 
        p.product_id,
        p.sku,
        p.name,
        SUM(oi.quantity) as total_sold,
        SUM(oi.line_total) as total_revenue
    FROM products p
    JOIN order_items oi ON p.product_id = oi.product_id
    JOIN orders o ON oi.order_id = o.order_id
    WHERE o.order_status IN ('delivered', 'shipped')
        AND o.order_date >= datetime('now', '-90 days')
    GROUP BY p.product_id, p.sku, p.name
),
stock_levels AS (
    SELECT 
        product_id,
        available
    FROM stock_levels
)
SELECT 
    ps.sku,
    ps.name,
    ps.total_sold,
    ps.total_revenue,
    sl.available,
    CASE 
        WHEN sl.available > 0 THEN CAST(ps.total_sold AS FLOAT) / sl.available
        ELSE NULL
    END as turnover_ratio
FROM product_sales ps
LEFT JOIN stock_levels sl ON ps.product_id = sl.product_id
ORDER BY turnover_ratio DESC;
```

### 7. Time-Series Analysis
```sql
-- Daily order trends
SELECT 
    DATE(order_date) as order_day,
    COUNT(*) as order_count,
    SUM(total_amount) as daily_revenue,
    AVG(total_amount) as avg_order_value
FROM orders
WHERE order_date >= datetime('now', '-30 days')
    AND order_status != 'cancelled'
GROUP BY DATE(order_date)
ORDER BY order_day;
```

### 8. Data Quality Checks
```sql
-- Find orders with missing customer information
SELECT 
    o.order_id,
    o.external_order_id,
    o.customer_id,
    o.customer_name_snapshot,
    o.customer_email_snapshot,
    o.order_date
FROM orders o
WHERE (o.customer_id IS NULL AND (o.customer_name_snapshot IS NULL OR o.customer_email_snapshot IS NULL))
    OR (o.customer_id IS NOT NULL AND (o.customer_name_snapshot IS NULL OR o.customer_email_snapshot IS NULL));
```

## Performance Optimization Techniques

### 1. Query Optimization
- Use `EXPLAIN QUERY PLAN` to analyze query performance
- Leverage composite indexes for multi-column WHERE clauses
- Use covering indexes to avoid table lookups
- Implement proper foreign key constraints

### 2. Partitioning Simulation (SQLite Limitations)
While SQLite doesn't support true partitioning, we can simulate it:

```sql
-- Create separate tables for different time periods
CREATE TABLE orders_2024_q1 AS SELECT * FROM orders WHERE order_date < '2024-04-01';
CREATE TABLE orders_2024_q2 AS SELECT * FROM orders WHERE order_date >= '2024-04-01' AND order_date < '2024-07-01';

-- Use UNION for queries across partitions
SELECT * FROM orders_2024_q1 WHERE tenant_id = 'some-uuid'
UNION ALL
SELECT * FROM orders_2024_q2 WHERE tenant_id = 'some-uuid';
```

### 3. Denormalization for Performance
- `StockLevels` table provides fast current stock lookups
- `MetricsPreagg` table stores pre-computed metrics
- Customer snapshots in orders for historical accuracy

### 4. Index Maintenance
```sql
-- Analyze table statistics for query optimizer
ANALYZE;

-- Rebuild indexes if needed (SQLite doesn't support REINDEX on specific indexes)
REINDEX;
```

## Migration Considerations

### From SQLite to PostgreSQL/MySQL

1. **Data Types**:
   - SQLite TEXT → PostgreSQL TEXT/VARCHAR
   - SQLite INTEGER → PostgreSQL BIGINT
   - SQLite REAL → PostgreSQL DECIMAL/NUMERIC

2. **Indexes**:
   - Convert to PostgreSQL GIN indexes for JSON fields
   - Use PostgreSQL partial indexes for better performance
   - Implement PostgreSQL-specific features like arrays

3. **Constraints**:
   - Add CHECK constraints for data validation
   - Use PostgreSQL's advanced constraint features

4. **Performance**:
   - Implement true table partitioning
   - Use PostgreSQL's advanced indexing features
   - Leverage PostgreSQL's parallel query execution
