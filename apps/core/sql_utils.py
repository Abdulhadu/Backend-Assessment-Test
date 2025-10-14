"""
Raw SQL utilities and optimizations for the Django project.
This module contains optimized SQL queries and database utilities for PostgreSQL.
"""
import uuid
from django.db import connection
from typing import List, Dict, Any


class SQLOptimizations:
    """Collection of optimized SQL queries and database utilities."""

    @staticmethod
    def get_tenant_order_analytics(tenant_id: uuid.UUID, days: int = 30) -> List[Dict[str, Any]]:
        """
        Get comprehensive order analytics for a tenant using raw SQL.
        Optimized for PostgreSQL with proper date functions.
        """
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    DATE(o.order_date) AS order_day,
                    COUNT(*) AS total_orders,
                    SUM(o.total_amount) AS daily_revenue,
                    AVG(o.total_amount) AS avg_order_value,
                    COUNT(DISTINCT o.customer) AS unique_customers,
                    COUNT(*) FILTER (WHERE o.order_status = 'delivered') AS delivered_orders,
                    COUNT(*) FILTER (WHERE o.order_status = 'cancelled') AS cancelled_orders
                FROM orders o
                WHERE o.tenant_id = %s 
                    AND o.order_date >= NOW() - INTERVAL '%s days'
                GROUP BY DATE(o.order_date)
                ORDER BY order_day DESC
            """, [str(tenant_id), days])

            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    @staticmethod
    def get_top_performing_products(tenant_id: uuid.UUID, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Get top performing products by revenue using optimized PostgreSQL syntax.
        """
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    p.sku,
                    p.name,
                    p.price,
                    COUNT(oi.order_item_id) AS times_ordered,
                    SUM(oi.quantity) AS total_quantity_sold,
                    SUM(oi.line_total) AS total_revenue,
                    AVG(oi.unit_price) AS avg_selling_price
                FROM products p
                JOIN order_items oi ON p.product_id = oi.product
                JOIN orders o ON oi.order = o.order_id
                WHERE p.tenant_id = %s
                    AND o.order_status IN ('delivered', 'shipped')
                    AND o.order_date >= NOW() - INTERVAL '90 days'
                GROUP BY p.product_id, p.sku, p.name, p.price
                ORDER BY total_revenue DESC
                LIMIT %s
            """, [str(tenant_id), limit])

            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    @staticmethod
    def get_low_stock_products(tenant_id: uuid.UUID, threshold: int = 10) -> List[Dict[str, Any]]:
        """
        Get products with low stock levels using PostgreSQL date arithmetic.
        Assumes a stock_levels table exists.
        """
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    p.sku,
                    p.name,
                    sl.available,
                    sl.last_updated,
                    CASE 
                        WHEN sl.available = 0 THEN 'Out of Stock'
                        WHEN sl.available <= %s THEN 'Low Stock'
                        ELSE 'In Stock'
                    END AS stock_status,
                    EXTRACT(DAY FROM (NOW() - sl.last_updated))::INTEGER AS days_since_update
                FROM products p
                JOIN stock_levels sl ON p.product_id = sl.product_id
                WHERE p.tenant_id = %s
                    AND p.active = TRUE
                    AND sl.available <= %s
                ORDER BY sl.available ASC, p.name
            """, [threshold, str(tenant_id), threshold])

            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    @staticmethod
    def get_price_change_analysis(tenant_id: uuid.UUID, days: int = 30) -> List[Dict[str, Any]]:
        """
        Analyze price changes and anomalies using PostgreSQL syntax.
        """
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    p.sku,
                    p.name,
                    pe.old_price,
                    pe.new_price,
                    pe.pct_change,
                    pe.anomaly_flag,
                    pe.received_at,
                    EXTRACT(DAY FROM (NOW() - pe.received_at))::INTEGER AS days_since_change
                FROM price_events pe
                JOIN products p ON pe.product_id = p.product_id
                WHERE pe.tenant_id = %s
                    AND pe.received_at >= NOW() - INTERVAL '%s days'
                ORDER BY ABS(pe.pct_change) DESC
            """, [str(tenant_id), days])

            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    @staticmethod
    def get_inventory_turnover_analysis(tenant_id: uuid.UUID) -> List[Dict[str, Any]]:
        """
        Calculate inventory turnover ratios using PostgreSQL CTE.
        """
        with connection.cursor() as cursor:
            cursor.execute("""
                WITH product_sales AS (
                    SELECT 
                        p.product_id,
                        p.sku,
                        p.name,
                        SUM(oi.quantity) AS total_sold,
                        SUM(oi.line_total) AS total_revenue,
                        COUNT(DISTINCT o.order_id) AS order_count
                    FROM products p
                    JOIN order_items oi ON p.product_id = oi.product
                    JOIN orders o ON oi.order = o.order_id
                    WHERE p.tenant_id = %s
                        AND o.order_status IN ('delivered', 'shipped')
                        AND o.order_date >= NOW() - INTERVAL '90 days'
                    GROUP BY p.product_id, p.sku, p.name
                )
                SELECT 
                    ps.sku,
                    ps.name,
                    ps.total_sold,
                    ps.total_revenue,
                    ps.order_count,
                    sl.available,
                    CASE 
                        WHEN sl.available > 0 THEN ps.total_sold::FLOAT / sl.available
                        ELSE NULL
                    END AS turnover_ratio,
                    CASE 
                        WHEN sl.available > 0 AND ps.total_sold::FLOAT / sl.available > 2.0 THEN 'High Turnover'
                        WHEN sl.available > 0 AND ps.total_sold::FLOAT / sl.available > 1.0 THEN 'Medium Turnover'
                        WHEN sl.available > 0 AND ps.total_sold::FLOAT / sl.available > 0.5 THEN 'Low Turnover'
                        ELSE 'Slow Moving'
                    END AS turnover_category
                FROM product_sales ps
                LEFT JOIN stock_levels sl ON ps.product_id = sl.product_id
                ORDER BY turnover_ratio DESC NULLS LAST
            """, [str(tenant_id)])

            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]


class DatabaseMaintenance:
    """Database maintenance utilities for PostgreSQL."""

    @staticmethod
    def optimize_database():
        """
        Perform PostgreSQL maintenance operations.
        """
        with connection.cursor() as cursor:
            cursor.execute("ANALYZE;")
            cursor.execute("REINDEX DATABASE current_database();")
            cursor.execute("VACUUM;")

    @staticmethod
    def cleanup_expired_data(days: int = 90):
        """
        Clean up expired data to maintain performance.
        """
        with connection.cursor() as cursor:
            cursor.execute("""
                DELETE FROM idempotency_keys 
                WHERE expires_at < NOW();
            """)
            expired_keys = cursor.rowcount

            cursor.execute("""
                DELETE FROM audit_logs 
                WHERE created_at < NOW() - INTERVAL '%s days';
            """, [days])
            expired_logs = cursor.rowcount

        return {
            'expired_idempotency_keys': expired_keys,
            'expired_audit_logs': expired_logs
        }
