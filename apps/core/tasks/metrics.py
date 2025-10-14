from celery import shared_task
from django.utils import timezone
from datetime import timedelta
import logging
import json
import os
from pathlib import Path

logger = logging.getLogger(__name__)


@shared_task
def generate_daily_metrics():
    """
    Generate daily metrics for all tenants.
    Computes and saves pre-aggregated metrics to MetricsPreagg table.
    """
    from apps.tenants.models import Tenant
    from apps.core.sql_utils import SQLOptimizations
    from apps.analytics.models import MetricsPreagg
    from django.utils import timezone
    from datetime import date, timedelta
    
    try:
        tenants = Tenant.objects.all()
        today = date.today()
        yesterday = today - timedelta(days=1)
        
        total_metrics_saved = 0
        
        for tenant in tenants:
            try:
                # Generate tenant-specific metrics
                analytics = SQLOptimizations.get_tenant_order_analytics(tenant.tenant_id, days=1)
                
                # Save metrics to MetricsPreagg table
                for day_data in analytics:
                    order_day = day_data.get('order_day')
                    if not order_day:
                        continue
                        
                    # Parse the order_day (should be a date string)
                    try:
                        if isinstance(order_day, str):
                            order_date = date.fromisoformat(order_day)
                        else:
                            order_date = order_day
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse order_day for tenant {tenant.name}: {order_day}")
                        continue
                    
                    # Create group key for daily metrics
                    group_key = f"day:{order_date.isoformat()}"
                    
                    # Prepare metrics data
                    metrics_data = {
                        'sum_sales': float(day_data.get('daily_revenue', 0)),
                        'num_orders': int(day_data.get('total_orders', 0)),
                        'unique_customers_est': int(day_data.get('unique_customers', 0)),
                        'avg_order_value': float(day_data.get('avg_order_value', 0)),
                        'delivered_orders': int(day_data.get('delivered_orders', 0)),
                        'cancelled_orders': int(day_data.get('cancelled_orders', 0)),
                    }
                    
                    # Upsert metrics record
                    MetricsPreagg.objects.update_or_create(
                        tenant=tenant,
                        group_key=group_key,
                        period_start=order_date,
                        period_end=order_date,
                        defaults={
                            'metrics': metrics_data,
                            'last_updated': timezone.now()
                        }
                    )
                    
                    total_metrics_saved += 1
                
                logger.info(f"Generated and saved metrics for tenant {tenant.name}: {len(analytics)} daily records")
                
            except Exception as e:
                logger.error(f"Failed to generate metrics for tenant {tenant.name}: {str(e)}")
                continue
        
        logger.info(f"Daily metrics generation completed: {total_metrics_saved} records saved across {tenants.count()} tenants")
        return {
            "status": "success",
            "tenants_processed": tenants.count(),
            "metrics_saved": total_metrics_saved,
            "date": today.isoformat()
        }
    except Exception as e:
        logger.error(f"Daily metrics generation failed: {str(e)}")
        raise


@shared_task
def generate_hourly_metrics():
    """
    Generate hourly metrics for all tenants.
    Computes and saves pre-aggregated metrics to MetricsPreagg table for hourly grouping.
    """
    from apps.tenants.models import Tenant
    from apps.core.sql_utils import SQLOptimizations
    from apps.analytics.models import MetricsPreagg
    from django.utils import timezone
    from datetime import datetime, timedelta
    from django.db import connection
    
    try:
        tenants = Tenant.objects.all()
        now = timezone.now()
        one_hour_ago = now - timedelta(hours=1)
        
        total_metrics_saved = 0
        
        for tenant in tenants:
            try:
                # Generate hourly metrics using raw SQL
                with connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT 
                            DATE_TRUNC('hour', o.order_date) AS order_hour,
                            COUNT(*) AS total_orders,
                            SUM(o.total_amount) AS hourly_revenue,
                            AVG(o.total_amount) AS avg_order_value,
                            COUNT(DISTINCT o.customer) AS unique_customers,
                            COUNT(*) FILTER (WHERE o.order_status = 'delivered') AS delivered_orders,
                            COUNT(*) FILTER (WHERE o.order_status = 'cancelled') AS cancelled_orders
                        FROM orders o
                        WHERE o.tenant_id = %s 
                            AND o.order_date >= %s
                            AND o.order_date < %s
                        GROUP BY DATE_TRUNC('hour', o.order_date)
                        ORDER BY order_hour DESC
                    """, [str(tenant.tenant_id), one_hour_ago, now])
                    
                    columns = [col[0] for col in cursor.description]
                    hourly_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                
                # Save metrics to MetricsPreagg table
                for hour_data in hourly_data:
                    order_hour = hour_data.get('order_hour')
                    if not order_hour:
                        continue
                    
                    # Parse the order_hour
                    try:
                        if isinstance(order_hour, str):
                            hour_datetime = datetime.fromisoformat(order_hour.replace('Z', '+00:00'))
                        else:
                            hour_datetime = order_hour
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse order_hour for tenant {tenant.name}: {order_hour}")
                        continue
                    
                    # Create group key for hourly metrics
                    group_key = f"hour:{hour_datetime.strftime('%Y-%m-%dT%H:00:00')}"
                    
                    # Prepare metrics data
                    metrics_data = {
                        'sum_sales': float(hour_data.get('hourly_revenue', 0)),
                        'num_orders': int(hour_data.get('total_orders', 0)),
                        'unique_customers_est': int(hour_data.get('unique_customers', 0)),
                        'avg_order_value': float(hour_data.get('avg_order_value', 0)),
                        'delivered_orders': int(hour_data.get('delivered_orders', 0)),
                        'cancelled_orders': int(hour_data.get('cancelled_orders', 0)),
                    }
                    
                    # Upsert metrics record
                    MetricsPreagg.objects.update_or_create(
                        tenant=tenant,
                        group_key=group_key,
                        period_start=hour_datetime.date(),
                        period_end=hour_datetime.date(),
                        defaults={
                            'metrics': metrics_data,
                            'last_updated': timezone.now()
                        }
                    )
                    
                    total_metrics_saved += 1
                
                logger.info(f"Generated and saved hourly metrics for tenant {tenant.name}: {len(hourly_data)} hourly records")
                
            except Exception as e:
                logger.error(f"Failed to generate hourly metrics for tenant {tenant.name}: {str(e)}")
                continue
        
        logger.info(f"Hourly metrics generation completed: {total_metrics_saved} records saved across {tenants.count()} tenants")
        return {
            "status": "success",
            "tenants_processed": tenants.count(),
            "metrics_saved": total_metrics_saved,
            "period": "hourly",
            "timestamp": now.isoformat()
        }
    except Exception as e:
        logger.error(f"Hourly metrics generation failed: {str(e)}")
        raise

