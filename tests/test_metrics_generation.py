#!/usr/bin/env python3
"""
Test script to manually trigger metrics generation and verify data is saved.
"""
import os
import sys
import django

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'main.settings.dev')
django.setup()

from apps.core.tasks.metrics import generate_daily_metrics, generate_hourly_metrics
from apps.analytics.models import MetricsPreagg
from apps.tenants.models import Tenant


def test_metrics_generation():
    """Test the metrics generation tasks."""
    print("Testing metrics generation...")
    
    # Check current metrics count
    initial_count = MetricsPreagg.objects.count()
    print(f"Initial metrics count: {initial_count}")
    
    # Test daily metrics generation
    print("\n--- Testing Daily Metrics Generation ---")
    try:
        result = generate_daily_metrics()
        print(f"Daily metrics result: {result}")
    except Exception as e:
        print(f"Daily metrics failed: {e}")
    
    # Test hourly metrics generation
    print("\n--- Testing Hourly Metrics Generation ---")
    try:
        result = generate_hourly_metrics()
        print(f"Hourly metrics result: {result}")
    except Exception as e:
        print(f"Hourly metrics failed: {e}")
    
    # Check final metrics count
    final_count = MetricsPreagg.objects.count()
    print(f"\nFinal metrics count: {final_count}")
    print(f"Metrics added: {final_count - initial_count}")
    
    # Show sample metrics
    if final_count > initial_count:
        print("\n--- Sample Metrics Records ---")
        recent_metrics = MetricsPreagg.objects.order_by('-last_updated')[:5]
        for metric in recent_metrics:
            print(f"Tenant: {metric.tenant.name}")
            print(f"Group Key: {metric.group_key}")
            print(f"Period: {metric.period_start} to {metric.period_end}")
            print(f"Metrics: {metric.metrics}")
            print(f"Last Updated: {metric.last_updated}")
            print("-" * 50)


if __name__ == "__main__":
    test_metrics_generation()
