#!/usr/bin/env python3
"""
Test script for comprehensive data ingestion system.
This script tests the complete ingestion pipeline with proper dependency handling.
"""
import os
import sys
import django
import requests
import time
import json
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'main.settings.dev')
django.setup()

from apps.tenants.models import Tenant
from apps.customers.models import Customer
from apps.products.models import Product
from apps.orders.models import Order, OrderItem
from django.db import connection


def test_comprehensive_ingestion():
    """Test the comprehensive ingestion system."""
    print("=" * 80)
    print("TESTING COMPREHENSIVE DATA INGESTION SYSTEM")
    print("=" * 80)
    
    # Test 1: Check if we have a tenant
    print("\n1. Checking tenant setup...")
    try:
        tenant = Tenant.objects.first()
        if not tenant:
            print("❌ No tenant found. Please run tenant setup first.")
            return False
        print(f"✅ Found tenant: {tenant.name} ({tenant.tenant_id})")
    except Exception as e:
        print(f"❌ Error checking tenant: {e}")
        return False
    
    # Test 2: Check data files
    print("\n2. Checking data files...")
    data_dir = Path("generated_data")
    if not data_dir.exists():
        print("❌ Generated data directory not found")
        return False
    
    # Check for each data type
    data_types = ['customers', 'products', 'orders', 'order_items']
    files_found = {}
    
    for data_type in data_types:
        pattern = f"{data_type}_{tenant.tenant_id}*"
        files = list(data_dir.glob(pattern))
        files_found[data_type] = len(files)
        print(f"   {data_type}: {len(files)} files")
    
    if not any(files_found.values()):
        print("❌ No data files found for tenant")
        return False
    
    print("✅ Data files found")
    
    # Test 3: Test API endpoint
    print("\n3. Testing comprehensive API endpoint...")
    api_url = "http://127.0.0.1:8000/api/v1/ingest/comprehensive/"
    api_key = f"api_key_{tenant.tenant_id}"
    
    # Test with a small customer file first
    customer_files = list(data_dir.glob(f"customers_{tenant.tenant_id}*"))
    if not customer_files:
        print("❌ No customer files found")
        return False
    
    test_file = customer_files[0]
    print(f"   Testing with file: {test_file.name}")
    
    try:
        with open(test_file, 'rb') as f:
            files = {'file': (test_file.name, f, 'application/x-ndjson')}
            headers = {
                'X-API-Key': api_key,
                'Idempotency-Key': f'test_{int(time.time())}'
            }
            
            response = requests.post(api_url, files=files, headers=headers, timeout=30)
            
            if response.status_code == 201:
                result = response.json()
                print(f"✅ API test successful")
                print(f"   Upload ID: {result.get('upload_id')}")
                print(f"   Data Type: {result.get('data_type')}")
                print(f"   Status: {result.get('status')}")
                return True
            else:
                print(f"❌ API test failed: {response.status_code}")
                print(f"   Response: {response.text}")
                return False
                
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to API. Is the server running?")
        return False
    except Exception as e:
        print(f"❌ API test error: {e}")
        return False


def test_database_state():
    """Test the current database state."""
    print("\n" + "=" * 80)
    print("CHECKING DATABASE STATE")
    print("=" * 80)
    
    try:
        # Count records in each table
        tenant_count = Tenant.objects.count()
        customer_count = Customer.objects.count()
        product_count = Product.objects.count()
        order_count = Order.objects.count()
        order_item_count = OrderItem.objects.count()
        
        print(f"Tenants: {tenant_count}")
        print(f"Customers: {customer_count}")
        print(f"Products: {product_count}")
        print(f"Orders: {order_count}")
        print(f"Order Items: {order_item_count}")
        
        # Check for foreign key violations
        print("\nChecking for foreign key violations...")
        
        with connection.cursor() as cursor:
            # Check orders with invalid customer references
            cursor.execute("""
                SELECT COUNT(*) FROM orders o 
                LEFT JOIN customers c ON o.customer_id = c.customer_id 
                WHERE o.customer_id IS NOT NULL AND c.customer_id IS NULL
            """)
            invalid_customer_refs = cursor.fetchone()[0]
            
            # Check order items with invalid product references
            cursor.execute("""
                SELECT COUNT(*) FROM order_items oi 
                LEFT JOIN products p ON oi.product_id = p.product_id 
                WHERE p.product_id IS NULL
            """)
            invalid_product_refs = cursor.fetchone()[0]
            
            # Check order items with invalid order references
            cursor.execute("""
                SELECT COUNT(*) FROM order_items oi 
                LEFT JOIN orders o ON oi.order_id = o.order_id 
                WHERE o.order_id IS NULL
            """)
            invalid_order_refs = cursor.fetchone()[0]
        
        print(f"Orders with invalid customer references: {invalid_customer_refs}")
        print(f"Order items with invalid product references: {invalid_product_refs}")
        print(f"Order items with invalid order references: {invalid_order_refs}")
        
        if invalid_customer_refs > 0 or invalid_product_refs > 0 or invalid_order_refs > 0:
            print("❌ Foreign key violations detected!")
            return False
        else:
            print("✅ No foreign key violations detected")
            return True
            
    except Exception as e:
        print(f"❌ Database check error: {e}")
        return False


def main():
    """Main test function."""
    print("Starting comprehensive ingestion system test...")
    
    # Test 1: API functionality
    api_test_passed = test_comprehensive_ingestion()
    
    # Test 2: Database state
    db_test_passed = test_database_state()
    
    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print(f"API Test: {'✅ PASSED' if api_test_passed else '❌ FAILED'}")
    print(f"Database Test: {'✅ PASSED' if db_test_passed else '❌ FAILED'}")
    
    if api_test_passed and db_test_passed:
        print("\n🎉 All tests passed! The comprehensive ingestion system is working correctly.")
        print("\nNext steps:")
        print("1. Run the bulk ingestion script:")
        print("   python bulk_ingest.py --data-dir generated_data --api-url http://localhost:8000")
        print("2. Monitor the ingestion progress in the logs")
        print("3. Verify data integrity after completion")
    else:
        print("\n❌ Some tests failed. Please check the issues above.")
        return 1
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
