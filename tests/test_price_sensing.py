#!/usr/bin/env python3
"""
Test script for price-sensing API endpoints.
"""
import requests
import json
import uuid
from datetime import datetime

# Configuration
BASE_URL = "http://127.0.0.1:8000"
TENANT_ID = "51e675b6-187b-4df0-9d87-5dd041ccafb5"  # Replace with actual tenant ID
PRODUCT_ID = "00007d36-82cb-4451-9b0b-1ae80fc7e30d"  # Replace with actual product ID
API_KEY = "df77af68dd9bf2e229d0f234bb430fb5286b6d10361a731d4219a8c1a503fc02"  # Replace with actual API key

def test_price_event():
    """Test price event submission."""
    url = f"{BASE_URL}/api/v1/tenants/{TENANT_ID}/products/{PRODUCT_ID}/price-event/"
    
    headers = {
        "X-API-Key": API_KEY,
        "Idempotency-Key": str(uuid.uuid4()),
        "Content-Type": "application/json"
    }
    
    # Test normal price change
    data = {
        "old_price": 100.0,
        "new_price": 105.0,
        "source": "manual",
        "metadata": {
            "user_id": "user123",
            "reason": "regular_update"
        }
    }
    
    print("Testing normal price change...")
    response = requests.post(url, headers=headers, json=data)
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}")
    
    # Test anomaly (large price increase)
    headers["Idempotency-Key"] = str(uuid.uuid4())
    data = {
        "old_price": 100.0,
        "new_price": 150.0,  # 50% increase - should trigger anomaly
        "source": "api",
        "metadata": {
            "user_id": "user456",
            "reason": "bulk_update"
        }
    }
    
    print("\nTesting anomaly detection...")
    response = requests.post(url, headers=headers, json=data)
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}")
    
    # Test idempotency
    print("\nTesting idempotency...")
    response = requests.post(url, headers=headers, json=data)  # Same request
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}")


def test_price_anomalies():
    """Test price anomalies streaming."""
    url = f"{BASE_URL}/api/v1/tenants/{TENANT_ID}/products/{PRODUCT_ID}/price-anomalies/"
    
    headers = {
        "X-API-Key": API_KEY
    }
    
    params = {
        "hours": 24,
        "limit": 10
    }
    
    print("Testing price anomalies streaming...")
    response = requests.get(url, headers=headers, params=params, stream=True)
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        print("Streaming anomalies:")
        for line in response.iter_lines():
            if line:
                try:
                    data = json.loads(line.decode())
                    if '_meta' in data:
                        print(f"Summary: {data}")
                    else:
                        print(f"Anomaly: {data}")
                except json.JSONDecodeError:
                    print(f"Raw line: {line.decode()}")
    else:
        print(f"Error: {response.text}")


def test_rate_limiting():
    """Test rate limiting by sending multiple requests quickly."""
    url = f"{BASE_URL}/api/v1/tenants/{TENANT_ID}/products/{PRODUCT_ID}/price-event/"
    
    headers = {
        "X-API-Key": API_KEY,
        "Content-Type": "application/json"
    }
    
    data = {
        "old_price": 100.0,
        "new_price": 101.0,
        "source": "test"
    }
    
    print("Testing rate limiting...")
    for i in range(15):  # Send 15 requests quickly
        headers["Idempotency-Key"] = str(uuid.uuid4())
        response = requests.post(url, headers=headers, json=data)
        print(f"Request {i+1}: Status {response.status_code}")
        
        if response.status_code == 429:
            print(f"Rate limited: {response.json()}")
            break


if __name__ == "__main__":
    print("=== Price-Sensing API Test ===")
    print(f"Base URL: {BASE_URL}")
    print(f"Tenant ID: {TENANT_ID}")
    print(f"Product ID: {PRODUCT_ID}")
    print()
    
    try:
        test_price_event()
        print("\n" + "="*50 + "\n")
        test_price_anomalies()
        print("\n" + "="*50 + "\n")
        test_rate_limiting()
    except Exception as e:
        print(f"Test failed: {e}")
