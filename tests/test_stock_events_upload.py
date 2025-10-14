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
