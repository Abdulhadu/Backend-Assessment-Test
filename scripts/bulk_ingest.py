#!/usr/bin/env python3
"""
Bulk Order Data Ingestion Script

This script performs bulk ingestion of order-related data for each tenant.
It reads the generated order and order_items files and uploads them using the tenant's API key.

Note: This script only processes order and order_items data. Other data types
(products, customers, etc.) are not supported by the current ingestion API.

Usage:
    python bulk_ingest.py --data-dir generated_data --api-url http://localhost:8000
    python bulk_ingest.py --data-dir generated_data --tenant-id <specific-tenant-id>
    python bulk_ingest.py --data-dir generated_data --dry-run
"""

import argparse
import csv
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Optional
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BulkIngestionClient:
    """Client for bulk data ingestion."""
    
    def __init__(self, api_base_url: str = "http://localhost:8000"):
        """Initialize the bulk ingestion client."""
        self.api_base_url = api_base_url.rstrip('/')
        self.session = requests.Session()
    
    def get_tenant_api_key(self, tenant_id: str) -> str:
        """Get the API key for a tenant."""
        return f"api_key_{tenant_id}"
    
    def create_upload_session(self, tenant_id: str) -> Dict:
        """Create an upload session for a tenant."""
        api_key = self.get_tenant_api_key(tenant_id)
        url = f"{self.api_base_url}/api/v1/ingest/sessions/"
        
        headers = {
            'X-API-Key': api_key,
            'Content-Type': 'application/json'
        }
        
        payload = {
            'manifest': {
                'description': f'Bulk data upload for tenant {tenant_id}',
                'source': 'bulk_ingest_script'
            }
        }
        
        response = self.session.post(url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()
    
    def upload_file(self, filepath: str, tenant_id: str, upload_token: str = None) -> Dict:
        """Upload a single file to the comprehensive API."""
        api_key = self.get_tenant_api_key(tenant_id)
        url = f"{self.api_base_url}/api/v1/ingest/comprehensive/"
        
        headers = {
            'X-API-Key': api_key
        }
        
        if upload_token:
            headers['Upload-Token'] = upload_token
        
        # Generate idempotency key based on file content
        with open(filepath, 'rb') as f:
            content_hash = hashlib.sha256(f.read()).hexdigest()
        headers['Idempotency-Key'] = content_hash
        
        # Determine content type
        if filepath.endswith('.gz'):
            content_type = 'application/gzip'
        elif filepath.endswith('.csv'):
            content_type = 'text/csv'
        else:
            content_type = 'application/x-ndjson'
        
        with open(filepath, 'rb') as f:
            files = {'file': (os.path.basename(filepath), f, content_type)}
            response = self.session.post(url, files=files, headers=headers)
        
        response.raise_for_status()
        return response.json()
    
    def get_upload_status(self, tenant_id: str, upload_token: str) -> Dict:
        """Get the status of an upload session."""
        api_key = self.get_tenant_api_key(tenant_id)
        url = f"{self.api_base_url}/api/v1/ingest/sessions/{upload_token}/status/"
        
        headers = {
            'X-API-Key': api_key
        }
        
        response = self.session.get(url, headers=headers)
        response.raise_for_status()
        return response.json()


class DataFileManager:
    """Manages data files for bulk ingestion."""
    
    def __init__(self, data_dir: str):
        """Initialize the data file manager."""
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            raise ValueError(f"Data directory not found: {data_dir}")
        
        # Log the data directory for debugging
        logger.info(f"Data directory: {self.data_dir.absolute()}")
        logger.info(f"Data directory exists: {self.data_dir.exists()}")
        if self.data_dir.exists():
            logger.info(f"Data directory contents: {list(self.data_dir.glob('*'))}")
    
    def get_tenants(self) -> List[Dict]:
        """Get tenant information from tenants.csv."""
        tenants_file = self.data_dir / 'tenants.csv'
        if not tenants_file.exists():
            raise ValueError(f"Tenants file not found: {tenants_file}")
        
        tenants = []
        with open(tenants_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                tenants.append(row)
        
        return tenants
    
    def get_tenant_files(self, tenant_id: str) -> Dict[str, List[str]]:
        """Get all data files for a specific tenant."""
        files = {
            'customers': [],
            'products': [],
            'orders': [],
            'order_items': []
        }
        
        # Find all files for this tenant - comprehensive data
        for file_path in self.data_dir.glob(f'*_{tenant_id}*'):
            filename = file_path.name
            
            if filename.startswith('customers_'):
                files['customers'].append(str(file_path))
            elif filename.startswith('products_'):
                files['products'].append(str(file_path))
            elif filename.startswith('orders_'):
                files['orders'].append(str(file_path))
            elif filename.startswith('order_items_'):
                files['order_items'].append(str(file_path))
        
        return files
    
    def get_all_tenant_files(self) -> Dict[str, Dict[str, List[str]]]:
        """Get all data files for all tenants."""
        tenants = self.get_tenants()
        all_files = {}
        
        for tenant in tenants:
            tenant_id = tenant['tenant_id']
            all_files[tenant_id] = self.get_tenant_files(tenant_id)
        
        return all_files


def upload_tenant_data(client: BulkIngestionClient, tenant_id: str, 
                      files: Dict[str, List[str]], dry_run: bool = False) -> Dict:
    """Upload all data files for a tenant in dependency order."""
    results = {
        'tenant_id': tenant_id,
        'upload_token': None,
        'files_uploaded': 0,
        'files_failed': 0,
        'total_rows': 0,
        'errors': []
    }
    
    if dry_run:
        total_files = sum(len(file_list) for file_list in files.values())
        logger.info(f"[DRY RUN] Would upload {total_files} files for tenant {tenant_id}")
        logger.info(f"[DRY RUN] Upload order: customers -> products -> orders -> order_items")
        return results
    
    try:
        # Create upload session
        session_result = client.create_upload_session(tenant_id)
        upload_token = session_result['upload_token']
        results['upload_token'] = upload_token
        
        logger.info(f"Created upload session for tenant {tenant_id}: {upload_token}")
        
        # Upload files in dependency order: customers -> products -> orders -> order_items
        upload_order = ['customers', 'products', 'orders', 'order_items']
        
        for data_type in upload_order:
            file_list = files.get(data_type, [])
            if not file_list:
                continue
                
            logger.info(f"Uploading {len(file_list)} {data_type} files for tenant {tenant_id}")
            
            with ThreadPoolExecutor(max_workers=2) as executor:
                future_to_file = {
                    executor.submit(client.upload_file, filepath, tenant_id, upload_token): filepath
                    for filepath in file_list
                }
                
                for future in as_completed(future_to_file):
                    filepath = future_to_file[future]
                    try:
                        result = future.result()
                        results['files_uploaded'] += 1
                        results['total_rows'] += result.get('rows_received', 0)
                        logger.info(f"Uploaded: {os.path.basename(filepath)} ({data_type})")
                    except Exception as e:
                        results['files_failed'] += 1
                        error_msg = f"Failed to upload {filepath} ({data_type}): {e}"
                        results['errors'].append(error_msg)
                        logger.error(error_msg)
        
        logger.info(f"Completed upload for tenant {tenant_id}: "
                   f"{results['files_uploaded']} successful, {results['files_failed']} failed")
        
    except Exception as e:
        error_msg = f"Error uploading data for tenant {tenant_id}: {e}"
        results['errors'].append(error_msg)
        logger.error(error_msg)
    
    return results


def main():
    """Main function for bulk ingestion."""
    parser = argparse.ArgumentParser(description='Bulk data ingestion script')
    
    parser.add_argument('--data-dir', required=True, help='Directory containing generated data files')
    parser.add_argument('--api-url', default='http://localhost:8000', help='API base URL')
    parser.add_argument('--tenant-id', help='Process only specific tenant ID')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be uploaded without uploading')
    parser.add_argument('--wait-for-completion', action='store_true', help='Wait for all uploads to complete')
    parser.add_argument('--verbose', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Initialize components
        client = BulkIngestionClient(args.api_url)
        file_manager = DataFileManager(args.data_dir)
        
        if args.tenant_id:
            # Process specific tenant
            tenants = [t for t in file_manager.get_tenants() if t['tenant_id'] == args.tenant_id]
            if not tenants:
                logger.error(f"Tenant {args.tenant_id} not found in tenants.csv")
                return
        else:
            # Process all tenants
            tenants = file_manager.get_tenants()
        
        logger.info(f"Processing {len(tenants)} tenants")
        
        if args.dry_run:
            logger.info("DRY RUN MODE - No actual uploads will be performed")
        
        # Get all files
        all_files = file_manager.get_all_tenant_files()
        
        # Process each tenant
        start_time = time.time()
        all_results = []
        
        for tenant in tenants:
            tenant_id = tenant['tenant_id']
            tenant_name = tenant['name']
            
            logger.info(f"Processing tenant: {tenant_name} ({tenant_id})")
            
            if tenant_id not in all_files:
                logger.warning(f"No data files found for tenant {tenant_id}")
                continue
            
            files = all_files[tenant_id]
            result = upload_tenant_data(client, tenant_id, files, args.dry_run)
            result['tenant_name'] = tenant_name
            all_results.append(result)
        
        # Summary
        end_time = time.time()
        duration = end_time - start_time
        
        total_files = sum(len(sum(files.values(), [])) for files in all_files.values())
        successful_uploads = sum(r['files_uploaded'] for r in all_results)
        failed_uploads = sum(r['files_failed'] for r in all_results)
        total_rows = sum(r['total_rows'] for r in all_results)
        
        logger.info("=" * 80)
        logger.info("COMPREHENSIVE BULK DATA INGESTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Tenants processed: {len(tenants)}")
        logger.info(f"Total files: {total_files}")
        logger.info(f"Successful uploads: {successful_uploads}")
        logger.info(f"Failed uploads: {failed_uploads}")
        logger.info(f"Total rows uploaded: {total_rows:,}")
        logger.info(f"Processing time: {duration:.2f} seconds")
        logger.info("Data types processed: customers, products, orders, order_items")
        logger.info("Processing order: customers -> products -> orders -> order_items")
        
        if not args.dry_run:
            logger.info(f"Throughput: {total_rows/duration:.0f} rows/second")
        
        # Show errors if any
        all_errors = []
        for result in all_results:
            all_errors.extend(result['errors'])
        
        if all_errors:
            logger.error(f"\nErrors encountered ({len(all_errors)}):")
            for error in all_errors:
                logger.error(f"  {error}")
        
        # Show upload tokens for monitoring
        if not args.dry_run:
            logger.info("\nUpload tokens for monitoring:")
            for result in all_results:
                if result['upload_token']:
                    logger.info(f"  {result['tenant_name']}: {result['upload_token']}")
        
        # Wait for completion if requested
        if args.wait_for_completion and not args.dry_run:
            logger.info("\nWaiting for uploads to complete...")
            for result in all_results:
                if result['upload_token']:
                    tenant_id = result['tenant_id']
                    upload_token = result['upload_token']
                    
                    while True:
                        try:
                            status = client.get_upload_status(tenant_id, upload_token)
                            logger.info(f"Tenant {tenant_id}: {status['status']} "
                                       f"({status.get('completed_chunks', 0)}/{status.get('total_chunks', 0)} chunks)")
                            
                            if status['status'] in ['completed', 'failed']:
                                break
                            
                            time.sleep(5)
                        except Exception as e:
                            logger.error(f"Error checking status for {tenant_id}: {e}")
                            break
    
    except Exception as e:
        logger.error(f"Error during bulk ingestion: {e}")
        raise


if __name__ == '__main__':
    main()
