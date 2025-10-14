#!/usr/bin/env python3
"""
Synthetic Data Generator for Multi-Tenant E-commerce System

This script generates realistic synthetic data for testing and benchmarking
the bulk ingestion system. It supports various data sizes and formats.

Usage:
    python gen_dataset.py --tenants 10 --orders 2000000 --products 500000
    python gen_dataset.py --preset small
    python gen_dataset.py --preset medium --format csv
    python gen_dataset.py --preset large --format ndjson --chunk-size 10000
"""

import argparse
import csv
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Generator
import hashlib
import gzip
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from faker import Faker
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Faker
fake = Faker()


class DataGenerator:
    """Generates synthetic data for the multi-tenant e-commerce system."""
    
    def __init__(self, seed: int = 42):
        """Initialize the data generator with a seed for reproducibility."""
        random.seed(seed)
        Faker.seed(seed)
        self.fake = Faker()
        
        # Pre-generated data pools for consistency
        self._product_categories = [
            'Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports',
            'Beauty', 'Toys', 'Automotive', 'Health', 'Food & Beverage'
        ]
        
        self._order_statuses = [
            'pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded'
        ]
        
        self._currencies = ['USD', 'EUR', 'GBP', 'CAD']
        
        # Cache for generated data
        self._tenant_cache: Dict[str, Dict] = {}
        self._product_cache: Dict[str, List[Dict]] = {}
        self._customer_cache: Dict[str, List[Dict]] = {}
    
    def generate_tenant(self, tenant_id: str) -> Dict:
        """Generate a single tenant record."""
        if tenant_id in self._tenant_cache:
            return self._tenant_cache[tenant_id]
        
        tenant = {
            'tenant_id': tenant_id,
            'name': f"{self.fake.company()} {self.fake.company_suffix()}",
            'api_key_hash': hashlib.sha256(f"api_key_{tenant_id}".encode()).hexdigest(),
            'created_at': self.fake.date_time_between(start_date='-2y', end_date='now').isoformat()
        }
        
        self._tenant_cache[tenant_id] = tenant
        return tenant
    
    def generate_products(self, tenant_id: str, count: int) -> Generator[Dict, None, None]:
        """Generate products for a tenant."""
        if tenant_id in self._product_cache:
            products = self._product_cache[tenant_id]
        else:
            products = []
            self._product_cache[tenant_id] = products
        
        for i in range(count):
            product_id = str(uuid.uuid4())
            category = random.choice(self._product_categories)
            
            product = {
                'product_id': product_id,
                'tenant_id': tenant_id,
                'sku': f"{category[:3].upper()}{i:06d}",
                'name': f"{self.fake.word().title()} {self.fake.word().title()} {category}",
                'price': round(random.uniform(5.99, 999.99), 2),
                'category_id': str(uuid.uuid4()) if random.random() < 0.7 else None,
                'active': random.random() < 0.95,
                'created_at': self.fake.date_time_between(start_date='-1y', end_date='now').isoformat()
            }
            
            products.append(product)
            yield product
    
    def generate_customers(self, tenant_id: str, count: int) -> Generator[Dict, None, None]:
        """Generate customers for a tenant."""
        if tenant_id in self._customer_cache:
            customers = self._customer_cache[tenant_id]
        else:
            customers = []
            self._customer_cache[tenant_id] = customers
        
        for i in range(count):
            customer_id = str(uuid.uuid4())
            
            customer = {
                'customer_id': customer_id,
                'tenant_id': tenant_id,
                'name': self.fake.name(),
                'email': self.fake.email(),
                'metadata': {
                    'phone': self.fake.phone_number(),
                    'address': self.fake.address(),
                    'loyalty_tier': random.choice(['bronze', 'silver', 'gold', 'platinum']),
                    'signup_source': random.choice(['web', 'mobile', 'referral', 'organic'])
                },
                'created_at': self.fake.date_time_between(start_date='-1y', end_date='now').isoformat()
            }
            
            customers.append(customer)
            yield customer
    
    def generate_orders(self, tenant_id: str, count: int, 
                       products: List[Dict], customers: List[Dict]) -> Generator[Dict, None, None]:
        """Generate orders for a tenant."""
        for i in range(count):
            order_id = str(uuid.uuid4())
            customer = random.choice(customers) if customers else None
            order_date = self.fake.date_time_between(start_date='-6m', end_date='now')
            
            order = {
                'order_id': order_id,
                'tenant_id': tenant_id,
                'external_order_id': f"EXT{tenant_id[:8]}{i:08d}",
                'customer_id': customer['customer_id'] if customer else None,
                'customer_name_snapshot': customer['name'] if customer else self.fake.name(),
                'customer_email_snapshot': customer['email'] if customer else self.fake.email(),
                'total_amount': 0,  # Will be calculated after order items
                'currency': random.choice(self._currencies),
                'order_status': random.choices(
                    self._order_statuses,
                    weights=[10, 15, 20, 25, 20, 5, 5]  # More delivered/shipped orders
                )[0],
                'order_date': order_date.isoformat(),
                'raw_payload': {
                    'source': random.choice(['web', 'mobile', 'api']),
                    'campaign': self.fake.word() if random.random() < 0.3 else None,
                    'discount_code': self.fake.word().upper() if random.random() < 0.1 else None
                },
                'created_at': order_date.isoformat()
            }
            
            yield order
    
    def generate_order_items(self, order: Dict, products: List[Dict]) -> Generator[Dict, None, None]:
        """Generate order items for an order."""
        # Random number of items per order (1-5, weighted towards 2-3)
        item_count = random.choices([1, 2, 3, 4, 5], weights=[5, 25, 35, 25, 10])[0]
        
        selected_products = random.sample(products, min(item_count, len(products)))
        total_amount = Decimal('0')
        
        for product in selected_products:
            quantity = random.randint(1, 5)
            unit_price = Decimal(str(product['price']))
            line_total = unit_price * quantity
            total_amount += line_total
            
            order_item = {
                'order_item_id': str(uuid.uuid4()),
                'order_id': order['order_id'],
                'tenant_id': order['tenant_id'],
                'product_id': product['product_id'],
                'quantity': quantity,
                'unit_price': float(unit_price),
                'line_total': float(line_total)
            }
            
            yield order_item
        
        # Update order total
        order['total_amount'] = float(total_amount)
    
    def generate_price_history(self, product: Dict, count: int = 100) -> Generator[Dict, None, None]:
        """Generate price history for a product."""
        base_price = Decimal(str(product['price']))
        current_price = base_price
        
        for i in range(count):
            # Price changes: mostly small, occasional large changes
            if random.random() < 0.1:  # 10% chance of price change
                change_pct = random.uniform(-0.15, 0.15)  # ±15% max change
                current_price = base_price * (1 + Decimal(str(change_pct)))
                current_price = current_price.quantize(Decimal('0.01'))
            
            price_record = {
                'id': str(uuid.uuid4()),
                'tenant_id': product['tenant_id'],
                'product_id': product['product_id'],
                'price': float(current_price),
                'effective_from': self.fake.date_time_between(
                    start_date='-1y', 
                    end_date='now'
                ).isoformat(),
                'effective_to': None
            }
            
            yield price_record
    
    def generate_stock_events(self, product: Dict, count: int = 1000) -> Generator[Dict, None, None]:
        """Generate stock events for a product."""
        current_stock = random.randint(0, 1000)
        
        for i in range(count):
            # Stock changes: mostly small adjustments
            delta = random.randint(-50, 100)
            current_stock = max(0, current_stock + delta)
            
            stock_event = {
                'stock_event_id': str(uuid.uuid4()),
                'tenant_id': product['tenant_id'],
                'product_id': product['product_id'],
                'delta': delta,
                'resulting_level': current_stock,
                'event_time': self.fake.date_time_between(
                    start_date='-6m', 
                    end_date='now'
                ).isoformat(),
                'source': random.choice(['manual', 'order', 'receipt', 'return', 'adjustment', 'system']),
                'meta': {
                    'reason': self.fake.sentence() if random.random() < 0.3 else None,
                    'operator': self.fake.name() if random.random() < 0.5 else None
                }
            }
            
            yield stock_event


class DataWriter:
    """Handles writing generated data to various formats."""
    
    def __init__(self, output_dir: str = "generated_data"):
        """Initialize the data writer."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
    
    def write_csv(self, data: List[Dict], filename: str, compress: bool = False) -> str:
        """Write data to CSV file."""
        filepath = self.output_dir / filename
        if compress:
            filepath = filepath.with_suffix('.csv.gz')
            with gzip.open(filepath, 'wt', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
        else:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
        
        return str(filepath)
    
    def write_ndjson(self, data: Generator[Dict, None, None], filename: str, 
                    compress: bool = False) -> str:
        """Write data to NDJSON file."""
        filepath = self.output_dir / filename
        if compress:
            filepath = filepath.with_suffix('.ndjson.gz')
            with gzip.open(filepath, 'wt', encoding='utf-8') as f:
                for record in data:
                    f.write(json.dumps(record) + '\n')
        else:
            with open(filepath, 'w', encoding='utf-8') as f:
                for record in data:
                    f.write(json.dumps(record) + '\n')
        
        return str(filepath)
    
    def write_chunked(self, data: Generator[Dict, None, None], filename: str, 
                     chunk_size: int = 10000, format: str = 'ndjson') -> List[str]:
        """Write data in chunks."""
        chunk_files = []
        chunk_data = []
        chunk_num = 0
        
        for record in data:
            chunk_data.append(record)
            
            if len(chunk_data) >= chunk_size:
                chunk_filename = f"{filename}_chunk_{chunk_num:04d}"
                if format == 'csv':
                    filepath = self.write_csv(chunk_data, f"{chunk_filename}.csv")
                else:
                    filepath = self.write_ndjson(iter(chunk_data), f"{chunk_filename}.ndjson")
                
                chunk_files.append(filepath)
                chunk_data = []
                chunk_num += 1
        
        # Write remaining data
        if chunk_data:
            chunk_filename = f"{filename}_chunk_{chunk_num:04d}"
            if format == 'csv':
                filepath = self.write_csv(chunk_data, f"{chunk_filename}.csv")
            else:
                filepath = self.write_ndjson(iter(chunk_data), f"{chunk_filename}.ndjson")
            
            chunk_files.append(filepath)
        
        return chunk_files


class BulkUploader:
    """Handles bulk uploading of generated data to the API."""
    
    def __init__(self, api_base_url: str = "http://localhost:8000", 
                 api_key: str = None, chunk_size: int = 10000):
        """Initialize the bulk uploader."""
        self.api_base_url = api_base_url.rstrip('/')
        self.api_key = api_key
        self.chunk_size = chunk_size
        self.session = requests.Session()
        
        if api_key:
            self.session.headers.update({'X-API-Key': api_key})
    
    def upload_file(self, filepath: str, upload_token: str = None) -> Dict:
        """Upload a single file to the API."""
        url = f"{self.api_base_url}/api/v1/ingest/orders/"
        
        headers = {}
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
    
    def upload_chunks(self, chunk_files: List[str], upload_token: str = None) -> List[Dict]:
        """Upload multiple chunks with progress tracking."""
        results = []
        total_files = len(chunk_files)
        
        logger.info(f"Starting upload of {total_files} chunks...")
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_file = {
                executor.submit(self.upload_file, filepath, upload_token): filepath
                for filepath in chunk_files
            }
            
            for i, future in enumerate(as_completed(future_to_file), 1):
                filepath = future_to_file[future]
                try:
                    result = future.result()
                    results.append(result)
                    logger.info(f"Uploaded {i}/{total_files}: {os.path.basename(filepath)}")
                except Exception as e:
                    logger.error(f"Failed to upload {filepath}: {e}")
                    results.append({'error': str(e), 'file': filepath})
        
        return results


def main():
    """Main function to generate and optionally upload synthetic data."""
    parser = argparse.ArgumentParser(description='Generate synthetic e-commerce data')
    
    # Data size arguments
    parser.add_argument('--tenants', type=int, default=10, help='Number of tenants')
    parser.add_argument('--products-per-tenant', type=int, default=500000, help='Products per tenant')
    parser.add_argument('--orders-per-tenant', type=int, default=2000000, help='Orders per tenant')
    parser.add_argument('--customers-per-tenant', type=int, default=10000, help='Customers per tenant')
    
    # Preset configurations
    parser.add_argument('--preset', choices=['small', 'medium', 'large'], 
                       help='Use preset configuration')
    
    # Output options
    parser.add_argument('--format', choices=['csv', 'ndjson'], default='ndjson',
                       help='Output format')
    parser.add_argument('--compress', action='store_true', help='Compress output files')
    parser.add_argument('--chunk-size', type=int, default=10000, 
                       help='Chunk size for large datasets')
    parser.add_argument('--output-dir', default='generated_data', 
                       help='Output directory')
    
    # Upload options
    parser.add_argument('--upload', action='store_true', help='Upload generated data')
    parser.add_argument('--api-url', default='http://localhost:8000', 
                       help='API base URL')
    parser.add_argument('--api-key', help='API key for authentication')
    
    # Performance options
    parser.add_argument('--seed', type=int, default=42, help='Random seed')
    parser.add_argument('--verbose', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Apply presets
    if args.preset:
        presets = {
            'small': {'tenants': 2, 'products': 1000, 'orders': 5000, 'customers': 100},
            'medium': {'tenants': 5, 'products': 50000, 'orders': 100000, 'customers': 1000},
            'large': {'tenants': 10, 'products': 500000, 'orders': 2000000, 'customers': 10000}
        }
        
        preset = presets[args.preset]
        args.tenants = preset['tenants']
        args.products_per_tenant = preset['products']
        args.orders_per_tenant = preset['orders']
        args.customers_per_tenant = preset['customers']
    
    logger.info(f"Generating data: {args.tenants} tenants, "
               f"{args.products_per_tenant} products/tenant, "
               f"{args.orders_per_tenant} orders/tenant")
    
    # Initialize components
    generator = DataGenerator(seed=args.seed)
    writer = DataWriter(args.output_dir)
    
    start_time = time.time()
    total_rows = 0
    
    # Generate tenants
    logger.info("Generating tenants...")
    tenants = []
    for i in range(args.tenants):
        tenant_id = str(uuid.uuid4())
        tenant = generator.generate_tenant(tenant_id)
        tenants.append(tenant)
    
    writer.write_csv(tenants, 'tenants.csv', args.compress)
    total_rows += len(tenants)
    logger.info(f"Generated {len(tenants)} tenants")
    
    # Generate data for each tenant
    for tenant in tenants:
        tenant_id = tenant['tenant_id']
        logger.info(f"Generating data for tenant: {tenant['name']}")
        
        # Generate products
        logger.info(f"  Generating {args.products_per_tenant} products...")
        products = list(generator.generate_products(tenant_id, args.products_per_tenant))
        
        if args.format == 'csv':
            writer.write_chunked(iter(products), f'products_{tenant_id}', 
                               args.chunk_size, 'csv')
        else:
            writer.write_chunked(iter(products), f'products_{tenant_id}', 
                               args.chunk_size, 'ndjson')
        
        total_rows += len(products)
        
        # Generate customers
        logger.info(f"  Generating {args.customers_per_tenant} customers...")
        customers = list(generator.generate_customers(tenant_id, args.customers_per_tenant))
        
        if args.format == 'csv':
            writer.write_csv(customers, f'customers_{tenant_id}.csv', args.compress)
        else:
            writer.write_ndjson(iter(customers), f'customers_{tenant_id}.ndjson', args.compress)
        
        total_rows += len(customers)
        
        # Generate orders
        logger.info(f"  Generating {args.orders_per_tenant} orders...")
        orders = list(generator.generate_orders(tenant_id, args.orders_per_tenant, products, customers))
        
        if args.format == 'csv':
            writer.write_chunked(iter(orders), f'orders_{tenant_id}', 
                               args.chunk_size, 'csv')
        else:
            writer.write_chunked(iter(orders), f'orders_{tenant_id}', 
                               args.chunk_size, 'ndjson')
        
        total_rows += len(orders)
        
        # Generate order items
        logger.info(f"  Generating order items...")
        order_items = []
        for order in orders:
            items = list(generator.generate_order_items(order, products))
            order_items.extend(items)
        
        if args.format == 'csv':
            writer.write_chunked(iter(order_items), f'order_items_{tenant_id}', 
                               args.chunk_size, 'csv')
        else:
            writer.write_chunked(iter(order_items), f'order_items_{tenant_id}', 
                               args.chunk_size, 'ndjson')
        
        total_rows += len(order_items)
        
        # Generate price history (sample)
        logger.info(f"  Generating price history...")
        price_history = []
        sample_products = random.sample(products, min(1000, len(products)))
        for product in sample_products:
            history = list(generator.generate_price_history(product, 100))
            price_history.extend(history)
        
        if args.format == 'csv':
            writer.write_chunked(iter(price_history), f'price_history_{tenant_id}', 
                               args.chunk_size, 'csv')
        else:
            writer.write_chunked(iter(price_history), f'price_history_{tenant_id}', 
                               args.chunk_size, 'ndjson')
        
        total_rows += len(price_history)
        
        # Generate stock events (sample)
        logger.info(f"  Generating stock events...")
        stock_events = []
        sample_products = random.sample(products, min(1000, len(products)))
        for product in sample_products:
            events = list(generator.generate_stock_events(product, 1000))
            stock_events.extend(events)
        
        if args.format == 'csv':
            writer.write_chunked(iter(stock_events), f'stock_events_{tenant_id}', 
                               args.chunk_size, 'csv')
        else:
            writer.write_chunked(iter(stock_events), f'stock_events_{tenant_id}', 
                               args.chunk_size, 'ndjson')
        
        total_rows += len(stock_events)
    
    end_time = time.time()
    duration = end_time - start_time
    
    logger.info(f"Data generation completed!")
    logger.info(f"Total rows generated: {total_rows:,}")
    logger.info(f"Generation time: {duration:.2f} seconds")
    logger.info(f"Throughput: {total_rows/duration:.0f} rows/second")
    logger.info(f"Output directory: {args.output_dir}")
    
    # Upload if requested
    if args.upload:
        if not args.api_key:
            logger.error("API key required for upload")
            return
        
        logger.info("Starting bulk upload...")
        uploader = BulkUploader(args.api_url, args.api_key, args.chunk_size)
        
        # Find all generated files
        output_path = Path(args.output_dir)
        files_to_upload = list(output_path.glob('*.csv')) + list(output_path.glob('*.ndjson'))
        
        upload_start = time.time()
        results = uploader.upload_chunks([str(f) for f in files_to_upload])
        upload_end = time.time()
        
        successful_uploads = [r for r in results if 'error' not in r]
        failed_uploads = [r for r in results if 'error' in r]
        
        logger.info(f"Upload completed!")
        logger.info(f"Successful uploads: {len(successful_uploads)}")
        logger.info(f"Failed uploads: {len(failed_uploads)}")
        logger.info(f"Upload time: {upload_end - upload_start:.2f} seconds")
        
        if failed_uploads:
            logger.error("Failed uploads:")
            for result in failed_uploads:
                logger.error(f"  {result['file']}: {result['error']}")


if __name__ == '__main__':
    main()
