"""
Comprehensive data processing utilities for all data types.
Handles customers, products, orders, and order items with proper dependency management.
"""
import csv
import gzip
import hashlib
import json
import logging
import time
from typing import Dict, List, Optional, Tuple, Any
from django.conf import settings
from django.core.files.storage import default_storage
from django.db import connection, transaction
from django.db.utils import OperationalError
from django.db.utils import ProgrammingError
from django.utils import timezone
from .data_processor import DataProcessor
from django.db.utils import OperationalError, IntegrityError
import uuid


class UUIDEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles UUID objects."""
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            return str(obj)
        return super().default(obj)


logger = logging.getLogger(__name__)


class ComprehensiveStagingTableManager:
    """Manages staging tables for comprehensive data processing."""

    @staticmethod
    def create_staging_tables():
        """Create staging tables for all data types."""
        with connection.cursor() as cursor:
            # Drop existing staging tables if they exist to ensure clean schema
            DataProcessor._execute_with_retry(cursor, "DROP TABLE IF EXISTS customers_staging CASCADE")
            DataProcessor._execute_with_retry(cursor, "DROP TABLE IF EXISTS products_staging CASCADE")
            DataProcessor._execute_with_retry(cursor, "DROP TABLE IF EXISTS orders_staging CASCADE")
            DataProcessor._execute_with_retry(cursor, "DROP TABLE IF EXISTS order_items_staging CASCADE")
            
            # Customers staging table
            DataProcessor._execute_with_retry(cursor, """
                CREATE TABLE customers_staging (
                    staging_id BIGSERIAL PRIMARY KEY,
                    tenant_id UUID NOT NULL,
                    customer_id UUID NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    metadata JSONB,
                    created_at TIMESTAMP NOT NULL,
                    processed_at TIMESTAMP,
                    error_message TEXT,
                    chunk_id UUID,
                    UNIQUE(tenant_id, email)
                )
            """)

            # Products staging table
            DataProcessor._execute_with_retry(cursor, """
                CREATE TABLE products_staging (
                    staging_id BIGSERIAL PRIMARY KEY,
                    tenant_id UUID NOT NULL,
                    product_id UUID NOT NULL,
                    sku VARCHAR(100) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    price DECIMAL(10,2) NOT NULL,
                    category_id UUID,
                    active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP NOT NULL,
                    processed_at TIMESTAMP,
                    error_message TEXT,
                    chunk_id UUID,
                    UNIQUE(tenant_id, sku)
                )
            """)
            
            # Orders staging table
            DataProcessor._execute_with_retry(cursor, """
                CREATE TABLE orders_staging (
                    staging_id BIGSERIAL PRIMARY KEY,
                    tenant_id UUID NOT NULL,
                    order_id UUID NOT NULL,
                    external_order_id VARCHAR(100) NOT NULL,
                    customer_id UUID,
                    customer_name_snapshot VARCHAR(255),
                    customer_email_snapshot VARCHAR(255),
                    total_amount DECIMAL(12,2) NOT NULL,
                    currency VARCHAR(3) NOT NULL,
                    order_status VARCHAR(100) NOT NULL,
                    order_date TIMESTAMP NOT NULL,
                    raw_payload JSONB,
                    created_at TIMESTAMP NOT NULL,
                    processed_at TIMESTAMP,
                    error_message TEXT,
                    chunk_id UUID,
                    UNIQUE(tenant_id, external_order_id)
                )
            """)

            # Order items staging table
            DataProcessor._execute_with_retry(cursor, """
                CREATE TABLE order_items_staging (
                    staging_id BIGSERIAL PRIMARY KEY,
                    tenant_id UUID NOT NULL,
                    order_item_id UUID NOT NULL,
                    order_id UUID NOT NULL,
                    product_id UUID NOT NULL,
                    quantity INTEGER NOT NULL,
                    unit_price DECIMAL(10,2) NOT NULL,
                    line_total DECIMAL(12,2) NOT NULL,
                    processed_at TIMESTAMP,
                    error_message TEXT,
                    chunk_id UUID,
                    UNIQUE(order_item_id)
                )
            """)


            # Create indexes with DataProcessor._execute_with_retry
            DataProcessor._execute_with_retry(cursor, "CREATE INDEX IF NOT EXISTS idx_customers_staging_tenant ON customers_staging(tenant_id)")
            DataProcessor._execute_with_retry(cursor, "CREATE INDEX IF NOT EXISTS idx_customers_staging_chunk ON customers_staging(chunk_id)")
            DataProcessor._execute_with_retry(cursor, "CREATE INDEX IF NOT EXISTS idx_products_staging_tenant ON products_staging(tenant_id)")
            DataProcessor._execute_with_retry(cursor, "CREATE INDEX IF NOT EXISTS idx_products_staging_chunk ON products_staging(chunk_id)")
            DataProcessor._execute_with_retry(cursor, "CREATE INDEX IF NOT EXISTS idx_orders_staging_tenant ON orders_staging(tenant_id)")
            DataProcessor._execute_with_retry(cursor, "CREATE INDEX IF NOT EXISTS idx_orders_staging_chunk ON orders_staging(chunk_id)")
            DataProcessor._execute_with_retry(cursor, "CREATE INDEX IF NOT EXISTS idx_order_items_staging_tenant ON order_items_staging(tenant_id)")
            DataProcessor._execute_with_retry(cursor, "CREATE INDEX IF NOT EXISTS idx_order_items_staging_chunk ON order_items_staging(chunk_id)")

            # Create indexes
            # cursor.execute("CREATE INDEX IF NOT EXISTS idx_customers_staging_tenant ON customers_staging(tenant_id)")
            # cursor.execute("CREATE INDEX IF NOT EXISTS idx_customers_staging_chunk ON customers_staging(chunk_id)")
            # cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_staging_tenant ON products_staging(tenant_id)")
            # cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_staging_chunk ON products_staging(chunk_id)")
            # cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_staging_tenant ON orders_staging(tenant_id)")
            # cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_staging_chunk ON orders_staging(chunk_id)")
            # cursor.execute("CREATE INDEX IF NOT EXISTS idx_order_items_staging_tenant ON order_items_staging(tenant_id)")
            # cursor.execute("CREATE INDEX IF NOT EXISTS idx_order_items_staging_chunk ON order_items_staging(chunk_id)")

    @staticmethod
    def cleanup_staging_tables():
        """Clean up processed staging data."""
        with connection.cursor() as cursor:
            # Check if tables exist before trying to delete from them
            tables = ['customers_staging', 'products_staging', 'orders_staging', 'order_items_staging']
            
            for table in tables:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = %s
                    )
                """, [table])
                table_exists = cursor.fetchone()[0]
                
                if table_exists:
                    try:
                        DataProcessor._execute_with_retry(cursor, f"""
                            DELETE FROM {table} 
                            WHERE processed_at IS NOT NULL 
                              AND processed_at < NOW() - INTERVAL '24 hours'
                        """)
                    except ProgrammingError as e:
                        # Table could have been dropped between existence check and delete; ignore
                        if 'does not exist' in str(e):
                            logger.warning("Staging table %s disappeared before cleanup; skipping.", table)
                        else:
                            raise


class ComprehensiveDataProcessor:
    """Handles comprehensive processing of all data types."""

    def __init__(self):
        self.staging_manager = ComprehensiveStagingTableManager()

    @staticmethod
    def _execute_with_retry(cursor, query, params=None, max_retries=3, retry_delay=0.1):
        """Execute database query with retry logic."""
        for attempt in range(max_retries):
            try:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                return True
            except OperationalError as e:
                if "lock" in str(e).lower() and attempt < max_retries - 1:
                    logger.warning("DB locked, retrying in %s s (attempt %s/%s)",
                                   retry_delay, attempt + 1, max_retries)
                    time.sleep(retry_delay * (attempt + 1))
                else:
                    raise
        return False

    def process_file(self, file_path: str, content_type: str,
                     tenant_id: str, chunk_id: str, data_type: str) -> Dict[str, Any]:
        """Process uploaded file and load data into appropriate staging tables."""
        start_time = time.time()
        rows_received = 0
        rows_inserted = 0
        rows_failed = 0
        errors = []

        try:
            if not default_storage.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")

            is_compressed = file_path.endswith('.gz')
            is_csv = content_type == 'text/csv' or file_path.endswith('.csv')

            if is_compressed:
                file_obj = gzip.open(default_storage.open(file_path, 'rb'), 'rt', encoding='utf-8')
            else:
                file_obj = default_storage.open(file_path, 'r')

            with file_obj as f:
                if is_csv:
                    rows_received, rows_inserted, rows_failed, errors = self._process_csv(
                        f, tenant_id, chunk_id, data_type)
                else:
                    rows_received, rows_inserted, rows_failed, errors = self._process_ndjson(
                        f, tenant_id, chunk_id, data_type)

            processing_time = time.time() - start_time
            return {
                'rows_received': rows_received,
                'rows_inserted': rows_inserted,
                'rows_failed': rows_failed,
                'processing_time': processing_time,
                'errors': errors[:10],
                'throughput': rows_received / processing_time if processing_time > 0 else 0
            }

        except IntegrityError as e:
            if "unique_upload_chunk_index" in str(e).lower():
                logger.warning("Chunk already processed, skipping: %s", chunk_id)
                return {
                    'rows_received': 0,
                    'rows_inserted': 0,
                    'rows_failed': 0,
                    'processing_time': time.time() - start_time,
                    'errors': [{'row': 0, 'error': f'Chunk already processed: {chunk_id}', 'skipped': True}],
                    'throughput': 0,
                    'skipped': True
                }
            else:
                raise

        except Exception as e:
            logger.error("Error processing file %s: %s", file_path, str(e), exc_info=True)
            return {
                'rows_received': 0,
                'rows_inserted': 0,
                'rows_failed': 0,
                'processing_time': time.time() - start_time,
                'errors': [{'row': 0, 'error': str(e)}],
                'throughput': 0
            }

    def _process_csv(self, file_obj, tenant_id: str, chunk_id: str, data_type: str) -> Tuple[int, int, int, List]:
        """Process CSV file."""
        reader = csv.DictReader(file_obj)
        rows_received = 0
        rows_inserted = 0
        rows_failed = 0
        errors = []
        batch_size = 1000
        batch_data = []

        for row_num, row in enumerate(reader, 1):
            rows_received += 1
            try:
                processed_row = self._validate_row(row, tenant_id, data_type)
                batch_data.append(processed_row)

                if len(batch_data) >= batch_size:
                    inserted, failed, batch_errors = self._insert_batch(batch_data, chunk_id, data_type)
                    rows_inserted += inserted
                    rows_failed += failed
                    errors.extend(batch_errors)
                    batch_data = []
            except Exception as e:
                rows_failed += 1
                # Convert any UUID objects to strings in error data
                serializable_row = {}
                for key, value in row.items():
                    if isinstance(value, uuid.UUID):
                        serializable_row[key] = str(value)
                    else:
                        serializable_row[key] = value
                errors.append({'row': row_num, 'error': str(e), 'data': serializable_row})

        if batch_data:
            inserted, failed, batch_errors = self._insert_batch(batch_data, chunk_id, data_type)
            rows_inserted += inserted
            rows_failed += failed
            errors.extend(batch_errors)

        return rows_received, rows_inserted, rows_failed, errors

    def _process_ndjson(self, file_obj, tenant_id: str, chunk_id: str, data_type: str) -> Tuple[int, int, int, List]:
        """Process NDJSON file."""
        rows_received = 0
        rows_inserted = 0
        rows_failed = 0
        errors = []
        batch_size = 1000
        batch_data = []

        for line_num, line in enumerate(file_obj, 1):
            if not line.strip():
                continue
            rows_received += 1
            try:
                row = json.loads(line.strip())
                processed_row = self._validate_row(row, tenant_id, data_type)
                batch_data.append(processed_row)
                if len(batch_data) >= batch_size:
                    inserted, failed, batch_errors = self._insert_batch(batch_data, chunk_id, data_type)
                    rows_inserted += inserted
                    rows_failed += failed
                    errors.extend(batch_errors)
                    batch_data = []
            except Exception as e:
                rows_failed += 1
                errors.append({'row': line_num, 'error': str(e), 'data': line.strip()})

        if batch_data:
            inserted, failed, batch_errors = self._insert_batch(batch_data, chunk_id, data_type)
            rows_inserted += inserted
            rows_failed += failed
            errors.extend(batch_errors)

        return rows_received, rows_inserted, rows_failed, errors

    def _validate_row(self, row: Dict, tenant_id: str, data_type: str) -> Dict:
        """Validate and transform row based on data type."""
        if data_type == 'customers':
            return self._validate_customer_row(row, tenant_id)
        elif data_type == 'products':
            return self._validate_product_row(row, tenant_id)
        elif data_type == 'orders':
            return self._validate_order_row(row, tenant_id)
        elif data_type == 'order_items':
            return self._validate_order_item_row(row, tenant_id)
        else:
            raise ValueError(f"Unknown data type: {data_type}")

    def _validate_customer_row(self, row: Dict, tenant_id: str) -> Dict:
        """Validate and transform customer row."""
        required_fields = ['customer_id', 'name', 'email']
        for field in required_fields:
            if field not in row:
                raise ValueError(f"Missing required field: {field}")

        # Validate UUIDs
        try:
            customer_id_uuid = uuid.UUID(str(row['customer_id']))
        except (ValueError, TypeError):
            raise ValueError(f"Invalid customer_id format: {row['customer_id']}")

        processed_row = {
            'tenant_id': uuid.UUID(tenant_id),
            'customer_id': customer_id_uuid,
            'name': str(row['name']),
            'email': str(row['email']),
            'metadata': self._process_raw_payload(row.get('metadata', {})),
            'created_at': row.get('created_at', timezone.now().isoformat())
        }

        # Ensure field lengths don't exceed limits
        if len(processed_row['name']) > 255:
            raise ValueError(f"Customer name too long: {processed_row['name']}")
        if len(processed_row['email']) > 255:
            raise ValueError(f"Customer email too long: {processed_row['email']}")

        return processed_row

    def _validate_product_row(self, row: Dict, tenant_id: str) -> Dict:
        """Validate and transform product row."""
        required_fields = ['product_id', 'sku', 'name', 'price']
        for field in required_fields:
            if field not in row:
                raise ValueError(f"Missing required field: {field}")

        # Validate UUIDs
        try:
            product_id_uuid = uuid.UUID(str(row['product_id']))
        except (ValueError, TypeError):
            raise ValueError(f"Invalid product_id format: {row['product_id']}")

        category_id_uuid = None
        if row.get('category_id'):
            try:
                category_id_uuid = uuid.UUID(str(row['category_id']))
            except (ValueError, TypeError):
                raise ValueError(f"Invalid category_id format: {row['category_id']}")

        processed_row = {
            'tenant_id': uuid.UUID(tenant_id),
            'product_id': product_id_uuid,
            'sku': str(row['sku']),
            'name': str(row['name']),
            'price': float(row['price']),
            'category_id': category_id_uuid,
            'active': row.get('active', True),
            'created_at': row.get('created_at', timezone.now().isoformat())
        }

        if processed_row['price'] < 0:
            raise ValueError("price must be non-negative")

        # Ensure field lengths don't exceed limits
        if len(processed_row['sku']) > 100:
            raise ValueError(f"SKU too long: {processed_row['sku']}")
        if len(processed_row['name']) > 255:
            raise ValueError(f"Product name too long: {processed_row['name']}")

        return processed_row

    def _validate_order_row(self, row: Dict, tenant_id: str) -> Dict:
        """Validate and transform order row."""
        required_fields = [
            'order_id', 'external_order_id', 'total_amount',
            'currency', 'order_status', 'order_date'
        ]
        for field in required_fields:
            if field not in row:
                raise ValueError(f"Missing required field: {field}")

        # Validate UUIDs
        try:
            order_id_uuid = uuid.UUID(str(row['order_id']))
        except (ValueError, TypeError):
            raise ValueError(f"Invalid order_id format: {row['order_id']}")

        customer_id_uuid = None
        if row.get('customer_id'):
            try:
                customer_id_uuid = uuid.UUID(str(row['customer_id']))
            except (ValueError, TypeError):
                raise ValueError(f"Invalid customer_id format: {row['customer_id']}")

        processed_row = {
            'tenant_id': uuid.UUID(tenant_id),
            'order_id': order_id_uuid,
            'external_order_id': str(row['external_order_id']),
            'customer_id': customer_id_uuid,
            'customer_name_snapshot': row.get('customer_name_snapshot', ''),
            'customer_email_snapshot': row.get('customer_email_snapshot', ''),
            'total_amount': float(row['total_amount']),
            'currency': str(row['currency']),
            'order_status': str(row['order_status']),
            'order_date': row['order_date'],
            'raw_payload': self._process_raw_payload(row.get('raw_payload', {})),
            'created_at': row.get('created_at', timezone.now().isoformat())
        }

        if processed_row['total_amount'] < 0:
            raise ValueError("total_amount must be non-negative")

        valid_currencies = ['USD', 'EUR', 'GBP', 'CAD']
        if processed_row['currency'] not in valid_currencies:
            raise ValueError(f"Invalid currency: {processed_row['currency']}")
        
        # Ensure currency is exactly 3 characters
        if len(processed_row['currency']) > 3:
            raise ValueError(f"Currency code too long: {processed_row['currency']}")

        valid_statuses = ['pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded']
        if processed_row['order_status'] not in valid_statuses:
            raise ValueError(f"Invalid order_status: {processed_row['order_status']}")
        
        # Ensure order_status doesn't exceed 100 characters
        if len(processed_row['order_status']) > 100:
            raise ValueError(f"Order status too long: {processed_row['order_status']}")
        
        # Ensure external_order_id doesn't exceed 100 characters
        if len(processed_row['external_order_id']) > 100:
            raise ValueError(f"External order ID too long: {processed_row['external_order_id']}")
        
        # Ensure customer name doesn't exceed 255 characters
        if len(processed_row['customer_name_snapshot']) > 255:
            raise ValueError(f"Customer name too long: {processed_row['customer_name_snapshot']}")
        
        # Ensure customer email doesn't exceed 255 characters
        if len(processed_row['customer_email_snapshot']) > 255:
            raise ValueError(f"Customer email too long: {processed_row['customer_email_snapshot']}")

        return processed_row

    def _validate_order_item_row(self, row: Dict, tenant_id: str) -> Dict:
        """Validate and transform order item row."""
        required_fields = ['order_item_id', 'order_id', 'product_id', 'quantity', 'unit_price', 'line_total']
        for field in required_fields:
            if field not in row:
                raise ValueError(f"Missing required field: {field}")

        # Validate UUIDs
        try:
            order_item_id_uuid = uuid.UUID(str(row['order_item_id']))
            order_id_uuid = uuid.UUID(str(row['order_id']))
            product_id_uuid = uuid.UUID(str(row['product_id']))
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid UUID format: {e}")

        processed_row = {
            'tenant_id': uuid.UUID(tenant_id),
            'order_item_id': order_item_id_uuid,
            'order_id': order_id_uuid,
            'product_id': product_id_uuid,
            'quantity': int(row['quantity']),
            'unit_price': float(row['unit_price']),
            'line_total': float(row['line_total'])
        }

        if processed_row['quantity'] < 1:
            raise ValueError("quantity must be at least 1")
        if processed_row['unit_price'] < 0:
            raise ValueError("unit_price must be non-negative")
        if processed_row['line_total'] < 0:
            raise ValueError("line_total must be non-negative")

        return processed_row

    def _process_raw_payload(self, raw_payload) -> dict:
        """Ensure valid JSON object with proper UUID serialization."""
        if raw_payload is None:
            return {}
        elif isinstance(raw_payload, str):
            try:
                return json.loads(raw_payload)
            except json.JSONDecodeError:
                return {'value': raw_payload}
        elif isinstance(raw_payload, dict):
            # Convert UUID objects to strings for JSON serialization
            serializable_payload = {}
            for key, value in raw_payload.items():
                if isinstance(value, uuid.UUID):
                    serializable_payload[key] = str(value)
                elif isinstance(value, dict):
                    serializable_payload[key] = self._process_raw_payload(value)
                elif isinstance(value, list):
                    serializable_payload[key] = [
                        str(item) if isinstance(item, uuid.UUID) else item
                        for item in value
                    ]
                else:
                    serializable_payload[key] = value
            return serializable_payload
        else:
            return {'value': str(raw_payload)}

    def _insert_batch(self, batch_data: List[Dict], chunk_id: str, data_type: str) -> Tuple[int, int, List]:
        """Insert batch into appropriate staging tables."""
        if not batch_data:
            return 0, 0, []

        inserted = 0
        failed = 0
        errors = []

        try:
            with connection.cursor() as cursor:
                if data_type == 'customers':
                    inserted, failed, errors = self._insert_customers_batch(cursor, batch_data, chunk_id)
                elif data_type == 'products':
                    inserted, failed, errors = self._insert_products_batch(cursor, batch_data, chunk_id)
                elif data_type == 'orders':
                    inserted, failed, errors = self._insert_orders_batch(cursor, batch_data, chunk_id)
                elif data_type == 'order_items':
                    inserted, failed, errors = self._insert_order_items_batch(cursor, batch_data, chunk_id)

        except Exception as e:
            logger.error("Database error in _insert_batch: %s", str(e), exc_info=True)

        return inserted, failed, errors

    def _insert_customers_batch(self, cursor, batch_data: List[Dict], chunk_id: str) -> Tuple[int, int, List]:
        """Insert customers batch."""
        inserted = 0
        failed = 0
        errors = []

        for i, row in enumerate(batch_data):
            try:
                values = [
                    row['tenant_id'], row['customer_id'], row['name'], row['email'],
                    json.dumps(row['metadata'], cls=UUIDEncoder), row['created_at'],
                    timezone.now(), uuid.UUID(chunk_id)
                ]
                sql = """
                    INSERT INTO customers_staging (
                        tenant_id, customer_id, name, email, metadata, created_at,
                        processed_at, chunk_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tenant_id, email)
                    DO UPDATE SET
                        customer_id = EXCLUDED.customer_id,
                        name = EXCLUDED.name,
                        metadata = EXCLUDED.metadata,
                        created_at = EXCLUDED.created_at,
                        processed_at = EXCLUDED.processed_at,
                        chunk_id = EXCLUDED.chunk_id
                """
                self._execute_with_retry(cursor, sql, values)
                inserted += 1
            except Exception as e:
                failed += 1
                # Convert UUID objects to strings in error data
                serializable_row = {}
                for key, value in row.items():
                    if isinstance(value, uuid.UUID):
                        serializable_row[key] = str(value)
                    else:
                        serializable_row[key] = value
                errors.append({'row': i, 'error': str(e), 'data': serializable_row})

        return inserted, failed, errors

    def _insert_products_batch(self, cursor, batch_data: List[Dict], chunk_id: str) -> Tuple[int, int, List]:
        """Insert products batch."""
        inserted = 0
        failed = 0
        errors = []

        for i, row in enumerate(batch_data):
            try:
                values = [
                    row['tenant_id'], row['product_id'], row['sku'], row['name'],
                    row['price'], row['category_id'], row['active'], row['created_at'],
                    timezone.now(), uuid.UUID(chunk_id)
                ]
                sql = """
                    INSERT INTO products_staging (
                        tenant_id, product_id, sku, name, price, category_id, active,
                        created_at, processed_at, chunk_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tenant_id, sku)
                    DO UPDATE SET
                        product_id = EXCLUDED.product_id,
                        name = EXCLUDED.name,
                        price = EXCLUDED.price,
                        category_id = EXCLUDED.category_id,
                        active = EXCLUDED.active,
                        created_at = EXCLUDED.created_at,
                        processed_at = EXCLUDED.processed_at,
                        chunk_id = EXCLUDED.chunk_id
                """
                self._execute_with_retry(cursor, sql, values)
                inserted += 1
            except Exception as e:
                failed += 1
                # Convert UUID objects to strings in error data
                serializable_row = {}
                for key, value in row.items():
                    if isinstance(value, uuid.UUID):
                        serializable_row[key] = str(value)
                    else:
                        serializable_row[key] = value
                errors.append({'row': i, 'error': str(e), 'data': serializable_row})

        return inserted, failed, errors

    def _insert_orders_batch(self, cursor, batch_data: List[Dict], chunk_id: str) -> Tuple[int, int, List]:
        """Insert orders batch."""
        inserted = 0
        failed = 0
        errors = []

        for i, row in enumerate(batch_data):
            try:
                values = [
                    row['tenant_id'], row['order_id'], row['external_order_id'],
                    row['customer_id'], row['customer_name_snapshot'],
                    row['customer_email_snapshot'], row['total_amount'],
                    row['currency'], row['order_status'], row['order_date'],
                    json.dumps(row['raw_payload'], cls=UUIDEncoder), row['created_at'],
                    timezone.now(), uuid.UUID(chunk_id)
                ]
                sql = """
                    INSERT INTO orders_staging (
                        tenant_id, order_id, external_order_id, customer_id,
                        customer_name_snapshot, customer_email_snapshot, total_amount,
                        currency, order_status, order_date, raw_payload, created_at,
                        processed_at, chunk_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tenant_id, external_order_id)
                    DO UPDATE SET
                        order_id = EXCLUDED.order_id,
                        customer_id = EXCLUDED.customer_id,
                        customer_name_snapshot = EXCLUDED.customer_name_snapshot,
                        customer_email_snapshot = EXCLUDED.customer_email_snapshot,
                        total_amount = EXCLUDED.total_amount,
                        currency = EXCLUDED.currency,
                        order_status = EXCLUDED.order_status,
                        order_date = EXCLUDED.order_date,
                        raw_payload = EXCLUDED.raw_payload,
                        created_at = EXCLUDED.created_at,
                        processed_at = EXCLUDED.processed_at,
                        chunk_id = EXCLUDED.chunk_id
                """
                self._execute_with_retry(cursor, sql, values)
                inserted += 1
            except Exception as e:
                failed += 1
                # Convert UUID objects to strings in error data
                serializable_row = {}
                for key, value in row.items():
                    if isinstance(value, uuid.UUID):
                        serializable_row[key] = str(value)
                    else:
                        serializable_row[key] = value
                errors.append({'row': i, 'error': str(e), 'data': serializable_row})

        return inserted, failed, errors

    def _insert_order_items_batch(self, cursor, batch_data: List[Dict], chunk_id: str) -> Tuple[int, int, List]:
        """Insert order items batch."""
        inserted = 0
        failed = 0
        errors = []

        for i, row in enumerate(batch_data):
            try:
                values = [
                    row['tenant_id'], row['order_item_id'], row['order_id'],
                    row['product_id'], row['quantity'], row['unit_price'],
                    row['line_total'], timezone.now(), uuid.UUID(chunk_id)
                ]
                sql = """
                    INSERT INTO order_items_staging (
                        tenant_id, order_item_id, order_id, product_id,
                        quantity, unit_price, line_total, processed_at, chunk_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_item_id)
                    DO UPDATE SET
                        order_id = EXCLUDED.order_id,
                        product_id = EXCLUDED.product_id,
                        quantity = EXCLUDED.quantity,
                        unit_price = EXCLUDED.unit_price,
                        line_total = EXCLUDED.line_total,
                        processed_at = EXCLUDED.processed_at,
                        chunk_id = EXCLUDED.chunk_id
                """
                self._execute_with_retry(cursor, sql, values)
                inserted += 1
            except Exception as e:
                failed += 1
                # Convert UUID objects to strings in error data
                serializable_row = {}
                for key, value in row.items():
                    if isinstance(value, uuid.UUID):
                        serializable_row[key] = str(value)
                    else:
                        serializable_row[key] = value
                errors.append({'row': i, 'error': str(e), 'data': serializable_row})

        return inserted, failed, errors

    def promote_staging_data(self, tenant_id: str, chunk_id: str, data_type: str) -> Dict[str, Any]:
        """Promote staging data to production tables in dependency order."""
        start_time = time.time()
        
        with transaction.atomic():
            with connection.cursor() as cursor:
                if data_type == 'customers':
                    return self._promote_customers(cursor, tenant_id, chunk_id, start_time)
                elif data_type == 'products':
                    return self._promote_products(cursor, tenant_id, chunk_id, start_time)
                elif data_type == 'orders':
                    return self._promote_orders(cursor, tenant_id, chunk_id, start_time)
                elif data_type == 'order_items':
                    return self._promote_order_items(cursor, tenant_id, chunk_id, start_time)
                else:
                    raise ValueError(f"Unknown data type: {data_type}")

    def _promote_customers(self, cursor, tenant_id: str, chunk_id: str, start_time: float) -> Dict[str, Any]:
        """Promote customers from staging to production."""
        try:
            self._execute_with_retry(cursor, """
                INSERT INTO customers (
                    customer_id, tenant_id, name, email, metadata, created_at
                )
                SELECT customer_id, tenant_id, name, email, metadata, created_at
                FROM customers_staging
                WHERE tenant_id = %s AND chunk_id = %s
                ON CONFLICT (tenant_id, email)
                DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    name = EXCLUDED.name,
                    metadata = EXCLUDED.metadata,
                    created_at = EXCLUDED.created_at
            """, [tenant_id, chunk_id])
            customers_inserted = cursor.rowcount

            # Mark staging rows processed
            self._execute_with_retry(cursor, """
                UPDATE customers_staging
                SET processed_at = %s
                WHERE tenant_id = %s AND chunk_id = %s
            """, [timezone.now(), tenant_id, chunk_id])

            return {
                'customers_promoted': customers_inserted,
                'processing_time': time.time() - start_time
            }
        except Exception as e:
            logger.exception("Failed promoting customers: %s", e)
            raise

    def _promote_products(self, cursor, tenant_id: str, chunk_id: str, start_time: float) -> Dict[str, Any]:
        """Promote products from staging to production."""
        try:
            self._execute_with_retry(cursor, """
                INSERT INTO products (
                    product_id, tenant_id, sku, name, price, category_id, active, created_at
                )
                SELECT product_id, tenant_id, sku, name, price, category_id, active, created_at
                FROM products_staging
                WHERE tenant_id = %s AND chunk_id = %s
                ON CONFLICT (tenant_id, sku)
                DO UPDATE SET
                    product_id = EXCLUDED.product_id,
                    name = EXCLUDED.name,
                    price = EXCLUDED.price,
                    category_id = EXCLUDED.category_id,
                    active = EXCLUDED.active,
                    created_at = EXCLUDED.created_at
            """, [tenant_id, chunk_id])
            products_inserted = cursor.rowcount

            # Mark staging rows processed
            self._execute_with_retry(cursor, """
                UPDATE products_staging
                SET processed_at = %s
                WHERE tenant_id = %s AND chunk_id = %s
            """, [timezone.now(), tenant_id, chunk_id])

            return {
                'products_promoted': products_inserted,
                'processing_time': time.time() - start_time
            }
        except Exception as e:
            logger.exception("Failed promoting products: %s", e)
            raise

    def _promote_orders(self, cursor, tenant_id: str, chunk_id: str, start_time: float) -> Dict[str, Any]:
        """Promote orders from staging to production."""
        try:
            self._execute_with_retry(cursor, """
            INSERT INTO orders (
                order_id, tenant_id, external_order_id, customer,
                customer_name_snapshot, customer_email_snapshot, total_amount,
                currency, order_status, order_date, raw_payload, created_at
            )
            SELECT order_id, tenant_id, external_order_id, customer_id,
                   customer_name_snapshot, customer_email_snapshot, total_amount,
                   currency, order_status, order_date, raw_payload, created_at
            FROM orders_staging
            WHERE tenant_id = %s AND chunk_id = %s
            ON CONFLICT (tenant_id, external_order_id)
            DO UPDATE SET
                order_id = EXCLUDED.order_id,
                customer = EXCLUDED.customer,
                customer_name_snapshot = EXCLUDED.customer_name_snapshot,
                customer_email_snapshot = EXCLUDED.customer_email_snapshot,
                total_amount = EXCLUDED.total_amount,
                currency = EXCLUDED.currency,
                order_status = EXCLUDED.order_status,
                order_date = EXCLUDED.order_date,
                raw_payload = EXCLUDED.raw_payload,
                created_at = EXCLUDED.created_at
            """, [tenant_id, chunk_id])
            orders_inserted = cursor.rowcount

            # Mark staging rows processed
            self._execute_with_retry(cursor, """
                UPDATE orders_staging
                SET processed_at = %s
                WHERE tenant_id = %s AND chunk_id = %s
            """, [timezone.now(), tenant_id, chunk_id])

            return {
                'orders_promoted': orders_inserted,
                'processing_time': time.time() - start_time
            }
        except Exception as e:
            logger.exception("Failed promoting orders: %s", e)
            raise

    def _promote_order_items(self, cursor, tenant_id: str, chunk_id: str, start_time: float) -> Dict[str, Any]:
        """Promote order items from staging to production."""
        try:
            self._execute_with_retry(cursor, """
            INSERT INTO order_items (
                order_item_id, "order", tenant_id, "product",
                quantity, unit_price, line_total
            )
            SELECT order_item_id, order_id, tenant_id, product_id,
                   quantity, unit_price, line_total
            FROM order_items_staging
            WHERE tenant_id = %s AND chunk_id = %s
            ON CONFLICT (order_item_id)
            DO UPDATE SET
                "order" = EXCLUDED."order",
                "product" = EXCLUDED."product",
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                line_total = EXCLUDED.line_total
            """, [tenant_id, chunk_id])
            items_inserted = cursor.rowcount
    
            # Mark staging rows processed
            self._execute_with_retry(cursor, """
                UPDATE order_items_staging
                SET processed_at = %s
                WHERE tenant_id = %s AND chunk_id = %s
            """, [timezone.now(), tenant_id, chunk_id])
    
            return {
                'items_promoted': items_inserted,
                'processing_time': time.time() - start_time
            }
        except Exception as e:
            logger.exception("Failed promoting order items: %s", e)
            raise
