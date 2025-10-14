# Data Generation Script

## Overview

The `gen_dataset.py` script generates realistic synthetic data for testing and benchmarking multi-tenant e-commerce systems. It creates comprehensive datasets with configurable sizes and formats.

## Features

- **Synthetic Data Generation**: Creates realistic e-commerce data using Faker library
- **Multiple Formats**: Supports CSV and NDJSON output formats
- **Chunked Output**: Automatically splits large datasets into manageable chunks
- **Compression**: Optional GZIP compression for reduced file sizes
- **Preset Configurations**: Quick setup with small, medium, and large presets
- **Bulk Upload**: Optional direct upload to API endpoints
- **Performance Metrics**: Real-time throughput measurement and reporting
- **Reproducibility**: Seed-based generation for consistent results

## Installation

```bash
# Install required dependencies
pip install faker requests

# Make script executable (Linux/Mac)
chmod +x gen_dataset.py
```

## Usage Examples

### Basic Usage

```bash
# Generate small dataset (2 tenants, 1K products, 5K orders)
python gen_dataset.py --preset small

# Generate medium dataset with CSV format
python gen_dataset.py --preset medium --format csv

# Generate large dataset with compression and chunking
python gen_dataset.py --preset large --format ndjson --compress --chunk-size 10000
```

### Custom Configuration

```bash
# Generate custom dataset
python gen_dataset.py \
  --tenants 5 \
  --products-per-tenant 100000 \
  --orders-per-tenant 500000 \
  --customers-per-tenant 10000

# Generate with specific output directory
python gen_dataset.py --preset medium --output-dir ./my_data

# Generate with specific random seed
python gen_dataset.py --preset small --seed 12345
```

### Upload to API

```bash
# Generate and upload directly
python gen_dataset.py \
  --preset medium \
  --upload \
  --api-url http://localhost:8000 \
  --api-key your-api-key

# Verbose mode for debugging
python gen_dataset.py --preset small --upload --verbose
```

## Command Line Arguments

### Data Size Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--tenants` | int | 10 | Number of tenants to generate |
| `--products-per-tenant` | int | 500000 | Number of products per tenant |
| `--orders-per-tenant` | int | 2000000 | Number of orders per tenant |
| `--customers-per-tenant` | int | 10000 | Number of customers per tenant |

### Preset Configurations

| Preset | Tenants | Products | Orders | Customers |
|--------|---------|----------|--------|-----------|
| `small` | 2 | 1,000 | 5,000 | 100 |
| `medium` | 5 | 50,000 | 100,000 | 1,000 |
| `large` | 10 | 500,000 | 2,000,000 | 10,000 |

### Output Options

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--format` | choice | ndjson | Output format: `csv` or `ndjson` |
| `--compress` | flag | False | Enable GZIP compression |
| `--chunk-size` | int | 10000 | Number of records per chunk file |
| `--output-dir` | string | generated_data | Output directory path |

### Upload Options

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--upload` | flag | False | Upload generated data to API |
| `--api-url` | string | http://localhost:8000 | API base URL |
| `--api-key` | string | None | API key for authentication |

### Performance Options

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--seed` | int | 42 | Random seed for reproducibility |
| `--verbose` | flag | False | Enable verbose logging |

## Generated Data Structure

### Tenants
```json
{
  "tenant_id": "uuid",
  "name": "Company Name LLC",
  "api_key_hash": "sha256_hash",
  "created_at": "2024-01-01T00:00:00"
}
```

### Products
```json
{
  "product_id": "uuid",
  "tenant_id": "uuid",
  "sku": "ELE000001",
  "name": "Premium Electronics Product",
  "price": 299.99,
  "category_id": "uuid",
  "active": true,
  "created_at": "2024-01-01T00:00:00"
}
```

### Customers
```json
{
  "customer_id": "uuid",
  "tenant_id": "uuid",
  "name": "John Doe",
  "email": "john.doe@example.com",
  "metadata": {
    "phone": "+1-555-0100",
    "address": "123 Main St",
    "loyalty_tier": "gold",
    "signup_source": "web"
  },
  "created_at": "2024-01-01T00:00:00"
}
```

### Orders
```json
{
  "order_id": "uuid",
  "tenant_id": "uuid",
  "external_order_id": "EXT12345678",
  "customer_id": "uuid",
  "customer_name_snapshot": "John Doe",
  "customer_email_snapshot": "john.doe@example.com",
  "total_amount": 599.98,
  "currency": "USD",
  "order_status": "delivered",
  "order_date": "2024-01-01T00:00:00",
  "raw_payload": {
    "source": "web",
    "campaign": "summer_sale",
    "discount_code": "SAVE20"
  },
  "created_at": "2024-01-01T00:00:00"
}
```

### Order Items
```json
{
  "order_item_id": "uuid",
  "order_id": "uuid",
  "tenant_id": "uuid",
  "product_id": "uuid",
  "quantity": 2,
  "unit_price": 299.99,
  "line_total": 599.98
}
```

### Price History
```json
{
  "id": "uuid",
  "tenant_id": "uuid",
  "product_id": "uuid",
  "price": 299.99,
  "effective_from": "2024-01-01T00:00:00",
  "effective_to": null
}
```

### Stock Events
```json
{
  "stock_event_id": "uuid",
  "tenant_id": "uuid",
  "product_id": "uuid",
  "delta": 50,
  "resulting_level": 150,
  "event_time": "2024-01-01T00:00:00",
  "source": "receipt",
  "meta": {
    "reason": "Weekly stock replenishment",
    "operator": "Jane Smith"
  }
}
```

## Output Files

The script generates the following file structure:

```
generated_data/
├── tenants.csv                           # All tenant records
├── products_<tenant_id>_chunk_0000.ndjson
├── products_<tenant_id>_chunk_0001.ndjson
├── customers_<tenant_id>.ndjson
├── orders_<tenant_id>_chunk_0000.ndjson
├── orders_<tenant_id>_chunk_0001.ndjson
├── order_items_<tenant_id>_chunk_0000.ndjson
├── price_history_<tenant_id>_chunk_0000.ndjso