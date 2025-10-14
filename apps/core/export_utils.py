import csv
import gzip
import io
import os
from typing import Iterable, Dict, Optional, Tuple, List

from django.conf import settings


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)


def get_export_file_path(tenant_id: str, export_id: str, fmt: str) -> str:
    base_dir = getattr(settings, 'MEDIA_ROOT', '.')
    if fmt == 'csv':
        return os.path.join(base_dir, 'exports', str(tenant_id), f'{export_id}.csv.gz')
    else:
        return os.path.join(base_dir, 'exports', str(tenant_id), f'{export_id}.parquet')


def write_csv_gzip_incremental(
    file_path: str,
    rows: Iterable[Dict],
    header: Optional[Tuple[str, ...]] = None,
    checkpoint_bytes: int = 1_000_000,
    resume: bool = False,
) -> Tuple[int, int]:
    """
    Stream rows to a gzip-compressed CSV without loading all data into RAM.
    Supports resuming by appending to existing file.

    Returns (bytes_written_delta, rows_written_delta).
    """
    ensure_parent_dir(file_path)

    bytes_before = os.path.getsize(file_path) if (resume and os.path.exists(file_path)) else 0
    rows_written = 0
    bytes_written = 0

    # Open gzip in append or write mode
    mode = 'ab' if resume and os.path.exists(file_path) else 'wb'
    with gzip.open(file_path, mode) as gz:
        text_stream = io.TextIOWrapper(gz, encoding='utf-8', newline='')
        writer = csv.writer(text_stream)

        wrote_header = False
        if not resume or bytes_before == 0:
            if header:
                writer.writerow(header)
                wrote_header = True

        for row in rows:
            if not header and not wrote_header:
                header = tuple(row.keys())
                writer.writerow(header)
                wrote_header = True
            writer.writerow([row.get(col, '') for col in header])
            rows_written += 1

            if rows_written % 1000 == 0:
                text_stream.flush()
                gz.flush()
                # update bytes written roughly by checking current size
                current_size = bytes_before + gz.fileobj.tell()
                if current_size - bytes_before >= checkpoint_bytes:
                    pass

        text_stream.flush()
        gz.flush()

    final_size = os.path.getsize(file_path)
    bytes_written = final_size - bytes_before
    return bytes_written, rows_written


def write_parquet_file(
    file_path: str,
    rows: Iterable[Dict],
    batch_size: int = 5000,
) -> Tuple[int, int]:
    """
    Stream rows to a Parquet file using pyarrow with column compression (gzip).
    Overwrites any existing file atomically using a temp file then renames.

    Returns (bytes_written, rows_written).
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except Exception as e:
        raise RuntimeError("parquet dependencies missing; install pyarrow") from e

    ensure_parent_dir(file_path)
    tmp_path = file_path + ".tmp"
    rows_written = 0

    # Remove temp if exists
    if os.path.exists(tmp_path):
        os.remove(tmp_path)

    writer = None
    try:
        batch_rows: List[Dict] = []
        for row in rows:
            batch_rows.append(row)
            if len(batch_rows) >= batch_size:
                table = pa.Table.from_pylist(batch_rows)
                if writer is None:
                    writer = pq.ParquetWriter(tmp_path, table.schema, compression='gzip')
                writer.write_table(table)
                rows_written += len(batch_rows)
                batch_rows.clear()
        if batch_rows:
            table = pa.Table.from_pylist(batch_rows)
            if writer is None:
                writer = pq.ParquetWriter(tmp_path, table.schema, compression='gzip')
            writer.write_table(table)
            rows_written += len(batch_rows)
            batch_rows.clear()
    finally:
        if writer is not None:
            writer.close()

    os.replace(tmp_path, file_path)
    bytes_written = os.path.getsize(file_path)
    return bytes_written, rows_written


