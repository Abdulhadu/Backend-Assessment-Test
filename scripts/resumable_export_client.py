#!/usr/bin/env python3
"""
Simple resumable downloader for export files supporting HTTP Range requests.

Usage:
  # CSV (gz)
  python scripts/resumable_export_client.py --url http://127.0.0.1:8000/api/v1/tenants/<tenant_id>/reports/export/<export_id>/download --out export.csv.gz
  # Parquet
  python scripts/resumable_export_client.py --url http://127.0.0.1:8000/api/v1/tenants/<tenant_id>/reports/export/<export_id>/download --out export.parquet
"""
import argparse
import os
import sys
import time
import requests


def download_resumable(url: str, out_path: str, chunk_size: int = 256 * 1024):
    temp_path = out_path + ".part"
    downloaded = 0
    if os.path.exists(temp_path):
        downloaded = os.path.getsize(temp_path)

    headers = {}
    if downloaded > 0:
        headers["Range"] = f"bytes={downloaded}-"

    with requests.get(url, headers=headers, stream=True) as r:
        r.raise_for_status()
        mode = "ab" if downloaded > 0 else "wb"
        with open(temp_path, mode) as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue
                f.write(chunk)
                downloaded += len(chunk)
                if downloaded % (1024 * 1024) == 0:
                    print(f"Downloaded ~{downloaded // (1024*1024)} MiB", flush=True)

    os.replace(temp_path, out_path)
    print(f"Saved to {out_path}")


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)

    download_resumable(args.url, args.out)


if __name__ == "__main__":
    sys.exit(main())


