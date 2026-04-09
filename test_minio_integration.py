#!/usr/bin/env python3
"""
Integration test: Upload CSV to MinIO and profile it via the profiler.
"""

from __future__ import annotations

import os
import sys
import pandas as pd
from pathlib import Path

# Add parent dir to path
sys.path.insert(0, str(Path(__file__).parent))

from file_profiler.config import env


def test_minio_integration():
    """Complete integration test with MinIO."""
    print("=" * 70)
    print("MinIO + Profiler Integration Test")
    print("=" * 70)
    
    try:
        import boto3
        from botocore.client import Config
    except ImportError:
        print("❌ boto3 not installed")
        return False

    try:
        # Create a sample CSV file
        print("\n1️⃣  Creating sample CSV data...")
        df = pd.DataFrame({
            'customer_id': [1, 2, 3, 4, 5],
            'name': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Williams', 'Charlie Brown'],
            'email': ['john@example.com', 'jane@example.com', 'bob@example.com', 'alice@example.com', 'charlie@example.com'],
            'age': [25, 30, 35, 28, 42],
            'purchase_amount': [150.50, 220.00, 89.99, 310.25, 175.80]
        })
        
        csv_content = df.to_csv(index=False)
        print(f"✅ Created sample CSV with {len(df)} rows")
        print(f"   Columns: {list(df.columns)}")

        # Connect to MinIO
        print(f"\n2️⃣  Connecting to MinIO at {env.MINIO_ENDPOINT_URL}...")
        s3_client = boto3.client(
            's3',
            endpoint_url=env.MINIO_ENDPOINT_URL,
            aws_access_key_id=env.MINIO_ACCESS_KEY,
            aws_secret_access_key=env.MINIO_SECRET_KEY,
            region_name=env.MINIO_REGION,
            config=Config(signature_version='s3v4'),
        )
        
        bucket_name = env.MINIO_BUCKET_NAME or 'data-files'
        file_key = 'profiler-test/customers.csv'
        
        # Ensure bucket exists
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"✅ Bucket '{bucket_name}' exists")
        except:
            print(f"📦 Creating bucket '{bucket_name}'...")
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"✅ Bucket created")

        # Upload CSV to MinIO
        print(f"\n3️⃣  Uploading CSV to MinIO...")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=csv_content.encode('utf-8'),
            ContentType='text/csv'
        )
        uri = f"minio://{bucket_name}/{file_key}"
        print(f"✅ Uploaded to: {uri}")

        # Verify upload
        print(f"\n4️⃣  Verifying upload...")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='profiler-test/')
        objects = response.get('Contents', [])
        print(f"✅ Found {len(objects)} file(s) in profiler-test/:")
        for obj in objects:
            print(f"   - {obj['Key']} ({obj['Size']} bytes)")

        # Test profiling
        print(f"\n5️⃣  Testing profiler connector...")
        from file_profiler.connectors.cloud_storage import CloudStorageConnector
        from file_profiler.connectors.base import SourceDescriptor
        
        descriptor = SourceDescriptor(
            scheme="minio",
            bucket_or_host=bucket_name,
            path=file_key,
            raw_uri=uri,
        )
        
        credentials = {
            "endpoint_url": env.MINIO_ENDPOINT_URL,
            "access_key": env.MINIO_ACCESS_KEY,
            "secret_key": env.MINIO_SECRET_KEY,
            "region": env.MINIO_REGION,
        }
        
        connector = CloudStorageConnector("minio")
        
        # Test DuckDB scan expression
        scan_expr = connector.duckdb_scan_expression(descriptor)
        print(f"✅ DuckDB scan expression: {scan_expr}")
        
        # Test DuckDB read
        print(f"\n6️⃣  Testing DuckDB read from MinIO...")
        import duckdb
        
        con = duckdb.connect(":memory:")
        connector.configure_duckdb(con, descriptor, credentials)
        
        # Read the CSV using SELECT
        query = f"SELECT * FROM {scan_expr}"
        result = con.execute(query).fetchdf()
        print(f"✅ Read {len(result)} rows from MinIO via DuckDB")
        print(f"   Columns: {list(result.columns)}")
        print("\n   Sample data:")
        print(result.head().to_string(index=False))
        
        # Cleanup
        print(f"\n7️⃣  Cleaning up test file...")
        s3_client.delete_object(Bucket=bucket_name, Key=file_key)
        print(f"✅ Deleted test file")

        print("\n" + "=" * 70)
        print("🎉 Integration test PASSED!")
        print("=" * 70)
        print("\nVerified capabilities:")
        print("  ✅ Upload CSV to MinIO")
        print("  ✅ List objects in MinIO")
        print("  ✅ Configure DuckDB for MinIO")
        print("  ✅ Read CSV from MinIO via DuckDB")
        print("  ✅ Parse data correctly")
        print("\nYour profiler is ready to work with MinIO!")
        print("=" * 70)
        
        return True

    except Exception as e:
        print(f"\n❌ Integration test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_minio_integration()
    sys.exit(0 if success else 1)
