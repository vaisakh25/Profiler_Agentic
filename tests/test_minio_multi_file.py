#!/usr/bin/env python3
"""
Test multi-file MinIO profiling with the new profile_multiple_remote_files tool.
"""

from __future__ import annotations

import os
import sys
import pandas as pd
from pathlib import Path
import pytest

# Add parent dir to path
sys.path.insert(0, str(Path(__file__).parent))

from file_profiler.config import env


@pytest.fixture(scope="module")
def file_uris() -> list[str]:
    """Upload sample files for pytest runs and clean them up after tests."""
    try:
        uris = upload_sample_files()
    except Exception as exc:
        pytest.skip(f"MinIO setup unavailable; skipping multi-file MinIO tests: {exc}")
    if not uris:
        pytest.skip("MinIO setup unavailable; skipping multi-file MinIO tests")
    try:
        yield uris
    finally:
        cleanup(uris)


def upload_sample_files():
    """Upload multiple sample CSV files to MinIO for testing."""
    print("=" * 70)
    print("Uploading Sample Files to MinIO")
    print("=" * 70)
    
    try:
        import boto3
        from botocore.client import Config
    except ImportError:
        print("❌ boto3 not installed")
        return None

    # Create S3 client for MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url=env.MINIO_ENDPOINT_URL,
        aws_access_key_id=env.MINIO_ACCESS_KEY,
        aws_secret_access_key=env.MINIO_SECRET_KEY,
        region_name=env.MINIO_REGION,
        config=Config(signature_version='s3v4'),
    )
    
    bucket_name = env.MINIO_BUCKET_NAME or 'data-files'
    
    # Ensure bucket exists
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"📦 Created bucket '{bucket_name}'")

    # Sample data 1: Sales Customers
    customers_df = pd.DataFrame({
        'CustomerID': [1, 2, 3, 4, 5],
        'CustomerName': ['Acme Corp', 'TechStart Inc', 'Global Traders', 'Local Shop', 'BigBox Retail'],
        'CustomerCategoryName': ['Corporate', 'Startup', 'Wholesale', 'Retail', 'Chain'],
        'PrimaryContact': ['John Smith', 'Jane Doe', 'Bob Wilson', 'Alice Brown', 'Charlie Davis'],
        'PhoneNumber': ['+1-555-1001', '+1-555-1002', '+1-555-1003', '+1-555-1004', '+1-555-1005'],
        'PostalCityName': ['New York', 'San Francisco', 'Chicago', 'Boston', 'Los Angeles'],
    })

    # Sample data 2: Sales Orders
    orders_df = pd.DataFrame({
        'OrderID': [101, 102, 103, 104, 105, 106, 107],
        'CustomerID': [1, 1, 2, 3, 3, 4, 5],
        'OrderDate': ['2024-01-15', '2024-01-20', '2024-01-22', '2024-01-25', '2024-02-01', '2024-02-03', '2024-02-10'],
        'TotalAmount': [1250.50, 890.00, 3400.75, 560.25, 2100.00, 125.50, 8900.00],
        'Status': ['Completed', 'Completed', 'Processing', 'Shipped', 'Completed', 'Pending', 'Completed'],
    })

    # Sample data 3: Sales OrderLines
    order_lines_df = pd.DataFrame({
        'OrderLineID': [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009],
        'OrderID': [101, 101, 102, 103, 103, 104, 105, 106, 107],
        'ProductID': [201, 202, 201, 203, 204, 201, 205, 201, 206],
        'ProductName': ['Widget A', 'Widget B', 'Widget A', 'Gadget X', 'Gadget Y', 'Widget A', 'Tool Z', 'Widget A', 'Premium Kit'],
        'Quantity': [10, 5, 8, 15, 20, 3, 25, 1, 50],
        'UnitPrice': [50.00, 75.00, 50.00, 120.00, 85.00, 50.00, 60.00, 50.00, 150.00],
        'LineTotal': [500.00, 375.00, 400.00, 1800.00, 1700.00, 150.00, 1500.00, 50.00, 7500.00],
    })

    # Upload files
    files_uploaded = []
    
    datasets = [
        ('sales/customers.csv', customers_df),
        ('sales/orders.csv', orders_df),
        ('sales/order_lines.csv', order_lines_df),
    ]

    print(f"\n📤 Uploading files to bucket '{bucket_name}'...")
    
    for key, df in datasets:
        csv_content = df.to_csv(index=False)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=csv_content.encode('utf-8'),
            ContentType='text/csv'
        )
        uri = f"minio://{bucket_name}/{key}"
        files_uploaded.append(uri)
        print(f"  ✅ {key} ({len(df)} rows, {len(df.columns)} columns) -> {uri}")

    print(f"\n✅ Uploaded {len(files_uploaded)} files")
    return files_uploaded


def test_multi_file_profiling(file_uris: list[str]):
    """Test profiling multiple files using the connector MCP server."""
    print("\n" + "=" * 70)
    print("Testing Multi-File Profiling")
    print("=" * 70)

    try:
        from file_profiler.main import profile_remote
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

    try:
        print(f"\n📊 Profiling {len(file_uris)} files...")
        
        profiles = []
        for i, uri in enumerate(file_uris, 1):
            file_name = uri.split("/")[-1]
            print(f"\n  [{i}/{len(file_uris)}] Profiling {file_name}...")
            
            result = profile_remote(
                uri=uri,
                connection_id=None,  # Use env vars
                table_filter=None,
                output_dir=None,  # Don't write to disk for test
            )
            
            # Normalize to list
            file_profiles = result if isinstance(result, list) else [result]
            profiles.extend(file_profiles)
            
            for profile in file_profiles:
                print(f"      ✅ {profile.table_name}")
                print(f"         Rows: {profile.row_count}")
                print(f"         Columns: {len(profile.columns)}")
                print(f"         Size: {profile.size_strategy}")

        print(f"\n✅ Successfully profiled {len(profiles)} file(s)")
        
        # Display summary
        print("\n" + "-" * 70)
        print("Profile Summary")
        print("-" * 70)
        
        for profile in profiles:
            print(f"\n📊 {profile.table_name}")
            print(f"   Rows: {profile.row_count:,}")
            print(f"   Columns: {len(profile.columns)}")
            print(f"   Columns:")
            for col in profile.columns[:5]:  # Show first 5 columns
                print(f"      - {col.name} ({col.inferred_type.name})")
                if col.distinct_count:
                    print(f"        Distinct: {col.distinct_count}")

        return True

    except Exception as e:
        print(f"\n❌ Profiling failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_relationship_detection(file_uris: list[str]):
    """Test if relationships can be detected between the profiled files."""
    print("\n" + "=" * 70)
    print("Testing Relationship Detection")
    print("=" * 70)

    try:
        from file_profiler.main import profile_remote
        from file_profiler.relationships.detector import detect_relationships
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

    try:
        print(f"\n📊 Profiling {len(file_uris)} files for relationship detection...")
        
        profiles = []
        for uri in file_uris:
            result = profile_remote(uri=uri, connection_id=None, table_filter=None, output_dir=None)
            file_profiles = result if isinstance(result, list) else [result]
            profiles.extend(file_profiles)

        print(f"✅ Profiled {len(profiles)} files")
        
        print(f"\n🔍 Detecting relationships...")
        report = detect_relationships(profiles)
        
        print(f"\n✅ Found {len(report.candidates)} relationship candidate(s)")
        
        if report.candidates:
            print("\n" + "-" * 70)
            print("Top Relationships")
            print("-" * 70)
            
            for i, rel in enumerate(report.candidates[:5], 1):
                print(f"\n{i}. {rel.source_table}.{rel.source_column} → {rel.target_table}.{rel.target_column}")
                print(f"   Confidence: {rel.confidence:.2%}")
                print(f"   Type: {rel.relationship_type}")
                if rel.reasoning:
                    print(f"   Reasoning: {rel.reasoning[:100]}...")

        return True

    except Exception as e:
        print(f"\n❌ Relationship detection failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def cleanup(file_uris: list[str]):
    """Clean up test files from MinIO."""
    print("\n" + "=" * 70)
    print("Cleaning Up Test Files")
    print("=" * 70)

    try:
        import boto3
        from botocore.client import Config
    except ImportError:
        return

    s3_client = boto3.client(
        's3',
        endpoint_url=env.MINIO_ENDPOINT_URL,
        aws_access_key_id=env.MINIO_ACCESS_KEY,
        aws_secret_access_key=env.MINIO_SECRET_KEY,
        region_name=env.MINIO_REGION,
        config=Config(signature_version='s3v4'),
    )

    bucket_name = env.MINIO_BUCKET_NAME or 'data-files'

    print(f"\n🗑️  Deleting test files from '{bucket_name}'...")
    
    for uri in file_uris:
        # Extract key from URI (minio://bucket/key)
        key = uri.split(f"{bucket_name}/", 1)[1] if f"{bucket_name}/" in uri else uri.split("/")[-1]
        try:
            s3_client.delete_object(Bucket=bucket_name, Key=key)
            print(f"  ✅ Deleted {key}")
        except Exception as e:
            print(f"  ⚠️  Failed to delete {key}: {e}")

    print("\n✅ Cleanup complete")


def main():
    """Run all multi-file MinIO tests."""
    print("\n🚀 Multi-File MinIO Profiling Test Suite\n")

    # Upload sample files
    file_uris = upload_sample_files()
    if not file_uris:
        print("❌ Failed to upload files")
        return 1

    # Test multi-file profiling
    success = test_multi_file_profiling(file_uris)
    if not success:
        cleanup(file_uris)
        return 1

    # Test relationship detection
    test_relationship_detection(file_uris)

    # Cleanup
    cleanup(file_uris)

    print("\n" + "=" * 70)
    print("🎉 All Tests Complete!")
    print("=" * 70)
    print("\nYou can now use profile_multiple_remote_files to profile")
    print("multiple MinIO files in a single operation:")
    print("\n  URIs: [")
    print("    'minio://bucket/file1.csv',")
    print("    'minio://bucket/file2.csv',")
    print("    'minio://bucket/file3.parquet'")
    print("  ]")
    print("=" * 70)

    return 0


if __name__ == "__main__":
    sys.exit(main())
