#!/usr/bin/env python3
"""Test MinIO connection and basic operations."""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Add parent dir to path to import file_profiler modules
sys.path.insert(0, str(Path(__file__).parent))

from file_profiler.config import env
from file_profiler.connectors.base import SourceDescriptor
from file_profiler.connectors.cloud_storage import CloudStorageConnector
from file_profiler.connectors.connection_manager import ConnectionManager


def test_minio_config():
    """Display MinIO configuration from environment."""
    print("=" * 70)
    print("MinIO Configuration")
    print("=" * 70)
    print(f"MINIO_ROOT_USER: {env.MINIO_ROOT_USER}")
    print(f"MINIO_ROOT_PASSWORD: {'*' * len(env.MINIO_ROOT_PASSWORD)}")
    print(f"MINIO_ENDPOINT_URL: {env.MINIO_ENDPOINT_URL}")
    print(f"MINIO_ENDPOINT: {env.MINIO_ENDPOINT}")
    print(f"MINIO_HOST: {env.MINIO_HOST}")
    print(f"MINIO_PORT: {env.MINIO_PORT}")
    print(f"MINIO_CONSOLE_PORT: {env.MINIO_CONSOLE_PORT}")
    print(f"MINIO_ACCESS_KEY: {env.MINIO_ACCESS_KEY}")
    print(f"MINIO_SECRET_KEY: {'*' * len(env.MINIO_SECRET_KEY)}")
    print(f"MINIO_BUCKET_NAME: {env.MINIO_BUCKET_NAME}")
    print(f"MINIO_REGION: {env.MINIO_REGION}")
    print(f"MINIO_TEST_BUCKET: {env.MINIO_TEST_BUCKET}")
    print()


def test_boto3_connection():
    """Test MinIO connection using boto3 directly."""
    print("=" * 70)
    print("Testing MinIO Connection with boto3")
    print("=" * 70)
    
    try:
        import boto3
        from botocore.client import Config
        from botocore.exceptions import ClientError
    except ImportError:
        print("❌ boto3 not installed. Install with: pip install boto3")
        return False

    try:
        # Create S3 client configured for MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url=env.MINIO_ENDPOINT_URL,
            aws_access_key_id=env.MINIO_ACCESS_KEY,
            aws_secret_access_key=env.MINIO_SECRET_KEY,
            region_name=env.MINIO_REGION,
            config=Config(signature_version='s3v4'),
        )

        # Test 1: List buckets
        print("\n📋 Listing buckets...")
        response = s3_client.list_buckets()
        buckets = response.get('Buckets', [])
        
        if buckets:
            print(f"✅ Found {len(buckets)} bucket(s):")
            for bucket in buckets:
                print(f"   - {bucket['Name']}")
        else:
            print("ℹ️  No buckets found")

        # Test 2: Create test bucket if it doesn't exist
        bucket_name = env.MINIO_BUCKET_NAME or env.MINIO_TEST_BUCKET or "data-files"
        print(f"\n🪣 Checking/creating bucket: {bucket_name}...")
        
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"✅ Bucket '{bucket_name}' already exists")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == '404':
                print(f"📦 Creating bucket '{bucket_name}'...")
                s3_client.create_bucket(Bucket=bucket_name)
                print(f"✅ Bucket '{bucket_name}' created successfully")
            else:
                raise

        # Test 3: Upload a test file
        print(f"\n📤 Uploading test file to '{bucket_name}'...")
        test_content = b"MinIO connection test - success!"
        test_key = "test/connection_test.txt"
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=test_key,
            Body=test_content
        )
        print(f"✅ Uploaded test file: s3://{bucket_name}/{test_key}")

        # Test 4: Read the test file back
        print(f"\n📥 Reading test file from '{bucket_name}'...")
        response = s3_client.get_object(Bucket=bucket_name, Key=test_key)
        content = response['Body'].read()
        
        if content == test_content:
            print(f"✅ File content verified successfully")
        else:
            print(f"❌ File content mismatch!")
            return False

        # Test 5: List objects in bucket
        print(f"\n📂 Listing objects in '{bucket_name}'...")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="test/")
        objects = response.get('Contents', [])
        
        if objects:
            print(f"✅ Found {len(objects)} object(s):")
            for obj in objects[:5]:  # Show first 5
                print(f"   - {obj['Key']} ({obj['Size']} bytes)")
        else:
            print("ℹ️  No objects found with prefix 'test/'")

        print("\n✅ All boto3 tests passed!")
        return True

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_profiler_connector():
    """Test MinIO connection using the profiler's CloudStorageConnector."""
    print("\n" + "=" * 70)
    print("Testing MinIO with Profiler CloudStorageConnector")
    print("=" * 70)

    try:
        # Create MinIO descriptor
        bucket_name = env.MINIO_BUCKET_NAME or env.MINIO_TEST_BUCKET or "data-files"
        descriptor = SourceDescriptor(
            scheme="minio",
            bucket_or_host=bucket_name,
            path="test/",
            raw_uri=f"minio://{bucket_name}/test/",
        )

        # Get credentials from environment
        credentials = {
            "endpoint_url": env.MINIO_ENDPOINT_URL,
            "access_key": env.MINIO_ACCESS_KEY,
            "secret_key": env.MINIO_SECRET_KEY,
            "region": env.MINIO_REGION,
        }

        print(f"\n🔗 Testing connection to: {descriptor.raw_uri}")
        print(f"   Endpoint: {credentials['endpoint_url']}")
        print(f"   Region: {credentials['region']}")

        # Create connector
        connector = CloudStorageConnector("minio")

        # Test connection
        print("\n🧪 Running connection test...")
        result = connector.test_connection(descriptor, credentials)
        
        if result:
            print("✅ Connector test passed!")
        else:
            print("❌ Connector test failed")
            return False

        # List objects
        print("\n📂 Listing objects...")
        objects = connector.list_objects(descriptor, credentials)
        
        if objects:
            print(f"✅ Found {len(objects)} profilable file(s):")
            for obj in objects[:5]:  # Show first 5
                print(f"   - {obj.name} ({obj.size_bytes} bytes) - {obj.file_format}")
        else:
            print("ℹ️  No profilable files found")

        print("\n✅ All profiler connector tests passed!")
        return True

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_connection_manager():
    """Test MinIO connection using ConnectionManager."""
    print("\n" + "=" * 70)
    print("Testing MinIO with ConnectionManager")
    print("=" * 70)

    try:
        bucket_name = env.MINIO_BUCKET_NAME or env.MINIO_TEST_BUCKET or "data-files"
        uri = f"minio://{bucket_name}/test/"

        print(f"\n🔗 Testing URI: {uri}")

        # Test connection
        manager = ConnectionManager()
        result = manager.test_connection(uri)

        if result:
            print("✅ ConnectionManager test passed!")
            return True
        else:
            print("❌ ConnectionManager test failed")
            return False

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all MinIO tests."""
    print("\n🚀 MinIO Connection Test Suite\n")

    # Display configuration
    test_minio_config()

    # Run tests
    results = []
    
    results.append(("boto3 Connection", test_boto3_connection()))
    results.append(("Profiler Connector", test_profiler_connector()))
    results.append(("Connection Manager", test_connection_manager()))

    # Summary
    print("\n" + "=" * 70)
    print("Test Summary")
    print("=" * 70)
    
    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} - {test_name}")

    all_passed = all(result[1] for result in results)
    
    print("\n" + "=" * 70)
    if all_passed:
        print("🎉 All tests passed!")
        print("=" * 70)
        return 0
    else:
        print("⚠️  Some tests failed. Check the output above.")
        print("=" * 70)
        return 1


if __name__ == "__main__":
    sys.exit(main())
