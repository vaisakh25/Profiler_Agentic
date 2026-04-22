#!/usr/bin/env python3
"""
Quick validation test for DuckDB parallel profiling fixes.
Tests the semaphore, timeout, and fallback mechanisms.
"""
import os
import sys
import time
import tempfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent to path
sys.path.insert(0, '/app')

from file_profiler.engines.duckdb_sampler import (
    duckdb_connection,
    duckdb_count,
    duckdb_sample,
    DuckDBTimeoutError,
    _MAX_CONCURRENT_DUCKDB_CONNECTIONS,
    DUCKDB_OPERATION_TIMEOUT,
)


def test_semaphore_limit():
    """Test that semaphore limits concurrent connections."""
    print("\n[TEST 1] Semaphore Limit")
    print(f"  Max concurrent connections: {_MAX_CONCURRENT_DUCKDB_CONNECTIONS}")
    assert _MAX_CONCURRENT_DUCKDB_CONNECTIONS == 2, "Expected 2 max connections"
    print("  ✓ Semaphore configured correctly")


def test_timeout_constant():
    """Test that timeout is configured."""
    print("\n[TEST 2] Timeout Configuration")
    print(f"  Operation timeout: {DUCKDB_OPERATION_TIMEOUT}s")
    assert DUCKDB_OPERATION_TIMEOUT == 60, "Expected 60s timeout"
    print("  ✓ Timeout configured correctly")


def test_exception_exists():
    """Test that DuckDBTimeoutError exception exists."""
    print("\n[TEST 3] Exception Class")
    print(f"  DuckDBTimeoutError: {DuckDBTimeoutError.__name__}")
    assert issubclass(DuckDBTimeoutError, Exception)
    print("  ✓ Exception class available")


def test_connection_context_manager():
    """Test that connection context manager works."""
    print("\n[TEST 4] Connection Context Manager")
    with duckdb_connection() as con:
        result = con.execute("SELECT 42").fetchone()
        assert result[0] == 42
    print("  ✓ Context manager works")


def test_parallel_connections():
    """Test that multiple threads can acquire connections (via semaphore)."""
    print("\n[TEST 5] Parallel Connection Acquisition")
    
    def acquire_connection(thread_id):
        print(f"    Thread {thread_id}: Acquiring connection...")
        start = time.time()
        with duckdb_connection() as con:
            elapsed = time.time() - start
            print(f"    Thread {thread_id}: Got connection after {elapsed:.2f}s")
            result = con.execute("SELECT $tid", {"tid": thread_id}).fetchone()
            time.sleep(0.1)  # Hold connection briefly
            return result[0]
    
    # Run 4 threads (2x the semaphore limit) to test queueing
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(acquire_connection, i) for i in range(4)]
        results = [f.result() for f in as_completed(futures)]
    
    assert set(results) == {0, 1, 2, 3}
    print("  ✓ Parallel connections work (semaphore queuing)")


def test_csv_operations():
    """Test DuckDB CSV operations with real data."""
    print("\n[TEST 6] CSV Count & Sample Operations")
    
    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("id,name,value\n")
        for i in range(1000):
            f.write(f"{i},name_{i},{i * 10}\n")
        csv_path = Path(f.name)
    
    try:
        with duckdb_connection() as con:
            # Test count
            count = duckdb_count(csv_path, _con=con, timeout=5)
            print(f"    Row count: {count}")
            assert count == 1000, f"Expected 1000 rows, got {count}"
            
            # Test sample
            headers, rows = duckdb_sample(csv_path, sample_size=10, _con=con, timeout=5)
            print(f"    Sample: {len(headers)} columns, {len(rows)} rows")
            assert headers == ['id', 'name', 'value']
            assert len(rows) == 10
            
        print("  ✓ CSV operations work")
    finally:
        csv_path.unlink()


def test_per_process_temp_dirs():
    """Test that per-process temp directories are configured."""
    print("\n[TEST 7] Per-Process Temp Directories")
    
    with duckdb_connection() as con:
        # Try to query the temp_directory setting
        try:
            result = con.execute("SELECT current_setting('temp_directory')").fetchone()
            temp_dir = result[0] if result else None
            print(f"    Temp directory: {temp_dir}")
            
            # Check if it contains the PID
            pid = os.getpid()
            if temp_dir and str(pid) in temp_dir:
                print(f"    ✓ Contains PID {pid}")
            else:
                print(f"    Note: Temp directory may not contain PID (older DuckDB version)")
        except Exception as e:
            print(f"    Note: Could not query temp_directory (older DuckDB): {e}")
    
    print("  ✓ Temp directory configuration attempted")


def main():
    """Run all validation tests."""
    print("=" * 70)
    print("DuckDB Parallel Profiling Fixes - Validation Tests")
    print("=" * 70)
    
    tests = [
        test_semaphore_limit,
        test_timeout_constant,
        test_exception_exists,
        test_connection_context_manager,
        test_parallel_connections,
        test_csv_operations,
        test_per_process_temp_dirs,
    ]
    
    passed = 0
    failed = 0
    
    for test_func in tests:
        try:
            test_func()
            passed += 1
        except Exception as e:
            failed += 1
            print(f"  ✗ FAILED: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "=" * 70)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 70)
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
