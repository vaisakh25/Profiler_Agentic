# MinIO Integration Summary

## ✅ Completed Configuration

Successfully configured the profiler to work with your existing MinIO container running on `localhost:9000`.

## Configuration Files Updated

### 1. `.env` - Production Environment Variables
Added MinIO connection settings:
```env
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
MINIO_ENDPOINT_URL=http://localhost:9000
MINIO_ENDPOINT=localhost:9000
MINIO_HOST=localhost
MINIO_PORT=9000
MINIO_CONSOLE_PORT=9001
MINIO_BUCKET_NAME=data-files
MINIO_REGION=us-east-1
MINIO_TEST_BUCKET=data-files
```

### 2. `.env.example` - Template for Production
Updated with all MinIO fields for deployment reference.

### 3. `docker-compose.yml` - Container Orchestration
- Updated profiler-suite service to connect to external MinIO via `host.docker.internal:9000`
- Added all MinIO environment variables to profiler container
- Removed internal MinIO service (using your existing one instead)

### 4. `file_profiler/config/env.py` - Application Configuration
Added environment variable readers:
- `MINIO_ROOT_USER`
- `MINIO_ROOT_PASSWORD`
- `MINIO_ENDPOINT_URL`
- `MINIO_ENDPOINT`
- `MINIO_HOST`
- `MINIO_PORT`
- `MINIO_CONSOLE_PORT`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `MINIO_BUCKET_NAME`
- `MINIO_REGION`
- `MINIO_TEST_BUCKET`

## Test Scripts Created

### 1. `test_minio_connection.py`
Basic connectivity tests:
- Lists buckets
- Creates test bucket
- Uploads/downloads test files
- Tests profiler connector

### 2. `test_minio_integration.py`
Full end-to-end integration test:
- Creates sample CSV data
- Uploads to MinIO
- Profiles via DuckDB connector
- Verifies data reading

## ✅ Integration Test Results

```
🎉 Integration test PASSED!

Verified capabilities:
  ✅ Upload CSV to MinIO
  ✅ List objects in MinIO
  ✅ Configure DuckDB for MinIO
  ✅ Read CSV from MinIO via DuckDB
  ✅ Parse data correctly
```

## How to Use

### 1. Access MinIO Console
Open http://localhost:9001 in your browser
- Username: `minioadmin`
- Password: `minioadmin123`

### 2. Upload Files to MinIO
Upload your data files to the `data-files` bucket (or create a new bucket).

### 3. Profile Files from MinIO
Use MinIO URIs in the profiler:
```
minio://data-files/path/to/file.csv
minio://data-files/path/to/file.parquet
minio://data-files/path/to/file.json
```

### 4. Access Profiler Services
- **Profiler MCP**: http://localhost:8080
- **Connector MCP**: http://localhost:8081
- **Web UI**: http://localhost:8501

## Architecture

```
┌─────────────────┐
│  Your Host      │
├─────────────────┤
│                 │
│  MinIO          │◄──── Existing container
│  :9000          │      (ports 9000:9000, 9001:9001)
│                 │
└────────┬────────┘
         │
         │ host.docker.internal:9000
         │
┌────────▼────────┐
│ Profiler Suite  │◄──── New container
│                 │      (ports 8080, 8081, 8501)
│ - Profiler MCP  │
│ - Connector MCP │
│ - Web UI        │
└─────────────────┘
```

## Next Steps

1. **Upload your data files** to MinIO using the web console or MinIO client
2. **Start profiling** using MinIO URIs in the profiler
3. **Monitor** via MinIO console at http://localhost:9001

## Troubleshooting

### Profiler can't connect to MinIO from Docker
If running on Linux, change `host.docker.internal` to `host.gateway.internal` or the host's IP address in docker-compose.yml.

### Connection refused
Ensure MinIO is running:
```bash
docker ps | grep minio
```

### Authentication errors
Verify credentials in `.env` match your MinIO setup.

## Testing Commands

Run integration test:
```bash
python test_minio_integration.py
```

Run basic connectivity test:
```bash
python test_minio_connection.py
```

Check profiler logs:
```bash
docker logs profiler-suite
```
