# Kivor Platform - MinIO Configuration Guide

## MCP Tools That Use MinIO

Your profiler exposes **2 MCP tools** for MinIO integration:

### 1. 🔌 `connect_source` - Register MinIO Connection
**Purpose**: Store MinIO credentials securely for reuse  
**Server**: Connector MCP (`localhost:8081`)

**Required Parameters:**
```json
{
  "connection_id": "kivor-minio",
  "scheme": "minio",
  "credentials": {
    "endpoint_url": "http://localhost:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin123",
    "region": "us-east-1",
    "test_bucket": "data-files"
  },
  "display_name": "Kivor MinIO Storage",
  "test": true
}
```

### 2. 📊 `profile_remote_source` - Profile MinIO Files
**Purpose**: Profile data files from MinIO  
**Server**: Connector MCP (`localhost:8081`)

**Required Parameters:**
```json
{
  "uri": "minio://data-files/sales/customers.csv",
  "connection_id": "kivor-minio"
}
```

Or to profile an entire bucket/folder:
```json
{
  "uri": "minio://data-files/sales/",
  "connection_id": "kivor-minio"
}
```

---

## MCP Server Endpoints

### Connector MCP Server (MinIO Tools)
- **URL**: `http://localhost:8081/sse`
- **Port**: `8081`
- **Transport**: SSE (Server-Sent Events)

**Tools Available:**
- ✅ `connect_source` - Register MinIO credentials
- ✅ `profile_remote_source` - Profile MinIO files
- ✅ `list_connections` - View registered connections
- ✅ `detect_relationships` - Find relationships between profiled files
- ✅ `enrich_relationships` - Add LLM-generated insights
- ✅ `visualize_schema` - Generate ER diagrams
- ✅ `query_knowledge_base` - Ask questions about profiled data

### Profiler MCP Server (Local Files)
- **URL**: `http://localhost:8080/sse`
- **Port**: `8080`
- **Transport**: SSE
- **Purpose**: Profile local files (not MinIO)

---

## Step-by-Step Workflow

### Option A: Using MCP Tools Directly

**Step 1: Register MinIO Connection**
```python
# Call the connect_source MCP tool
{
  "tool": "connect_source",
  "connection_id": "kivor-minio",
  "scheme": "minio",
  "credentials": {
    "endpoint_url": "http://localhost:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin123"
  },
  "test": true
}
```

**Step 2: Profile Files from MinIO**
```python
# Single file
{
  "tool": "profile_remote_source",
  "uri": "minio://data-files/sales/customers.csv",
  "connection_id": "kivor-minio"
}

# Multiple files (entire folder)
{
  "tool": "profile_remote_source",
  "uri": "minio://data-files/sales/",
  "connection_id": "kivor-minio"
}
```

**Step 3: Detect Relationships**
```python
{
  "tool": "detect_relationships",
  "connection_id": "kivor-minio"
}
```

**Step 4: Enrich with LLM**
```python
{
  "tool": "enrich_relationships",
  "connection_id": "kivor-minio"
}
```

### Option B: Using Environment Variables (Auto-Discovery)

If you set MinIO credentials in `.env`, you can skip `connect_source`:

```python
# Profile directly - credentials auto-loaded from .env
{
  "tool": "profile_remote_source",
  "uri": "minio://data-files/sales/customers.csv"
}
```

---

## Configuration Values

Use these values from your `.env` file to configure MinIO in Kivor:

### Service Configuration

| Field | Value | Notes |
|-------|-------|-------|
| **Service Name** | `profiler-minio` | Choose any descriptive name |
| **Minio Bucket** | `data-files` | From `MINIO_BUCKET_NAME` |
| **Minio Endpoint** | `http://localhost:9000` | From `MINIO_ENDPOINT_URL` |
| **Minio Access Key** | `minioadmin` | From `MINIO_ACCESS_KEY` |
| **Minio Secret Key** | `minioadmin123` | From `MINIO_SECRET_KEY` |

### Integration Settings

| Field | Value |
|-------|-------|
| **Frequency** | `one-time` |
| **Direction** | `inbound` |
| **Type** | `import` |

---

## Current Environment Configuration

### From `.env` (Credentials - Required)
```env
MINIO_ENDPOINT_URL=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
```

### From `config.yml` (Runtime Defaults)
```yaml
MINIO_BUCKET_NAME: data-files
MINIO_REGION: us-east-1
MINIO_PORT: 9000
MINIO_CONSOLE_PORT: 9001
```

---

## For Different Deployment Scenarios

### Local Development (Current Setup)
- **Endpoint**: `http://localhost:9000`
- Use when profiler runs on your local machine

### Docker Container Access (If Kivor runs in Docker)
- **Endpoint**: `http://host.docker.internal:9000`
- Use when Kivor platform runs inside Docker on Windows/Mac

### Production/Remote Server
- **Endpoint**: `http://<server-ip>:9000` or `https://<domain>:9000`
- Replace with your actual MinIO server address

### Kubernetes/Cloud Deployment
- **Endpoint**: `http://profiler-minio-service:9000`
- Use the Kubernetes service name if MinIO is deployed in the same cluster

---

## MinIO Console Access

You can verify your MinIO setup at: **http://localhost:9001**
- Username: `minioadmin`
- Password: `minioadmin123`

---

## Testing Connection

After configuring in Kivor, verify the connection:

```bash
# Test from command line
python -c "
from file_profiler.config import env
print(f'Bucket: {env.MINIO_BUCKET_NAME}')
print(f'Endpoint: {env.MINIO_ENDPOINT_URL}')
print(f'Access Key: {env.MINIO_ACCESS_KEY}')
"
```

Or run the multi-file test:
```bash
python test_minio_multi_file.py
```

---

## Security Notes

⚠️ **Production Considerations:**

1. **Change Default Credentials**: Replace `minioadmin`/`minioadmin123` with strong credentials
2. **Use HTTPS**: Configure TLS/SSL for production endpoints
3. **Restrict Access**: Use IAM policies to limit bucket access
4. **Secure Storage**: Never commit real credentials to version control
5. **Use Secrets Management**: Consider AWS Secrets Manager, HashiCorp Vault, or Kubernetes Secrets

---

## 🎯 Quick Answer: Which Tool Needs MinIO?

**Primary Tool**: `connect_source` (on Connector MCP Server)

**How Kivor Should Connect:**
1. **MCP Server URL**: `http://localhost:8081/sse`
2. **Call Tool**: `connect_source`
3. **With Parameters**:
   ```json
   {
     "connection_id": "kivor-minio",
     "scheme": "minio",
     "credentials": {
       "endpoint_url": "http://localhost:9000",
       "access_key": "minioadmin",
       "secret_key": "minioadmin123"
     }
   }
   ```

**Then Use**: `profile_remote_source` with `connection_id: "kivor-minio"`

---

## All MinIO-Related MCP Tools

| Tool Name | Purpose | MCP Server | MinIO Parameter |
|-----------|---------|------------|-----------------|
| `connect_source` | Register MinIO connection | Connector (8081) | `credentials.endpoint_url`, `credentials.access_key`, `credentials.secret_key` |
| `profile_remote_source` | Profile files from MinIO | Connector (8081) | `uri` (e.g., `minio://bucket/file.csv`) |
| `list_connections` | View registered connections | Connector (8081) | None (lists all) |
| `test_connection` | Verify MinIO connectivity | Connector (8081) | `connection_id` |
| `detect_relationships` | Find FK relationships | Connector (8081) | `connection_id` |
| `enrich_relationships` | Add LLM insights | Connector (8081) | `connection_id` |
| `visualize_schema` | Generate ER diagrams | Connector (8081) | `connection_id` |
| `query_knowledge_base` | Ask questions about data | Connector (8081) | Uses cached profiles |

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│                    Kivor Platform                        │
│                                                          │
│  ┌─────────────────────────────────────────────────┐   │
│  │  Kivor → MCP Client                             │   │
│  │  Connect to: http://localhost:8081/sse          │   │
│  └──────────────────┬──────────────────────────────┘   │
└─────────────────────┼──────────────────────────────────┘
                      │
                 MCP Protocol
                      │
┌─────────────────────▼──────────────────────────────────┐
│         Connector MCP Server (:8081)                   │
│                                                         │
│  Tools:                                                 │
│  • connect_source(credentials) ──┐                     │
│  • profile_remote_source(uri) ───┼─► Uses MinIO       │
│  • detect_relationships() ────────┘    credentials     │
│  • enrich_relationships()                              │
│                                                         │
└─────────────────────┬──────────────────────────────────┘
                      │
                  Connects to
                      │
┌─────────────────────▼──────────────────────────────────┐
│              MinIO Server (:9000)                       │
│              Console (:9001)                            │
│                                                         │
│  Bucket: data-files                                    │
│  Files:                                                 │
│    • sales/customers.csv                               │
│    • sales/orders.csv                                  │
│    • sales/order_lines.csv                             │
└────────────────────────────────────────────────────────┘
```

---

## Suggested Production Values

For production deployment, update `.env` with:

```env
# Production MinIO Configuration
MINIO_ENDPOINT_URL=https://minio.your-domain.com
MINIO_ACCESS_KEY=<generated-access-key>
MINIO_SECRET_KEY=<generated-secret-key>
```

And in Kivor platform:
- **Service Name**: `production-minio`
- **Minio Bucket**: `profiler-data-prod`
- **Minio Endpoint**: `https://minio.your-domain.com`
- **Minio Access Key**: `<your-production-access-key>`
- **Minio Secret Key**: `<your-production-secret-key>`
