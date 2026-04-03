# Production Readiness: Immediate Action Plan

**Objective:** Address critical security vulnerabilities and establish baseline monitoring before production deployment.

**Timeline:** 4-6 weeks (2 engineers, full-time)  
**Current Status:** 🔴 NOT PRODUCTION READY

---

## Week 1-2: Emergency Security Hardening

### Day 1-2: Authentication Foundation
**Owner:** Security Engineer  
**Priority:** 🔴 CRITICAL

**Tasks:**
1. Choose auth provider (Auth0, AWS Cognito, or custom OAuth2)
2. Implement authentication middleware for FastAPI
3. Add API key generation and validation
4. Protect all REST endpoints with `@requires_auth` decorator
5. Add authentication to WebSocket connections
6. Update frontend to handle login flow

**Files to modify:**
- `file_profiler/agent/web_server.py` - Add auth middleware
- `frontend/app.js` - Add login UI and token management
- Create `file_profiler/auth/` module for auth logic

**Acceptance Criteria:**
- All endpoints return 401 without valid token
- Login flow tested manually
- Token refresh working
- Unit tests for auth middleware

---

### Day 3-4: Secrets Migration
**Owner:** DevOps Engineer  
**Priority:** 🔴 CRITICAL

**Tasks:**
1. Set up AWS Secrets Manager / Azure Key Vault / HashiCorp Vault
2. Create migration script to move secrets from .env
3. Update `file_profiler/config/env.py` to fetch from vault
4. Add SDK clients for secret retrieval with caching
5. Configure IAM roles/service principals
6. Update docker-compose and Dockerfile for vault access

**Files to modify:**
- `file_profiler/config/env.py` - Add vault client
- `file_profiler/config/secrets.py` - NEW: Vault abstraction layer
- `docker-compose.yml` - Remove plaintext secrets
- `.env.example` - Update with vault references

**Acceptance Criteria:**
- Zero secrets in environment variables
- Vault connection tested in staging
- Graceful fallback if vault unreachable
- Secret rotation procedure documented

---

### Day 5-7: API Security Hardening
**Owner:** Backend Engineer  
**Priority:** 🔴 HIGH

**Tasks:**
1. Install and configure `slowapi` for rate limiting
2. Add CORS middleware with whitelist
3. Add security headers middleware
4. Add request timeout middleware (30s global, 300s for profiling)
5. Global exception handler to prevent stack trace leakage
6. Add Pydantic models for all request bodies

**Files to modify:**
- `file_profiler/agent/web_server.py` - Add all middleware
- Create `file_profiler/schemas/` for Pydantic models
- `requirements.txt` - Add slowapi, pydantic

**Code example:**
```python
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

limiter = Limiter(key_func=get_remote_address, default_limits=["100/minute"])
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

@app.post("/api/upload")
@limiter.limit("5/minute")  # Stricter for expensive ops
async def upload_file(...):
    ...
```

**Acceptance Criteria:**
- Rate limits tested (verify 429 responses)
- CORS tested with cross-origin requests
- Security headers verified with securityheaders.com
- All request handlers use Pydantic validation

---

### Day 8-10: File Upload Security
**Owner:** Security Engineer  
**Priority:** 🔴 HIGH

**Tasks:**
1. Add file content-type validation (magic bytes)
2. Integrate ClamAV for malware scanning (or VirusTotal API)
3. Require authentication for upload endpoints
4. Add file size limits per user tier
5. Implement secure filename sanitization
6. Add upload virus scan metrics

**Files to modify:**
- `file_profiler/agent/web_server.py` - Upload handler
- Create `file_profiler/security/scanner.py` - Malware scanning
- `docker-compose.yml` - Add ClamAV container if using local

**Acceptance Criteria:**
- Malicious file test samples blocked
- Content-type validation prevents extension spoofing
- Upload requires valid auth token
- Scanned files logged to audit trail

---

## Week 3: Essential Monitoring

### Day 11-13: Metrics & Observability
**Owner:** DevOps Engineer  
**Priority:** 🔴 CRITICAL

**Tasks:**
1. Add prometheus_client to FastAPI (Starlette-Prometheus)
2. Define custom metrics (LLM calls, file uploads, profile duration)
3. Add OpenTelemetry auto-instrumentation
4. Deploy Prometheus (docker-compose or managed service)
5. Create initial Grafana dashboards
6. Add /metrics endpoint

**Files to modify:**
- `file_profiler/agent/web_server.py` - Add Prometheus middleware
- `file_profiler/observability/metrics.py` - NEW: Custom metrics
- `docker-compose.yml` - Add Prometheus + Grafana containers
- `requirements.txt` - Add prometheus-client, opentelemetry

**Key Metrics to Track:**
```python
from prometheus_client import Counter, Histogram, Gauge

# Request metrics (RED)
http_requests_total = Counter('http_requests_total', 'Total HTTP requests', ['method', 'endpoint', 'status'])
http_request_duration = Histogram('http_request_duration_seconds', 'HTTP request latency')

# Business metrics
llm_api_calls = Counter('llm_api_calls_total', 'LLM API calls', ['provider', 'status'])
llm_api_cost = Counter('llm_api_cost_usd', 'Estimated LLM API cost')
files_profiled = Counter('files_profiled_total', 'Files profiled', ['format'])
profile_duration = Histogram('profile_duration_seconds', 'Profiling duration')

# Resource metrics
active_websocket_sessions = Gauge('websocket_sessions_active', 'Active WebSocket sessions')
database_connections = Gauge('database_connections', 'Active database connections', ['state'])
```

**Acceptance Criteria:**
- /metrics endpoint returns Prometheus format
- Metrics visible in Grafana
- No performance degradation from instrumentation

---

### Day 14-15: Error Tracking & Alerting
**Owner:** DevOps Engineer  
**Priority:** 🔴 CRITICAL

**Tasks:**
1. Set up Sentry account and create project
2. Add Sentry SDK to application
3. Configure error grouping and breadcrumbs
4. Set up PagerDuty integration
5. Create alert rules (error rate, latency SLA, disk space)
6. Test alert delivery

**Files to modify:**
- `file_profiler/agent/web_server.py` - Add Sentry middleware
- `file_profiler/utils/logging_setup.py` - Sentry initialization
- `requirements.txt` - Add sentry-sdk[fastapi]

**Alert Rules:**
```yaml
# Example alerting rules (Prometheus AlertManager)
groups:
  - name: production
    rules:
      - alert: HighErrorRate
        expr: rate(http_requests_total{status=~"5.."}[5m]) > 0.05
        for: 5m
        annotations:
          summary: "High error rate detected"
        
      - alert: HighLatency
        expr: histogram_quantile(0.95, http_request_duration_seconds) > 2.0
        for: 10m
        annotations:
          summary: "P95 latency above SLA"
      
      - alert: DiskSpaceWarning
        expr: node_filesystem_avail_bytes / node_filesystem_size_bytes < 0.2
        for: 5m
        annotations:
          summary: "Disk usage above 80%"
```

**Acceptance Criteria:**
- Errors appear in Sentry dashboard
- Alert triggered in test (manual exception)
- PagerDuty notification delivered
- Runbook links in alerts

---

## Week 4: Data & Reliability

### Day 16-18: Database Migrations
**Owner:** Backend Engineer  
**Priority:** 🔴 CRITICAL

**Tasks:**
1. Install Alembic
2. Initialize migration environment
3. Create initial migration from current schema
4. Add migration CI check (prevent merging without migration)
5. Document migration workflow
6. Test rollback procedure

**Commands:**
```bash
# Initialize
alembic init alembic

# Auto-generate migration from models
alembic revision --autogenerate -m "Initial schema"

# Apply migration
alembic upgrade head

# Rollback
alembic downgrade -1
```

**Files to create:**
- `alembic/` - Migration directory
- `alembic.ini` - Alembic configuration
- `file_profiler/config/database.py` - Add engine for Alembic

**Acceptance Criteria:**
- Migration applies cleanly on fresh database
- Rollback tested successfully
- CI fails if migration not created for schema changes

---

### Day 19-20: Backup & Recovery
**Owner:** DevOps Engineer  
**Priority:** 🔴 CRITICAL

**Tasks:**
1. Set up automated PostgreSQL backups (pg_dump or AWS RDS snapshots)
2. Configure backup retention (30 days)
3. Enable WAL archiving for PITR
4. Test restore procedure (restore to separate instance)
5. Document recovery runbook (RTO: 4 hours, RPO: 15 minutes)
6. Add backup success/failure alerts

**Backup Script Example:**
```bash
#!/bin/bash
# backup.sh - Run daily via cron

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR=/backups/postgres
BUCKET=s3://your-backups-bucket/postgres

# Dump database
pg_dump -h $POSTGRES_HOST -U $POSTGRES_USER -F c -f $BACKUP_DIR/backup_$TIMESTAMP.dump $POSTGRES_DB

# Upload to S3
aws s3 cp $BACKUP_DIR/backup_$TIMESTAMP.dump $BUCKET/backup_$TIMESTAMP.dump

# Clean local backups older than 7 days
find $BACKUP_DIR -name "*.dump" -mtime +7 -delete

# Alert on failure
if [ $? -ne 0 ]; then
  curl -X POST https://your-alert-webhook ...
fi
```

**Acceptance Criteria:**
- Automated backup running daily
- Backup uploaded to S3/cloud storage
- Restore tested successfully
- Recovery time under 4 hours in test

---

## Week 5-6: Operations & Testing

### Day 21-23: Infrastructure as Code
**Owner:** DevOps Engineer  
**Priority:** 🔴 HIGH

**Tasks:**
1. Create Terraform modules for:
   - ECS/Cloud Run service
   - Application Load Balancer
   - RDS PostgreSQL instance
   - S3 buckets (uploads, backups)
   - VPC and security groups
   - IAM roles and policies
2. Initialize Terraform state in S3 backend
3. Apply infrastructure in staging environment
4. Document infrastructure variables

**Files to create:**
```
terraform/
├── main.tf
├── variables.tf
├── outputs.tf
├── modules/
│   ├── ecs/
│   ├── rds/
│   ├── alb/
│   └── s3/
└── environments/
    ├── staging/
    └── production/
```

**Acceptance Criteria:**
- Terraform plan runs without errors
- Applied successfully in staging
- Infrastructure matches production requirements
- State stored remotely (S3 + DynamoDB lock)

---

### Day 24-26: Runbooks & Documentation
**Owner:** Tech Lead  
**Priority:** 🔴 CRITICAL

**Tasks:**
1. Create incident response runbooks:
   - High error rate
   - Database connection failures
   - Disk full
   - LLM API failures
   - Security incident (data breach)
2. Document escalation procedures
3. Create on-call rotation schedule
4. Write deployment playbook
5. Update README with production architecture

**Runbook Template:**
```markdown
# Runbook: High Error Rate

## Symptoms
- Error rate > 5% for 5+ minutes
- Sentry alert triggered
- Users reporting 500 errors

## Diagnosis
1. Check Grafana dashboard: "Error Rate by Endpoint"
2. Review Sentry for error patterns
3. Check CloudWatch logs for stack traces
4. Verify database connectivity
5. Check LLM provider status pages

## Resolution
1. If database connection issue:
   - Restart database connection pool
   - Check RDS metrics for CPU/connections
   
2. If LLM provider down:
   - Agent will auto-failover to backup provider
   - Verify fallback working in logs
   
3. If code bug:
   - Identify failing endpoint
   - Rollback to previous version
   - Create hotfix

## Escalation
- SEV1: On-call engineer responds in 15 min
- SEV2: Escalate to Tech Lead after 30 min
- SEV3: Notify CTO if downtime > 1 hour

## Prevention
- Add test coverage for error path
- Improve input validation
- Add circuit breaker
```

**Acceptance Criteria:**
- 5+ runbooks documented
- Tested in dry-run incident drill
- Accessible to all on-call engineers

---

### Day 27-30: Security & Load Testing
**Owner:** QA Engineer + Security Engineer  
**Priority:** 🔴 HIGH

**Tasks:**
1. Run OWASP ZAP security scan
2. Fix all critical findings
3. Create Locust load test scenarios:
   - Concurrent file uploads
   - WebSocket chat sessions
   - Profiling throughput
4. Run load test until failure (find breaking point)
5. Validate auto-scaling behavior
6. Document capacity limits

**Load Test Example:**
```python
# locustfile.py
from locust import HttpUser, task, between

class ProfilerUser(HttpUser):
    wait_time = between(1, 5)
    
    def on_start(self):
        # Login
        response = self.client.post("/api/login", json={
            "username": "test@example.com",
            "password": "test123"
        })
        self.token = response.json()["token"]
    
    @task(3)
    def upload_file(self):
        files = {"file": open("test_data.csv", "rb")}
        self.client.post(
            "/api/upload",
            files=files,
            headers={"Authorization": f"Bearer {self.token}"}
        )
    
    @task(1)
    def profile_file(self):
        self.client.post(
            "/api/profile",
            json={"file_path": "/data/test_data.csv"},
            headers={"Authorization": f"Bearer {self.token}"}
        )
```

**Run:**
```bash
# 100 concurrent users, ramp up 10/second
locust -f locustfile.py --users 100 --spawn-rate 10 --host https://staging.example.com
```

**Acceptance Criteria:**
- Zero critical security findings
- System handles 50 concurrent users without degradation
- P95 latency under 2 seconds at target load
- Auto-scaling triggers correctly

---

## Post-Week 6: Production Launch Checklist

### Pre-Launch (Day 28-30)
- [ ] All Phase 1 (Critical Security) items complete
- [ ] All Phase 2 (Monitoring) items complete
- [ ] Staging environment deployed and tested
- [ ] Load test passed
- [ ] Security scan passed (zero critical)
- [ ] Backup/restore tested
- [ ] Runbooks published
- [ ] On-call rotation scheduled
- [ ] Legal review of Terms of Service complete

### Launch Day
- [ ] Deploy to production using blue-green strategy
- [ ] Verify /health endpoints
- [ ] Run smoke tests
- [ ] Monitor error rate for 1 hour
- [ ] Announce launch to users
- [ ] Monitor dashboards continuously for 24 hours

### Post-Launch (Week 1)
- [ ] Daily error review
- [ ] Check backup success
- [ ] Review cost metrics (LLM API)
- [ ] Collect user feedback
- [ ] Address any P0/P1 bugs
- [ ] Document lessons learned

---

## Success Metrics (30 Days Post-Launch)

| Metric | Target | Tracking |
|--------|--------|----------|
| Uptime SLA | 99.5% | CloudWatch / Pingdom |
| P95 Latency | < 2s | Prometheus |
| Error Rate | < 0.5% | Sentry |
| Security Incidents | 0 | Security audit logs |
| MTTR (Mean Time to Repair) | < 1 hour | PagerDuty |
| Customer Satisfaction | > 4.0/5.0 | NPS surveys |

---

## Budget Estimate

| Item | Monthly Cost | Notes |
|------|--------------|-------|
| AWS ECS (2 containers) | $150 | Fargate pricing |
| RDS PostgreSQL | $180 | db.t3.medium |
| S3 storage | $50 | Uploads + backups |
| LLM API (estimated) | $500 | Based on usage |
| Sentry | $26 | Team plan |
| DataDog / Prometheus | $100 | Monitoring stack |
| PagerDuty | $25 | Starter plan |
| Domain + SSL | $10 | Route53 + ACM |
| **Total** | **~$1,041/month** | + usage-based LLM costs |

---

## Risk Register

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Secrets leaked in .env | CRITICAL | Medium | Migrate to Vault (Week 1) |
| LLM API cost overrun | HIGH | High | Add cost monitoring + quotas |
| Database data loss | CRITICAL | Low | Automated backups + PITR |
| DDoS attack | HIGH | Medium | Rate limiting + CloudFlare |
| Authentication bypass | CRITICAL | Low | Security testing + audit |
| Slow queries at scale | MEDIUM | Medium | Database indexes + caching |

---

## Team Assignments

| Engineer | Primary Focus | Week 1-2 | Week 3-4 | Week 5-6 |
|----------|---------------|----------|----------|----------|
| Security Engineer | Auth & Security | Auth + File Upload | Security Testing | Security Review |
| DevOps Engineer | Infrastructure | Secrets + Monitoring | IaC + Backups | Load Testing |
| Backend Engineer | Application | API Hardening | Migrations | Deployment |
| Tech Lead | Coordination | Planning | Runbooks | Launch Prep |

---

## Next Review: End of Week 2

**Review Agenda:**
1. Demo authentication flow
2. Verify secrets migrated to vault
3. Check security headers implementation
4. Review rate limiting behavior
5. Adjust timeline if needed
6. Address blockers

**Meeting:** Friday, Week 2, 2:00 PM  
**Attendees:** Full team + stakeholders
