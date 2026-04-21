"""Background job manager for long-running enrichment tasks.

Allows the MCP server to remain responsive while enrichment runs
asynchronously.  Jobs are tracked in-memory with LRU eviction.

Usage:
    job_id = job_manager.submit(coro, job_type="enrichment")
    status = job_manager.get(job_id)
    all_jobs = job_manager.list_jobs()
"""

from __future__ import annotations

import asyncio
import dataclasses
import enum
import logging
import time
import uuid
from collections import OrderedDict
from typing import Any, Coroutine, Optional

log = logging.getLogger(__name__)

_MAX_JOBS = 100


class JobStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclasses.dataclass
class Job:
    job_id: str
    job_type: str
    status: JobStatus
    created_at: float
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    progress: int = 0
    progress_total: int = 100
    progress_message: str = ""
    result: Optional[Any] = None
    error: Optional[str] = None
    _task: Optional[asyncio.Task] = dataclasses.field(default=None, repr=False)

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "job_type": self.job_type,
            "status": self.status.value,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "progress": self.progress,
            "progress_total": self.progress_total,
            "progress_message": self.progress_message,
            "result": self.result if self.status == JobStatus.COMPLETED else None,
            "error": self.error,
            "elapsed_seconds": round(
                (self.completed_at or time.time()) - (self.started_at or self.created_at), 1
            ),
        }


class _JobStore(OrderedDict):
    """LRU-evicting job store."""

    def __init__(self, max_size: int = _MAX_JOBS) -> None:
        super().__init__()
        self._max_size = max_size

    def __setitem__(self, key: str, value: Job) -> None:
        if key in self:
            self.move_to_end(key)
        super().__setitem__(key, value)
        while len(self) > self._max_size:
            oldest_key = next(iter(self))
            oldest = self[oldest_key]
            # Don't evict running jobs
            if oldest.status == JobStatus.RUNNING:
                break
            del self[oldest_key]
            log.debug("Evicted job %s (max %d)", oldest_key, self._max_size)


_jobs = _JobStore()


def submit(coro: Coroutine, job_type: str = "enrichment") -> str:
    """Submit a coroutine as a background job. Returns the job_id."""
    job_id = uuid.uuid4().hex[:12]
    now = time.time()

    job = Job(
        job_id=job_id,
        job_type=job_type,
        status=JobStatus.PENDING,
        created_at=now,
    )
    _jobs[job_id] = job

    async def _run():
        job.status = JobStatus.RUNNING
        job.started_at = time.time()
        log.info("Job %s (%s) started", job_id, job_type)
        try:
            job.result = await coro
            job.status = JobStatus.COMPLETED
            log.info("Job %s completed", job_id)
        except Exception as exc:
            job.status = JobStatus.FAILED
            job.error = f"{type(exc).__name__}: {exc}"
            log.error("Job %s failed: %s", job_id, job.error)
        finally:
            job.completed_at = time.time()

    job._task = asyncio.create_task(_run())
    return job_id


def get(job_id: str) -> Optional[Job]:
    """Get a job by ID."""
    return _jobs.get(job_id)


def update_progress(job_id: str, progress: int, total: int = 100, message: str = "") -> None:
    """Update progress on a running job (called from within the job coroutine)."""
    job = _jobs.get(job_id)
    if job and job.status == JobStatus.RUNNING:
        job.progress = progress
        job.progress_total = total
        job.progress_message = message


def list_jobs(job_type: Optional[str] = None, status: Optional[JobStatus] = None) -> list[dict]:
    """List jobs, optionally filtered by type and/or status."""
    result = []
    for job in reversed(_jobs.values()):
        if job_type and job.job_type != job_type:
            continue
        if status and job.status != status:
            continue
        result.append(job.to_dict())
    return result


def cancel(job_id: str) -> bool:
    """Cancel a running job. Returns True if cancelled."""
    job = _jobs.get(job_id)
    if not job or not job._task:
        return False
    if job.status != JobStatus.RUNNING:
        return False
    job._task.cancel()
    job.status = JobStatus.FAILED
    job.error = "Cancelled by user"
    job.completed_at = time.time()
    log.info("Job %s cancelled", job_id)
    return True
