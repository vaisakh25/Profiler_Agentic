"""
Encrypted credential storage for remote data source connections.

Credentials are encrypted at rest using Fernet symmetric encryption.
The encryption key is derived from PROFILER_SECRET_KEY env var.
If no key is set, falls back to in-memory-only storage (no persistence).

Storage backends:
  - PostgreSQL (if chat persistence DB is configured)
  - JSON file (encrypted, fallback when PG unavailable)
  - In-memory dict (fallback when no secret key is set)
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Encryption helpers
# ---------------------------------------------------------------------------

def _get_fernet_key() -> Optional[bytes]:
    """Derive a Fernet key from PROFILER_SECRET_KEY env var.

    Returns None if the env var is not set, which disables persistence
    and keeps credentials in-memory only.
    """
    secret = os.getenv("PROFILER_SECRET_KEY", "")
    if not secret:
        return None
    # Fernet requires a 32-byte URL-safe base64-encoded key.
    # Derive one from the user's secret using SHA-256.
    raw = hashlib.sha256(secret.encode()).digest()
    return base64.urlsafe_b64encode(raw)


def _encrypt(plaintext: str, key: bytes) -> str:
    """Encrypt a string and return base64-encoded ciphertext."""
    from cryptography.fernet import Fernet
    f = Fernet(key)
    return f.encrypt(plaintext.encode()).decode()


def _decrypt(ciphertext: str, key: bytes) -> str:
    """Decrypt base64-encoded ciphertext back to plaintext."""
    from cryptography.fernet import Fernet
    f = Fernet(key)
    return f.decrypt(ciphertext.encode()).decode()


# ---------------------------------------------------------------------------
# Storage interface
# ---------------------------------------------------------------------------

@dataclass
class StoredConnection:
    """A connection with encrypted credentials."""
    connection_id: str
    scheme: str
    display_name: str
    encrypted_credentials: str   # Fernet-encrypted JSON
    created_at: float = field(default_factory=time.time)
    last_tested: Optional[float] = None
    is_healthy: Optional[bool] = None


class CredentialStore:
    """Encrypted credential persistence.

    Stores connections in PostgreSQL (preferred) or a local encrypted
    JSON file.  Decryption happens only when credentials are needed
    for an active connection.
    """

    def __init__(self) -> None:
        self._key = _get_fernet_key()
        self._file_path = Path(
            os.getenv("PROFILER_OUTPUT_DIR", "data/output")
        ) / ".connections.enc"

        if self._key:
            log.info("Credential store: encryption enabled (Fernet)")
        else:
            log.info(
                "Credential store: PROFILER_SECRET_KEY not set — "
                "credentials will be stored in-memory only (not persisted)"
            )

    @property
    def persistence_enabled(self) -> bool:
        return self._key is not None

    def encrypt_credentials(self, credentials: dict) -> str:
        """Encrypt a credential dict.  Returns encrypted string."""
        if not self._key:
            # No encryption key — store as base64 (NOT secure, but functional
            # for in-memory-only mode where nothing is written to disk)
            return base64.b64encode(json.dumps(credentials).encode()).decode()
        return _encrypt(json.dumps(credentials), self._key)

    def decrypt_credentials(self, encrypted: str) -> dict:
        """Decrypt an encrypted credential string back to a dict."""
        if not self._key:
            return json.loads(base64.b64decode(encrypted).decode())
        plaintext = _decrypt(encrypted, self._key)
        return json.loads(plaintext)

    # -------------------------------------------------------------------
    # File-based persistence
    # -------------------------------------------------------------------

    def save_to_file(self, connections: dict[str, StoredConnection]) -> None:
        """Persist all connections to an encrypted JSON file."""
        if not self._key:
            return

        data = {
            cid: {
                "connection_id": sc.connection_id,
                "scheme": sc.scheme,
                "display_name": sc.display_name,
                "encrypted_credentials": sc.encrypted_credentials,
                "created_at": sc.created_at,
                "last_tested": sc.last_tested,
                "is_healthy": sc.is_healthy,
            }
            for cid, sc in connections.items()
        }

        # Encrypt the entire file contents
        payload = json.dumps(data)
        encrypted_payload = _encrypt(payload, self._key)

        self._file_path.parent.mkdir(parents=True, exist_ok=True)
        self._file_path.write_text(encrypted_payload, encoding="utf-8")
        log.debug("Credentials saved to %s", self._file_path)

    def load_from_file(self) -> dict[str, StoredConnection]:
        """Load connections from the encrypted JSON file."""
        if not self._key or not self._file_path.exists():
            return {}

        try:
            encrypted_payload = self._file_path.read_text(encoding="utf-8")
            payload = _decrypt(encrypted_payload, self._key)
            data = json.loads(payload)

            connections = {}
            for cid, entry in data.items():
                connections[cid] = StoredConnection(
                    connection_id=entry["connection_id"],
                    scheme=entry["scheme"],
                    display_name=entry["display_name"],
                    encrypted_credentials=entry["encrypted_credentials"],
                    created_at=entry.get("created_at", 0),
                    last_tested=entry.get("last_tested"),
                    is_healthy=entry.get("is_healthy"),
                )
            log.info("Loaded %d connections from %s", len(connections), self._file_path)
            return connections
        except Exception as exc:
            log.warning("Could not load credentials from %s: %s", self._file_path, exc)
            return {}

    def delete_file(self) -> None:
        """Remove the persisted credential file."""
        if self._file_path.exists():
            self._file_path.unlink()


# Module-level singleton
_store = CredentialStore()


def get_credential_store() -> CredentialStore:
    return _store
