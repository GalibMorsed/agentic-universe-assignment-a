"""PII protection at rest.

* Encryption (Fernet: AES-128-CBC + HMAC) for values we may need to read back later
  (customer name, full phone number).
* Blind index (keyed HMAC-SHA256 of the canonical phone) so events can be matched to leads
  by phone without any plaintext phone in the database. A plain unkeyed hash would be
  reversible by brute force (only ~4e9 Indian mobile numbers), hence the secret key.

Key handling: env LEDGER_PII_KEY (urlsafe-base64, 32 bytes) wins; otherwise a key file
'<db>.key' is created next to the database (mode 0600). Never commit it.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet


class Crypto:
    def __init__(self, master: bytes):
        if len(master) < 16:
            raise ValueError("PII master key too short")
        self._index_key = hmac.digest(master, b"ledger/phone-blind-index", "sha256")
        enc_key = hmac.digest(master, b"ledger/pii-encryption", "sha256")
        self._fernet = Fernet(base64.urlsafe_b64encode(enc_key))

    def phone_hash(self, canonical_phone: str) -> bytes:
        return hmac.digest(self._index_key, canonical_phone.encode("ascii"), "sha256")

    def encrypt(self, obj: Any) -> bytes:
        return self._fernet.encrypt(json.dumps(obj, ensure_ascii=False, sort_keys=True).encode())

    def decrypt(self, token: bytes) -> Any:
        return json.loads(self._fernet.decrypt(token))

    def key_check(self) -> str:
        """Stored in the DB so that opening it with the wrong key fails loudly."""
        return hashlib.sha256(self._index_key + b"check").hexdigest()


def load_crypto(db_path: str) -> Crypto:
    env = os.environ.get("LEDGER_PII_KEY")
    if env:
        return Crypto(base64.urlsafe_b64decode(env))
    key_path = Path(db_path + ".key")
    if not key_path.exists():
        fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as fh:
            fh.write(base64.urlsafe_b64encode(os.urandom(32)))
    return Crypto(base64.urlsafe_b64decode(key_path.read_bytes().strip()))
