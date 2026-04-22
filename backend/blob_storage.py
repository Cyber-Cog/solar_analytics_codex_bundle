"""
Optional Vercel Blob storage helpers for serverless uploads.
"""

from __future__ import annotations

import os
from typing import Optional

from vercel.blob import BlobClient


_TOKEN = os.getenv("BLOB_READ_WRITE_TOKEN", "").strip()
_STORE_UPLOADS = os.getenv("ENABLE_BLOB_UPLOADS", "").strip().lower() in {
    "1",
    "true",
    "yes",
}


def blob_uploads_enabled() -> bool:
    return bool(_TOKEN and _STORE_UPLOADS)


def upload_bytes(pathname: str, content: bytes, content_type: Optional[str] = None) -> str:
    if not blob_uploads_enabled():
        raise RuntimeError("Vercel Blob uploads are not enabled")

    client = BlobClient(token=_TOKEN)
    blob = client.put(
        pathname,
        content,
        access=os.getenv("BLOB_ACCESS", "public"),
        content_type=content_type or "application/octet-stream",
        add_random_suffix=True,
    )
    return blob.url
