"""
Uploader via presigned URLs pour cache Blender → Storj.
Ne nécessite aucune clé S3 côté VM.
"""

import logging
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

CONTENT_TYPES: Dict[str, str] = {
    '.bphys': 'application/octet-stream',
    '.vdb': 'application/octet-stream',
    '.uni': 'application/octet-stream',
    '.gz': 'application/gzip',
    '.png': 'image/png',
    '.exr': 'application/octet-stream',
    '.abc': 'application/octet-stream',
    '.obj': 'text/plain',
    '.ply': 'application/octet-stream',
}


class PresignedUploader:
    """Upload de fichiers via presigned PUT URLs."""

    def __init__(self):
        self.total_bytes_uploaded = 0
        self.total_files_uploaded = 0
        self.total_errors = 0
        self.last_error: Optional[str] = None

    def upload_file(
        self,
        file_path: Path,
        presigned_url: str,
        content_type: Optional[str] = None,
        max_retries: int = 3,
    ) -> bool:
        """Upload un fichier vers le presigned URL."""
        if not file_path.exists():
            logger.warning(f"Fichier introuvable: {file_path}")
            return False

        if content_type is None:
            content_type = CONTENT_TYPES.get(
                file_path.suffix.lower(), 'application/octet-stream'
            )

        data = file_path.read_bytes()
        file_size = len(data)

        for attempt in range(1, max_retries + 1):
            try:
                req = urllib.request.Request(
                    presigned_url,
                    data=data,
                    headers={
                        'Content-Type': content_type,
                        'Content-Length': str(file_size),
                    },
                    method='PUT',
                )

                with urllib.request.urlopen(req, timeout=120) as resp:
                    if resp.status not in (200, 201, 204):
                        body = resp.read().decode('utf-8', errors='replace')
                        raise RuntimeError(f"PUT {resp.status}: {body[:500]}")

                self.total_bytes_uploaded += file_size
                self.total_files_uploaded += 1
                return True

            except (urllib.error.HTTPError, urllib.error.URLError, RuntimeError) as e:
                self.last_error = str(e)
                if attempt < max_retries:
                    delay = 2 ** (attempt - 1)
                    logger.warning(
                        f"Upload échoué ({attempt}/{max_retries}): "
                        f"{file_path.name} → {e} — retry dans {delay}s"
                    )
                    time.sleep(delay)
                else:
                    self.total_errors += 1
                    logger.error(
                        f"Upload abandonné après {max_retries} tentatives: "
                        f"{file_path.name} → {e}"
                    )
                    return False

        return False

    def get_stats(self) -> Dict[str, object]:
        return {
            'total_bytes_uploaded': self.total_bytes_uploaded,
            'total_files_uploaded': self.total_files_uploaded,
            'total_errors': self.total_errors,
            'last_error': self.last_error,
        }