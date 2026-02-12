"""
pipeline.py — Pipeline 3 threads pour le cache Blender.

Thread 1 (FrameWatcher)  : surveille le cache, détecte les nouvelles frames
Thread 2 (BatchCompressor): accumule N frames, compresse en tar.zst
Thread 3 (BatchUploader)  : upload vers S3/R2, confirme via WebSocket
"""

import asyncio
import io
import logging
import os
import re
import threading
import time
from pathlib import Path
from queue import Queue, Empty
from typing import Dict, List, Optional, Set, Tuple

import boto3
from botocore.config import Config as BotoConfig
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent

from compression import ZstdDictManager, compress_batch
from config import Config
from progress import ProgressTracker, BatchInfo
from utils import format_bytes

logger = logging.getLogger(__name__)

# Extensions de cache Blender surveillées
CACHE_EXTENSIONS = {
    '.bphys', '.vdb', '.uni', '.gz',
    '.png', '.exr', '.abc', '.obj', '.ply',
}

# Regex pour extraire le numéro de frame depuis le nom de fichier
FRAME_PATTERNS = [
    re.compile(r'_(\d{4,6})_\d+\.bphys$'),      # ptcache : prefix_FRAME_index.bphys
    re.compile(r'_(\d{4,6})\.bphys$'),            # ptcache simple
    re.compile(r'_(\d{4,6})\.vdb$'),              # fluids OpenVDB
    re.compile(r'data_(\d{4,6})\.vdb$'),          # Mantaflow
    re.compile(r'_(\d+)\.\w+$'),                  # fallback générique
]


def extract_frame_number(filepath: Path) -> Optional[int]:
    """Extrait le numéro de frame depuis le nom d'un fichier cache."""
    name = filepath.name
    for pattern in FRAME_PATTERNS:
        m = pattern.search(name)
        if m:
            return int(m.group(1))
    return None


# ═══════════════════════════════════════════
# Thread 1 : FrameWatcher
# ═══════════════════════════════════════════

class _CacheEventHandler(FileSystemEventHandler):
    def __init__(self, watcher: 'FrameWatcher'):
        self._watcher = watcher
        super().__init__()

    def on_created(self, event: FileSystemEvent):
        if not event.is_directory:
            self._watcher._on_file(Path(event.src_path))

    def on_modified(self, event: FileSystemEvent):
        if not event.is_directory:
            self._watcher._on_file(Path(event.src_path))


class FrameWatcher:
    """Surveille le dossier cache et envoie les nouvelles frames dans la queue."""

    def __init__(
        self,
        cache_dir: Path,
        frame_queue: Queue,
        progress: ProgressTracker,
        already_secured: Optional[Set[int]] = None,
    ):
        self.cache_dir = cache_dir
        self.frame_queue = frame_queue
        self.progress = progress
        self._seen_files: Set[str] = set()
        self._already_secured = already_secured or set()
        self._observer: Optional[Observer] = None
        self._stop_event = threading.Event()

    def start(self):
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._scan_existing()
        self._observer = Observer()
        self._observer.schedule(
            _CacheEventHandler(self),
            str(self.cache_dir),
            recursive=True,
        )
        self._observer.start()
        logger.info(f"FrameWatcher démarré : {self.cache_dir}")

    def stop(self):
        self._stop_event.set()
        if self._observer:
            self._observer.stop()
            self._observer.join(timeout=5)
        logger.info("FrameWatcher arrêté")

    def _scan_existing(self):
        """Scan initial des fichiers déjà présents."""
        count = 0
        for ext in CACHE_EXTENSIONS:
            for fp in self.cache_dir.rglob(f'*{ext}'):
                if fp.is_file():
                    self._process_file(fp, initial=True)
                    count += 1
        if count > 0:
            logger.info(f"FrameWatcher : {count} fichiers existants détectés")

    def _on_file(self, path: Path):
        if path.suffix.lower() not in CACHE_EXTENSIONS:
            return
        self._process_file(path, initial=False)

    def _process_file(self, path: Path, initial: bool = False):
        key = str(path)
        if key in self._seen_files:
            return
        self._seen_files.add(key)

        frame = extract_frame_number(path)
        if frame is not None:
            self.progress.register_baked_frame(frame)
            # Ne pas re-uploader les frames déjà sécurisées
            if frame in self._already_secured:
                return

        # Attendre stabilité du fichier (écriture terminée)
        if not initial:
            if not self._wait_stable(path):
                return

        self.frame_queue.put(path)

    def _wait_stable(self, path: Path, timeout: float = 3.0) -> bool:
        """Attend que le fichier soit stable (taille constante)."""
        last_size = -1
        waited = 0.0
        while waited < timeout:
            try:
                size = path.stat().st_size
                if size == last_size and size > 0:
                    return True
                last_size = size
            except OSError:
                return False
            time.sleep(0.3)
            waited += 0.3
        return last_size > 0


# ═══════════════════════════════════════════
# Thread 2 : BatchCompressor
# ═══════════════════════════════════════════

class BatchCompressor:
    """Accumule des frames et les compresse en tar.zst par batch."""

    def __init__(
        self,
        cache_dir: Path,
        frame_queue: Queue,
        batch_queue: Queue,
        progress: ProgressTracker,
        dict_manager: ZstdDictManager,
    ):
        self.cache_dir = cache_dir
        self.frame_queue = frame_queue
        self.batch_queue = batch_queue
        self.progress = progress
        self.dict_manager = dict_manager

        self._pending_frames: List[Path] = []
        self._pending_frame_numbers: List[int] = []
        self._dict_training_samples: List[Path] = []
        self._dict_trained = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._batch_size = Config.DEFAULT_BATCH_SIZE

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True, name='Compressor')
        self._thread.start()
        logger.info(f"BatchCompressor démarré (batch initial={self._batch_size})")

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("BatchCompressor arrêté")

    def update_batch_size(self):
        """Recalcule le batch size adaptatif."""
        speed = self.progress.upload_speed_bps
        ratio = self.progress.compression_ratio
        if speed <= 0 or ratio <= 0:
            return
        # Taille brute moyenne par frame
        confirmed = [
            b for b in self.progress.batches.values()
            if b.status == 'confirmed' and len(b.frames) > 0
        ]
        if not confirmed:
            return
        avg_raw_per_frame = sum(
            b.raw_size / len(b.frames) for b in confirmed
        ) / len(confirmed)
        if avg_raw_per_frame <= 0:
            return
        compressed_per_frame = avg_raw_per_frame / ratio
        optimal = (speed * Config.TARGET_UPLOAD_TIME) / compressed_per_frame
        self._batch_size = max(
            Config.MIN_BATCH_SIZE,
            min(Config.MAX_BATCH_SIZE, int(optimal)),
        )

    def flush(self):
        """Force la compression du batch en cours."""
        if self._pending_frames:
            self._compress_batch()

    def _run(self):
        while not self._stop_event.is_set():
            try:
                # Collecter les frames depuis la queue
                try:
                    frame_path = self.frame_queue.get(timeout=Config.BATCH_INTERVAL)
                    self._add_frame(frame_path)
                except Empty:
                    pass

                # Vider la queue rapidement si des frames s'accumulent
                while not self.frame_queue.empty():
                    try:
                        frame_path = self.frame_queue.get_nowait()
                        self._add_frame(frame_path)
                    except Empty:
                        break

                # Compresser si on a assez de frames
                if len(self._pending_frames) >= self._batch_size:
                    self._compress_batch()

            except Exception as e:
                logger.error(f"Erreur BatchCompressor : {e}", exc_info=True)
                time.sleep(1.0)

        # Flush les frames restantes à l'arrêt
        if self._pending_frames:
            self._compress_batch()

    def _add_frame(self, path: Path):
        self._pending_frames.append(path)
        frame_num = extract_frame_number(path)
        if frame_num is not None:
            self._pending_frame_numbers.append(frame_num)

        # Collecter des échantillons pour le dictionnaire
        if not self._dict_trained and len(self._dict_training_samples) < 30:
            self._dict_training_samples.append(path)

    def _compress_batch(self):
        if not self._pending_frames:
            return

        # Entraîner le dictionnaire au premier batch si possible
        if not self._dict_trained and len(self._dict_training_samples) >= Config.ZSTD_MIN_TRAINING_SAMPLES:
            success = self.dict_manager.train(self._dict_training_samples)
            if success:
                self.dict_manager.save_to_file(Config.DICT_FILE)
                self._dict_trained = True

        frames_to_compress = self._pending_frames[:]
        frame_numbers = self._pending_frame_numbers[:]
        self._pending_frames.clear()
        self._pending_frame_numbers.clear()

        batch = self.progress.create_batch(frame_numbers)
        logger.info(
            f"Compression batch #{batch.batch_id} : "
            f"{len(frames_to_compress)} fichiers, frames {frame_numbers[:3]}...{frame_numbers[-1:]}"
        )

        try:
            compressed_data, raw_size = compress_batch(
                frames_to_compress,
                self.cache_dir,
                self.dict_manager,
            )
            self.progress.register_compressed(
                batch.batch_id,
                len(compressed_data),
                raw_size,
            )
            # Passer au thread 3
            self.batch_queue.put((batch.batch_id, compressed_data, frame_numbers))
            self.update_batch_size()
            logger.info(
                f"Batch #{batch.batch_id} compressé : "
                f"{format_bytes(raw_size)} → {format_bytes(len(compressed_data))} "
                f"(x{raw_size / max(len(compressed_data), 1):.1f})"
            )
        except Exception as e:
            self.progress.register_batch_failed(batch.batch_id)
            logger.error(f"Erreur compression batch #{batch.batch_id} : {e}", exc_info=True)


# ═══════════════════════════════════════════
# Thread 3 : BatchUploader
# ═══════════════════════════════════════════

class BatchUploader:
    """Upload les batches compressés vers S3/R2."""

    def __init__(
        self,
        batch_queue: Queue,
        progress: ProgressTracker,
        s3_credentials: Dict,
        ws_client,
        cache_prefix: str,
    ):
        self.batch_queue = batch_queue
        self.progress = progress
        self.ws_client = ws_client
        self.cache_prefix = cache_prefix

        self._s3 = boto3.client(
            's3',
            endpoint_url=s3_credentials['endpoint'],
            aws_access_key_id=s3_credentials['accessKeyId'],
            aws_secret_access_key=s3_credentials['secretAccessKey'],
            region_name=s3_credentials.get('region', 'us-east-1'),
            config=BotoConfig(
                signature_version='s3v4',
                retries={'max_attempts': 3, 'mode': 'adaptive'},
            ),
        )
        self._bucket = s3_credentials['bucket']
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True, name='Uploader')
        self._thread.start()
        logger.info("BatchUploader démarré")

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=30)
        logger.info("BatchUploader arrêté")

    def upload_dict(self, dict_bytes: bytes):
        """Upload le dictionnaire zstd vers S3."""
        key = f"{self.cache_prefix}dictionary.zstd"
        try:
            self._s3.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=dict_bytes,
                ContentType='application/octet-stream',
            )
            logger.info(f"Dictionnaire uploadé : {key} ({format_bytes(len(dict_bytes))})")
        except Exception as e:
            logger.error(f"Erreur upload dictionnaire : {e}")

    def _run(self):
        while not self._stop_event.is_set():
            try:
                batch_id, data, frame_numbers = self.batch_queue.get(timeout=1.0)
                self._upload_batch(batch_id, data, frame_numbers)
            except Empty:
                continue
            except Exception as e:
                logger.error(f"Erreur BatchUploader : {e}", exc_info=True)
                time.sleep(1.0)

    def _upload_batch(self, batch_id: int, data: bytes, frame_numbers: List[int]):
        r2_key = f"{self.cache_prefix}batch_{batch_id:04d}.tar.zst"
        start_time = time.time()

        try:
            if len(data) > Config.S3_MULTIPART_THRESHOLD:
                self._multipart_upload(r2_key, data)
            else:
                self._s3.put_object(
                    Bucket=self._bucket,
                    Key=r2_key,
                    Body=data,
                    ContentType='application/octet-stream',
                    Metadata={
                        'batch_id': str(batch_id),
                        'frames': ','.join(str(f) for f in frame_numbers),
                        'frame_count': str(len(frame_numbers)),
                    },
                )

            duration = time.time() - start_time
            self.progress.register_secured(batch_id, r2_key, duration)

            speed_mbps = (len(data) / duration / 1024 / 1024) if duration > 0 else 0
            logger.info(
                f"Batch #{batch_id} uploadé : {r2_key} "
                f"({format_bytes(len(data))}, {duration:.1f}s, {speed_mbps:.1f} Mo/s)"
            )

            # Notifier le serveur
            self._notify_secured(batch_id, r2_key, frame_numbers, duration)

        except Exception as e:
            self.progress.register_batch_failed(batch_id)
            logger.error(f"Erreur upload batch #{batch_id} : {e}", exc_info=True)

    def _multipart_upload(self, key: str, data: bytes):
        """Upload multipart S3 pour les gros batches."""
        mpu = self._s3.create_multipart_upload(
            Bucket=self._bucket,
            Key=key,
            ContentType='application/octet-stream',
        )
        upload_id = mpu['UploadId']
        parts = []

        try:
            chunk_size = Config.S3_MULTIPART_CHUNK_SIZE
            offset = 0
            part_number = 1

            while offset < len(data):
                chunk = data[offset:offset + chunk_size]
                resp = self._s3.upload_part(
                    Bucket=self._bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=chunk,
                )
                parts.append({
                    'PartNumber': part_number,
                    'ETag': resp['ETag'],
                })
                offset += chunk_size
                part_number += 1

            self._s3.complete_multipart_upload(
                Bucket=self._bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts},
            )
        except Exception:
            self._s3.abort_multipart_upload(
                Bucket=self._bucket,
                Key=key,
                UploadId=upload_id,
            )
            raise

    def _notify_secured(
        self,
        batch_id: int,
        r2_key: str,
        frame_numbers: List[int],
        upload_duration: float,
    ):
        """Envoie la confirmation au serveur via WebSocket."""
        msg = {
            'type': 'PROGRESS_SECURED',
            'frames': frame_numbers,
            'batchId': batch_id,
            'r2Key': r2_key,
            'uploadSpeedBps': int(self.progress.upload_speed_bps),
            'timestamp': time.time(),
        }
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self.ws_client.send(msg), loop
                )
            else:
                asyncio.run(self.ws_client.send(msg))
        except Exception as e:
            logger.warning(f"Erreur notification secured : {e}")


# ═══════════════════════════════════════════
# Orchestrateur Pipeline
# ═══════════════════════════════════════════

class Pipeline:
    """Orchestre les 3 threads du pipeline de cache."""

    def __init__(
        self,
        cache_dir: Path,
        ws_client,
        s3_credentials: Dict,
        total_frames: int = 250,
        already_secured: Optional[Set[int]] = None,
        dict_bytes: Optional[bytes] = None,
    ):
        self.cache_dir = cache_dir
        self.ws_client = ws_client
        self.s3_credentials = s3_credentials
        self.cache_prefix = s3_credentials.get('cachePrefix', 'cache/')

        # Queues inter-threads
        self._frame_queue: Queue = Queue()
        self._batch_queue: Queue = Queue()

        # Progression
        self.progress = ProgressTracker(
            total_frames=total_frames,
            already_secured=already_secured,
        )

        # Dictionnaire zstd
        self.dict_manager = ZstdDictManager()
        if dict_bytes:
            self.dict_manager.load_from_bytes(dict_bytes)
        elif Config.DICT_FILE.exists():
            self.dict_manager.load_from_file(Config.DICT_FILE)

        # Composants
        self.watcher = FrameWatcher(
            cache_dir, self._frame_queue, self.progress, already_secured
        )
        self.compressor = BatchCompressor(
            cache_dir, self._frame_queue, self._batch_queue,
            self.progress, self.dict_manager
        )
        self.uploader = BatchUploader(
            self._batch_queue, self.progress, s3_credentials,
            ws_client, self.cache_prefix
        )

        self._progress_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self):
        """Démarre les 3 threads du pipeline."""
        logger.info("═══ Pipeline démarré ═══")
        self.watcher.start()
        self.compressor.start()
        self.uploader.start()
        self._progress_thread = threading.Thread(
            target=self._progress_loop, daemon=True, name='Progress'
        )
        self._progress_thread.start()

    def stop(self):
        """Arrête proprement les 3 threads."""
        logger.info("Arrêt du pipeline...")
        self._stop_event.set()
        self.watcher.stop()
        self.compressor.stop()
        self.uploader.stop()
        if self._progress_thread:
            self._progress_thread.join(timeout=5)
        logger.info("═══ Pipeline arrêté ═══")

    def finalize(self):
        """Flush le compresseur et attend la fin des uploads."""
        logger.info("Finalisation du pipeline...")
        self.compressor.flush()

        # Attendre que la queue d'upload se vide
        timeout = 120.0
        waited = 0.0
        while not self._batch_queue.empty() and waited < timeout:
            time.sleep(0.5)
            waited += 0.5

        # Upload le dictionnaire si entraîné
        if self.dict_manager.is_trained and self.dict_manager.dict_bytes:
            self.uploader.upload_dict(self.dict_manager.dict_bytes)

        logger.info(
            f"Pipeline finalisé : "
            f"{len(self.progress.secured_frames)}/{self.progress.total_frames} frames sécurisées"
        )

    def _progress_loop(self):
        """Envoie périodiquement la progression via WebSocket."""
        while not self._stop_event.is_set():
            time.sleep(Config.PROGRESS_REPORT_INTERVAL)
            try:
                status = self.progress.get_status_dict()
                status['currentBatchSize'] = self.compressor._batch_size
                msg = {
                    'type': 'PROGRESS_UPDATE',
                    # Champs legacy pour compatibilité
                    'uploadPercent': int(status['securedPercent']),
                    'diskBytes': 0,
                    'diskFiles': status['bakedFrames'],
                    'uploadedBytes': 0,
                    'uploadedFiles': status['securedFrames'],
                    'errors': 0,
                    'rateBytesPerSec': status['uploadSpeedBps'],
                    # Nouveau : état complet
                    'progress': status,
                }
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.run_coroutine_threadsafe(
                        self.ws_client.send(msg), loop
                    )
            except Exception as e:
                logger.debug(f"Erreur envoi progression : {e}")