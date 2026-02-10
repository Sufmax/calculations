"""
Thread/Task dédié au streaming du cache Blender.

Surveille le répertoire de cache en temps réel via watchdog.
Dès qu'un fichier apparaît ou est modifié, il est découpé en chunks
et envoyé au serveur via WebSocket sans attendre la fin du bake.
"""

import asyncio
import logging
import math
from pathlib import Path
from typing import Set, Optional
import time

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent

from config import Config
from utils import chunk_file, format_bytes

logger = logging.getLogger(__name__)

# Extensions de fichiers de cache Blender reconnues
CACHE_EXTENSIONS = {
    '.bphys',
    '.vdb',
    '.uni',
    '.gz',
    '.png',
    '.exr',
    '.abc',
    '.obj',
    '.ply',
}


class CacheFileHandler(FileSystemEventHandler):
    """Handler watchdog pour détecter les nouveaux fichiers de cache."""

    def __init__(self, cache_streamer: 'CacheStreamer'):
        self.streamer = cache_streamer
        super().__init__()

    def _should_process(self, src_path: str) -> bool:
        p = Path(src_path)
        return p.suffix.lower() in CACHE_EXTENSIONS

    def on_created(self, event: FileSystemEvent):
        if not event.is_directory and self._should_process(event.src_path):
            logger.debug(f"Fichier créé: {event.src_path}")
            self.streamer.schedule_file(Path(event.src_path))

    def on_modified(self, event: FileSystemEvent):
        if not event.is_directory and self._should_process(event.src_path):
            logger.debug(f"Fichier modifié: {event.src_path}")
            self.streamer.schedule_file(Path(event.src_path))


class CacheStreamer:
    """Streamer de cache Blender vers le serveur."""

    def __init__(self, cache_dir: Path, ws_client):
        self.cache_dir = cache_dir
        self.ws_client = ws_client
        self.queue: asyncio.Queue = asyncio.Queue()
        self.processed_files: Set[str] = set()
        self.pending_files: Set[str] = set()
        self.is_running = False
        self.observer: Optional[Observer] = None
        self.stream_task: Optional[asyncio.Task] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # Stats
        self.total_bytes_sent = 0
        self.total_chunks_sent = 0
        self.total_files_sent = 0
        self.start_time = time.time()

    def start(self):
        """Démarre le streamer."""
        logger.info(f"Démarrage du streamer de cache: {self.cache_dir}")
        self.is_running = True
        self._loop = asyncio.get_event_loop()
        self._start_watching()
        self.stream_task = asyncio.create_task(self._stream_loop())
        self._scan_existing_files()

    def stop(self):
        """Arrête le streamer proprement."""
        logger.info("Arrêt du streamer de cache")
        self.is_running = False

        if self.observer:
            self.observer.stop()
            self.observer.join(timeout=5)

        if self.stream_task:
            self.stream_task.cancel()

    def _start_watching(self):
        """Démarre la surveillance du répertoire cache via watchdog."""
        self.observer = Observer()
        event_handler = CacheFileHandler(self)
        self.observer.schedule(event_handler, str(self.cache_dir), recursive=True)
        self.observer.start()
        logger.info("Surveillance du cache activée")

    def _scan_existing_files(self):
        """Scanne les fichiers de cache déjà présents dans le répertoire."""
        if not self.cache_dir.exists():
            logger.warning(f"Répertoire cache inexistant: {self.cache_dir}")
            return

        files_found = 0
        for ext in CACHE_EXTENSIONS:
            for file_path in self.cache_dir.rglob(f'*{ext}'):
                if file_path.is_file():
                    self._queue_file(file_path)
                    files_found += 1

        logger.info(f"{files_found} fichiers de cache existants trouvés")

    def schedule_file(self, file_path: Path):
        """Ajoute un fichier à la queue de façon thread-safe."""
        if self._loop is None or not self.is_running:
            return
        try:
            self._loop.call_soon_threadsafe(self._queue_file, file_path)
        except RuntimeError:
            pass

    def _queue_file(self, file_path: Path):
        """Ajoute un fichier à la queue (doit être appelé sur le loop asyncio)."""
        file_key = str(file_path)

        if file_key in self.processed_files or file_key in self.pending_files:
            return

        self.pending_files.add(file_key)
        self.queue.put_nowait(file_path)

    async def _stream_loop(self):
        """Boucle principale de streaming des fichiers de cache."""
        logger.info("Boucle de streaming démarrée")

        try:
            while self.is_running:
                try:
                    file_path = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=1.0
                    )

                    await self._stream_file(file_path)

                except asyncio.TimeoutError:
                    continue

                except Exception as e:
                    logger.error(f"Erreur dans stream_loop: {e}", exc_info=True)
                    await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            logger.info("Stream loop annulée")

        logger.info("Boucle de streaming terminée")

    async def _stream_file(self, file_path: Path):
        """Stream un fichier vers le serveur, chunk par chunk."""
        file_key = str(file_path)

        if file_key in self.processed_files:
            self.pending_files.discard(file_key)
            return

        if not self.ws_client.is_connected():
            logger.warning("Non connecté, report du streaming")
            self.pending_files.discard(file_key)
            return

        if not file_path.exists():
            logger.warning(f"Fichier disparu: {file_path}")
            self.pending_files.discard(file_key)
            return

        await self._wait_file_stable(file_path)

        try:
            file_size = file_path.stat().st_size
        except OSError:
            self.pending_files.discard(file_key)
            return

        if file_size == 0:
            logger.debug(f"Fichier vide ignoré: {file_path.name}")
            self.processed_files.add(file_key)
            self.pending_files.discard(file_key)
            return

        # Nombre total de chunks pour ce fichier
        total_chunks = max(1, math.ceil(file_size / Config.CHUNK_SIZE))

        logger.info(
            f"Streaming: {file_path.name} "
            f"({format_bytes(file_size)}, "
            f"{total_chunks} chunk(s) prévu(s), "
            f"CHUNK_SIZE={Config.CHUNK_SIZE})"
        )

        try:
            relative_path = file_path.relative_to(self.cache_dir)
            safe_path = str(relative_path).replace("/", "__").replace("\\", "__")

            chunk_count = 0
            for chunk_id, chunk_data in chunk_file(file_path, Config.CHUNK_SIZE):
                chunk_key = f"{safe_path}_chunk{chunk_id}"
                is_final = (chunk_id == total_chunks - 1)

                success = await self.ws_client.send_cache_chunk(
                    chunk_key,
                    chunk_data,
                    final=is_final
                )

                if not success:
                    logger.error(
                        f"Échec envoi chunk {chunk_key} "
                        f"({chunk_id + 1}/{total_chunks})"
                    )
                    self.pending_files.discard(file_key)
                    return

                chunk_count += 1
                self.total_chunks_sent += 1
                self.total_bytes_sent += len(chunk_data)

                # Yield pour laisser le heartbeat s'exécuter entre les chunks
                await asyncio.sleep(0.01)

            self.processed_files.add(file_key)
            self.pending_files.discard(file_key)
            self.total_files_sent += 1

            logger.info(
                f"✓ {file_path.name} streamé "
                f"({chunk_count} chunks, {format_bytes(file_size)})"
            )

        except Exception as e:
            logger.error(f"Erreur streaming {file_path}: {e}", exc_info=True)
            self.pending_files.discard(file_key)

    async def _wait_file_stable(self, file_path: Path, max_wait: float = 5.0):
        """Attend que le fichier soit stable (taille constante)."""
        last_size = -1
        wait_time = 0.0
        check_interval = 0.5

        while wait_time < max_wait:
            try:
                current_size = file_path.stat().st_size

                if current_size == last_size and current_size > 0:
                    return

                last_size = current_size
                await asyncio.sleep(check_interval)
                wait_time += check_interval

            except OSError:
                await asyncio.sleep(check_interval)
                wait_time += check_interval

    async def finalize(self):
        """Finalise le streaming : vide la queue et envoie CACHE_COMPLETE."""
        logger.info("Finalisation du streaming...")

        timeout = 30.0
        waited = 0.0
        while not self.queue.empty() and waited < timeout:
            await asyncio.sleep(0.1)
            waited += 0.1

        await self.ws_client.send_cache_complete()

        elapsed = time.time() - self.start_time
        rate = self.total_bytes_sent / elapsed if elapsed > 0 else 0
        logger.info(
            f"Streaming terminé: {self.total_files_sent} fichiers, "
            f"{self.total_chunks_sent} chunks, "
            f"{format_bytes(self.total_bytes_sent)} en {elapsed:.1f}s "
            f"({format_bytes(rate)}/s)"
        )

    def get_stats(self) -> dict:
        """Retourne les statistiques de streaming."""
        elapsed = time.time() - self.start_time

        return {
            'total_bytes': self.total_bytes_sent,
            'total_chunks': self.total_chunks_sent,
            'total_files': self.total_files_sent,
            'elapsed_seconds': elapsed,
            'bytes_per_second': self.total_bytes_sent / elapsed if elapsed > 0 else 0,
            'files_processed': len(self.processed_files),
        }
        