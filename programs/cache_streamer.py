"""
Thread/Task dédié au streaming du cache Blender
"""

import asyncio
import logging
from pathlib import Path
from typing import Set, Optional
import time

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent

from config import Config
from utils import chunk_file, format_bytes

logger = logging.getLogger(__name__)


class CacheFileHandler(FileSystemEventHandler):
    """Handler pour détecter les changements dans le cache"""
    
    def __init__(self, cache_streamer: 'CacheStreamer'):
        self.streamer = cache_streamer
        super().__init__()
    
    def on_created(self, event: FileSystemEvent):
        if not event.is_directory:
            logger.debug(f"Fichier créé: {event.src_path}")
            self.streamer.queue_file(Path(event.src_path))
    
    def on_modified(self, event: FileSystemEvent):
        if not event.is_directory:
            logger.debug(f"Fichier modifié: {event.src_path}")
            self.streamer.queue_file(Path(event.src_path))


class CacheStreamer:
    """Streamer de cache Blender vers le serveur"""
    
    def __init__(self, cache_dir: Path, ws_client):
        self.cache_dir = cache_dir
        self.ws_client = ws_client
        self.queue: asyncio.Queue = asyncio.Queue()
        self.processed_files: Set[str] = set()
        self.is_running = False
        self.observer: Optional[Observer] = None
        self.stream_task: Optional[asyncio.Task] = None
        
        # Stats
        self.total_bytes_sent = 0
        self.total_chunks_sent = 0
        self.start_time = time.time()
    
    def start(self):
        """Démarre le streamer"""
        logger.info(f"Démarrage du streamer de cache: {self.cache_dir}")
        self.is_running = True
        
        # Démarre le watchdog pour surveiller les changements
        self.start_watching()
        
        # Démarre la task de streaming
        self.stream_task = asyncio.create_task(self.stream_loop())
        
        # Ajoute les fichiers existants à la queue
        self.scan_existing_files()
    
    def stop(self):
        """Arrête le streamer"""
        logger.info("Arrêt du streamer de cache")
        self.is_running = False
        
        if self.observer:
            self.observer.stop()
            self.observer.join()
        
        if self.stream_task:
            self.stream_task.cancel()
    
    def start_watching(self):
        """Démarre la surveillance du répertoire cache"""
        self.observer = Observer()
        event_handler = CacheFileHandler(self)
        self.observer.schedule(event_handler, str(self.cache_dir), recursive=True)
        self.observer.start()
        logger.info("Surveillance du cache activée")
    
    def scan_existing_files(self):
        """Scanne les fichiers de cache existants"""
        if not self.cache_dir.exists():
            logger.warning(f"Répertoire cache inexistant: {self.cache_dir}")
            return
        
        # Extensions de fichiers de cache Blender
        cache_extensions = {'.bphys', '.vdb', '.png', '.exr', '.abc', '.obj', '.ply', '.uni', '.gz'}
        
        files_found = 0
        for ext in cache_extensions:
            for file_path in self.cache_dir.rglob(f'*{ext}'):
                if file_path.is_file():
                    self.queue_file(file_path)
                    files_found += 1
        
        logger.info(f"{files_found} fichiers de cache existants trouvés")
    
    def queue_file(self, file_path: Path):
        """Ajoute un fichier à la queue de streaming"""
        file_key = str(file_path)
        
        if file_key not in self.processed_files:
            self.queue.put_nowait(file_path)
    
    async def stream_loop(self):
        """Boucle principale de streaming"""
        logger.info("Boucle de streaming démarrée")
        
        try:
            while self.is_running:
                try:
                    # Récupère le prochain fichier (avec timeout)
                    file_path = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=1.0
                    )
                    
                    # Stream le fichier
                    await self.stream_file(file_path)
                    
                except asyncio.TimeoutError:
                    # Pas de fichier dans la queue, on continue
                    continue
                    
                except Exception as e:
                    logger.error(f"Erreur dans stream_loop: {e}", exc_info=True)
                    await asyncio.sleep(1.0)
                    
        except asyncio.CancelledError:
            logger.info("Stream loop annulée")
        
        logger.info("Boucle de streaming terminée")
    
    async def stream_file(self, file_path: Path):
        """Stream un fichier vers le serveur"""
        if not self.ws_client.is_connected():
            logger.warning("Non connecté, impossible de streamer")
            return
        
        file_key = str(file_path)
        
        # Vérifie si le fichier existe toujours
        if not file_path.exists():
            logger.warning(f"Fichier disparu: {file_path}")
            return
        
        # Attend que le fichier soit stable (pas en cours d'écriture)
        await self.wait_file_stable(file_path)
        
        file_size = file_path.stat().st_size
        logger.info(f"Streaming: {file_path.name} ({format_bytes(file_size)})")
        
        try:
            chunk_count = 0
            relative_path = file_path.relative_to(self.cache_dir)
            
            # Stream par chunks
            for chunk_id, chunk_data in chunk_file(file_path, Config.CHUNK_SIZE):
                chunk_key = f"{relative_path}_{chunk_id}"
                
                success = await self.ws_client.send_cache_chunk(
                    chunk_key,
                    chunk_data,
                    final=False
                )
                
                if not success:
                    logger.error(f"Échec envoi chunk {chunk_id}")
                    return
                
                chunk_count += 1
                self.total_chunks_sent += 1
                self.total_bytes_sent += len(chunk_data)
                
                # Petit délai pour éviter de saturer
                await asyncio.sleep(0.01)
            
            # Marque comme traité
            self.processed_files.add(file_key)
            
            logger.info(
                f"✓ {file_path.name} streamé "
                f"({chunk_count} chunks, {format_bytes(file_size)})"
            )
            
        except Exception as e:
            logger.error(f"Erreur streaming {file_path}: {e}", exc_info=True)
    
    async def wait_file_stable(self, file_path: Path, max_wait: float = 5.0):
        """Attend que le fichier soit stable (pas en cours d'écriture)"""
        last_size = -1
        wait_time = 0.0
        check_interval = 0.5
        
        while wait_time < max_wait:
            try:
                current_size = file_path.stat().st_size
                
                if current_size == last_size:
                    # Taille stable, on peut y aller
                    return
                
                last_size = current_size
                await asyncio.sleep(check_interval)
                wait_time += check_interval
                
            except OSError:
                # Fichier inaccessible temporairement
                await asyncio.sleep(check_interval)
                wait_time += check_interval
    
    async def finalize(self):
        """Finalise le streaming et envoie le signal de complétion"""
        logger.info("Finalisation du streaming...")
        
        # Attend que la queue soit vide
        while not self.queue.empty():
            await asyncio.sleep(0.1)
        
        # Envoie le signal de complétion
        await self.ws_client.send_cache_complete()
        
        elapsed = time.time() - self.start_time
        logger.info(
            f"Streaming terminé: {self.total_chunks_sent} chunks, "
            f"{format_bytes(self.total_bytes_sent)} en {elapsed:.1f}s "
            f"({format_bytes(self.total_bytes_sent / elapsed)}/s)"
        )
    
    def get_stats(self) -> dict:
        """Retourne les statistiques de streaming"""
        elapsed = time.time() - self.start_time
        
        return {
            'total_bytes': self.total_bytes_sent,
            'total_chunks': self.total_chunks_sent,
            'elapsed_seconds': elapsed,
            'bytes_per_second': self.total_bytes_sent / elapsed if elapsed > 0 else 0,
            'files_processed': len(self.processed_files),
        }
