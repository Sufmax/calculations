"""
Configuration pour le script VM
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Charge les variables d'environnement depuis .env si présent
load_dotenv()

class Config:
    """Configuration globale"""

    # WebSocket
    WS_URL = os.getenv('WS_URL', 'wss://your-worker.pages.dev/ws/vm')
    VM_PASSWORD = os.getenv('VM_PASSWORD')

    # Chemins
    BASE_DIR = Path(__file__).parent
    WORK_DIR = BASE_DIR / 'work'
    BLEND_FILE = WORK_DIR / 'current.blend'
    CACHE_DIR = WORK_DIR / 'cache'

    # Blender
    BLENDER_EXECUTABLE = os.getenv('BLENDER_EXECUTABLE', 'blender')
    BLENDER_SCRIPT = BASE_DIR / 'bake_all.py'

    # Timing
    # Intervalle heartbeat réduit à 3s pour marge confortable avec
    # le timeout Worker de 11s (pire cas : heartbeat retardé de ~0.4s
    # par un chunk de 32 KB → arrive à 3.4s, bien sous les 11s)
    HEARTBEAT_INTERVAL = int(os.getenv('HEARTBEAT_INTERVAL', '3'))
    CACHE_CHECK_INTERVAL = float(os.getenv('CACHE_CHECK_INTERVAL', '2.0'))

    # Taille des chunks de cache en bytes.
    # 32 KB : à ~120 KB/s, chaque envoi prend ~0.36s (base64 : ~43 KB)
    # → le heartbeat n'est jamais bloqué plus de 0.36s
    # 1 MB (ancien défaut) : ~11s d'envoi → bloque les heartbeats → timeout
    CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', str(32 * 1024)))

    # Limites
    MAX_RECONNECT_ATTEMPTS = int(os.getenv('MAX_RECONNECT_ATTEMPTS', '10'))
    RECONNECT_DELAY = int(os.getenv('RECONNECT_DELAY', '5'))

    @classmethod
    def ensure_dirs(cls):
        """Crée les répertoires nécessaires"""
        cls.WORK_DIR.mkdir(parents=True, exist_ok=True)
        cls.CACHE_DIR.mkdir(parents=True, exist_ok=True)

    @classmethod
    def validate(cls):
        """Valide la configuration"""
        if not cls.VM_PASSWORD:
            raise ValueError(
                "VM_PASSWORD non défini. "
                "Définissez-le dans .env ou comme variable d'environnement"
            )

        cls.ensure_dirs()

        return True