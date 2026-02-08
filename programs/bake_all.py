#!/usr/bin/env python3
"""
bake_all.py — Script Blender (exécution en mode background)

Objectif :
- Forcer Blender à écrire TOUS les caches (ptcache, fluids, rigidbody…)
  dans un répertoire unique (--cache-dir)
- Lancer un bake robuste en mode background (headless)
- Produire un cache_manifest.json pour le cache_streamer

Usage :
  blender --background fichier.blend --python bake_all.py -- --cache-dir /path/to/cache

Compatibilité : Blender 3.2+ (3.6 LTS ciblé), Python 3.8+
Aucun droit superutilisateur requis.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import bpy

# ─────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────

# Sous-dossiers de cache (spec)
CACHE_SUBDIRS = ("ptcache", "fluids", "rigidbody", "alembic", "geonodes")

# Extensions de cache reconnues par cache_streamer.py
CACHE_EXTENSIONS = {".bphys", ".vdb", ".png", ".exr", ".abc", ".obj", ".ply"}

# ─────────────────────────────────────────────
# État global pour gestion des signaux
# ─────────────────────────────────────────────

_interrupted = False
_interrupt_count = 0


# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────

def log(msg: str) -> None:
    """Log standard, préfixé pour identification par blender_runner."""
    print(f"[BAKE_ALL] {msg}", flush=True)


def warn(msg: str) -> None:
    print(f"[BAKE_ALL][WARN] {msg}", flush=True)


def err(msg: str) -> None:
    print(f"[BAKE_ALL][ERROR] {msg}", flush=True)


# ─────────────────────────────────────────────
# Gestion des signaux (SIGTERM / SIGINT)
# ─────────────────────────────────────────────

def _signal_handler(signum: int, frame: Any) -> None:
    """Gestionnaire de signaux pour arrêt propre."""
    global _interrupted, _interrupt_count
    _interrupted = True
    _interrupt_count += 1

    sig_name = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
    warn(f"Signal {sig_name} reçu (#{_interrupt_count})")

    if _interrupt_count >= 3:
        err("3 interruptions reçues → arrêt immédiat")
        sys.exit(1)


def install_signal_handlers() -> None:
    """Installe les gestionnaires SIGTERM et SIGINT."""
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _signal_handler)
        except (OSError, ValueError):
            # Peut échouer dans certains contextes embarqués
            pass


# ─────────────────────────────────────────────
# Parsing d'arguments CLI
# ─────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    """Parse les arguments après '--' (standard Blender)."""
    argv = sys.argv
    if "--" in argv:
        argv = argv[argv.index("--") + 1:]
    else:
        argv = []

    parser = argparse.ArgumentParser(description="Blender bake-all helper")

    # Obligatoire
    parser.add_argument(
        "--cache-dir", required=True,
        help="Répertoire racine où écrire TOUS les caches"
    )

    # Frame range
    parser.add_argument("--frame-start", type=int, default=None,
                        help="Override du frame de début")
    parser.add_argument("--frame-end", type=int, default=None,
                        help="Override du frame de fin")

    # Contrôle des caches
    parser.add_argument("--clear-existing", action="store_true",
                        help="Purge des caches existants avant bake")

    # Toggles par type de simulation (activés par défaut)
    parser.add_argument("--bake-fluids", action="store_true", default=True,
                        help="Activer bake fluides (défaut: activé)")
    parser.add_argument("--no-bake-fluids", dest="bake_fluids", action="store_false",
                        help="Désactiver bake fluides")

    parser.add_argument("--bake-particles", action="store_true", default=True,
                        help="Activer bake particules (défaut: activé)")
    parser.add_argument("--no-bake-particles", dest="bake_particles", action="store_false",
                        help="Désactiver bake particules")

    parser.add_argument("--bake-cloth", action="store_true", default=True,
                        help="Activer bake cloth (défaut: activé)")
    parser.add_argument("--no-bake-cloth", dest="bake_cloth", action="store_false",
                        help="Désactiver bake cloth")

    # Modes
    parser.add_argument("--strict", action="store_true",
                        help="Si erreur → exit(1) au lieu de continuer")
    parser.add_argument("--all-scenes", action="store_true",
                        help="Bake toutes les scènes (pas seulement active)")
    parser.add_argument("--verbose", action="store_true",
                        help="Logs détaillés")

    return parser.parse_args(argv)


# ─────────────────────────────────────────────
# Vérifications et initialisation
# ─────────────────────────────────────────────

def verify_blend_loaded() -> bool:
    """Vérifie qu'un fichier .blend est ouvert."""
    if not bpy.data.filepath:
        err("Aucun fichier .blend chargé (bpy.data.filepath vide)")
        return False
    log(f"Fichier .blend chargé : {bpy.data.filepath}")
    return True


def setup_cache_directories(cache_root: Path) -> Dict[str, Path]:
    """Crée l'arborescence de cache complète.

    Retourne un dict {nom_logique: chemin_absolu}.
    """
    dirs: Dict[str, Path] = {}
    for name in CACHE_SUBDIRS:
        d = cache_root / name
        d.mkdir(parents=True, exist_ok=True)
        dirs[name] = d
    return dirs


# ─────────────────────────────────────────────
# Redirection ptcache via symlink
# ─────────────────────────────────────────────

def setup_ptcache_symlink(cache_root: Path, verbose: bool = False) -> bool:
    """Crée un lien symbolique blendcache_<nom> → <CACHE_DIR>/ptcache/.

    Blender écrit les point caches (.bphys) dans un dossier
    blendcache_<stem>/ à côté du .blend. En créant un symlink,
    ces fichiers atterrissent directement dans cache_root/ptcache/
    et sont visibles par cache_streamer.py en temps réel.

    Retourne True si le symlink est en place, False sinon.
    """
    blend_path = Path(bpy.data.filepath)
    if not blend_path.exists():
        warn("Fichier .blend introuvable sur disque, symlink impossible")
        return False

    blendcache_name = f"blendcache_{blend_path.stem}"
    blendcache_dir = blend_path.parent / blendcache_name
    target = cache_root / "ptcache"

    # Cas 1 : symlink déjà correct
    if blendcache_dir.is_symlink():
        try:
            if blendcache_dir.resolve() == target.resolve():
                if verbose:
                    log(f"Symlink déjà correct : {blendcache_dir} → {target}")
                return True
        except OSError:
            pass
        # Symlink incorrect → supprimer
        try:
            blendcache_dir.unlink()
        except OSError as e:
            warn(f"Impossible de supprimer l'ancien symlink : {e}")
            return False

    # Cas 2 : vrai dossier existant → migrer le contenu
    if blendcache_dir.is_dir() and not blendcache_dir.is_symlink():
        if verbose:
            log(f"Migration du contenu de {blendcache_dir} vers {target}")
        try:
            for f in blendcache_dir.rglob("*"):
                if f.is_file():
                    dest = target / f.relative_to(blendcache_dir)
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(str(f), str(dest))
            shutil.rmtree(str(blendcache_dir), ignore_errors=True)
        except OSError as e:
            warn(f"Erreur migration blendcache : {e}")
            return False

    # Cas 3 : fichier quelconque bloquant
    if blendcache_dir.exists() and not blendcache_dir.is_dir():
        try:
            blendcache_dir.unlink()
        except OSError as e:
            warn(f"Impossible de supprimer {blendcache_dir} : {e}")
            return False

    # Création du symlink
    try:
        blendcache_dir.symlink_to(target, target_is_directory=True)
        log(f"Symlink créé : {blendcache_dir} → {target}")
        return True
    except OSError as e:
        warn(f"Échec création symlink : {e}")
        warn("Les fichiers ptcache devront être récupérés après le bake")
        return False


# ─────────────────────────────────────────────
# Configuration des chemins de cache
# ─────────────────────────────────────────────

def configure_fluid_domains(
    scene: bpy.types.Scene,
    fluids_dir: Path,
    verbose: bool = False,
) -> int:
    """Redirige le cache_directory de tous les fluid domains vers fluids_dir.

    Tente aussi de forcer le format OpenVDB pour que cache_streamer
    détecte les fichiers .vdb.

    Retourne le nombre de domains configurés.
    """
    count = 0
    for obj in scene.objects:
        for mod in obj.modifiers:
            if mod.type != "FLUID":
                continue
            # Accès au fluid_type (peut être NONE, DOMAIN, FLOW, EFFECTOR)
            fluid_type = getattr(mod, "fluid_type", None)
            if fluid_type != "DOMAIN":
                continue
            ds = getattr(mod, "domain_settings", None)
            if ds is None:
                continue
            try:
                ds.cache_directory = str(fluids_dir)
                # Forcer OpenVDB si disponible (streamer surveille .vdb)
                if hasattr(ds, "cache_data_format"):
                    ds.cache_data_format = "OPENVDB"
                if hasattr(ds, "openvdb_cache_compress_type"):
                    ds.openvdb_cache_compress_type = "BLOSC"
                count += 1
                if verbose:
                    log(f"Fluid domain '{obj.name}' → {fluids_dir} (OpenVDB)")
            except Exception as e:
                warn(f"Erreur config fluid domain '{obj.name}' : {e}")
    return count


def configure_disk_caches(
    scene: bpy.types.Scene,
    verbose: bool = False,
) -> int:
    """Active use_disk_cache et désactive use_external sur tous les point_cache.

    Cela force Blender à écrire les .bphys sur disque (dans blendcache_*,
    redirigé via symlink vers ptcache/).

    Retourne le nombre de caches configurés.
    """
    count = 0

    def _configure_pc(pc: Any, label: str) -> bool:
        """Configure un PointCache individuel."""
        nonlocal count
        try:
            if hasattr(pc, "use_disk_cache"):
                pc.use_disk_cache = True
            if hasattr(pc, "use_external"):
                pc.use_external = False
            if hasattr(pc, "use_library_path"):
                pc.use_library_path = False
            count += 1
            if verbose:
                log(f"  Disk cache activé : {label}")
            return True
        except Exception as e:
            if verbose:
                warn(f"  Erreur config cache {label} : {e}")
            return False

    # Rigid Body World
    rbw = getattr(scene, "rigidbody_world", None)
    if rbw is not None:
        pc = getattr(rbw, "point_cache", None)
        if pc is not None:
            _configure_pc(pc, "RigidBodyWorld")

    # Objets
    for obj in scene.objects:
        # Particle systems
        for i, psys in enumerate(getattr(obj, "particle_systems", [])):
            pc = getattr(psys, "point_cache", None)
            if pc is not None:
                _configure_pc(pc, f"{obj.name}/particles[{i}]")

        # Modifiers avec point_cache
        for mod in obj.modifiers:
            # Cloth, SoftBody → point_cache direct
            if mod.type in ("CLOTH", "SOFT_BODY"):
                pc = getattr(mod, "point_cache", None)
                if pc is not None:
                    _configure_pc(pc, f"{obj.name}/{mod.name}")

            # Dynamic Paint → surfaces
            elif mod.type == "DYNAMIC_PAINT":
                canvas = getattr(mod, "canvas_settings", None)
                if canvas is not None and hasattr(canvas, "canvas_surfaces"):
                    for j, surf in enumerate(canvas.canvas_surfaces):
                        pc = getattr(surf, "point_cache", None)
                        if pc is not None:
                            _configure_pc(pc, f"{obj.name}/DynPaint[{j}]")

            # Autres modifiers avec point_cache générique
            elif mod.type not in ("FLUID",):
                pc = getattr(mod, "point_cache", None)
                if pc is not None:
                    _configure_pc(pc, f"{obj.name}/{mod.name}")

    if verbose:
        log(f"Total caches disque configurés : {count}")
    return count


# ─────────────────────────────────────────────
# Optimisation performances (sans root)
# ─────────────────────────────────────────────

def optimize_performance(scene: bpy.types.Scene) -> Dict[str, Any]:
    """Tente de maximiser les performances CPU/IO sans droits root."""
    report: Dict[str, Any] = {"cpu_count": os.cpu_count() or 1}

    # Affinité CPU → utiliser tous les cœurs
    try:
        if hasattr(os, "sched_setaffinity"):
            cpu_count = os.cpu_count() or 1
            os.sched_setaffinity(0, set(range(cpu_count)))
            report["affinity"] = f"all({cpu_count})"
        else:
            report["affinity"] = "not_supported"
    except Exception as e:
        report["affinity"] = f"error: {e}"

    # I/O priority via ionice (best-effort, prio 0)
    try:
        pid = str(os.getpid())
        res = subprocess.run(
            ["ionice", "-c2", "-n0", "-p", pid],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, check=False, timeout=5,
        )
        report["ionice"] = "ok" if res.returncode == 0 else f"failed({res.returncode})"
    except FileNotFoundError:
        report["ionice"] = "not_installed"
    except Exception as e:
        report["ionice"] = f"error: {e}"

    # Threads Blender
    try:
        cpu_count = os.cpu_count() or 1
        scene.render.threads_mode = "AUTO"
        scene.render.threads = cpu_count
        report["blender_threads"] = cpu_count
    except Exception:
        pass

    try:
        prefs = bpy.context.preferences
        if hasattr(prefs, "system") and hasattr(prefs.system, "threads"):
            prefs.system.threads = os.cpu_count() or 1
    except Exception:
        pass

    return report


# ─────────────────────────────────────────────
# Utilitaires de contexte Blender
# ─────────────────────────────────────────────

def activate_scene(scene: bpy.types.Scene) -> None:
    """Active une scène dans le contexte courant."""
    try:
        window = bpy.context.window
        if window is not None:
            window.scene = scene
    except Exception:
        pass
    # Forcer la mise à jour du depsgraph
    try:
        bpy.context.view_layer.update()
    except Exception:
        pass


def activate_object(scene: bpy.types.Scene, obj: bpy.types.Object) -> None:
    """Sélectionne et active un objet dans la scène."""
    activate_scene(scene)
    try:
        # Désélectionner tout
        for o in bpy.context.view_layer.objects:
            o.select_set(False)
        # Sélectionner et activer l'objet cible
        obj.select_set(True)
        bpy.context.view_layer.objects.active = obj
    except Exception:
        pass


# ─────────────────────────────────────────────
# Nettoyage des caches existants
# ─────────────────────────────────────────────

def clear_all_caches(scene: bpy.types.Scene, verbose: bool = False) -> None:
    """Libère tous les caches existants (ptcache + fluids)."""
    activate_scene(scene)

    # Free ptcache
    try:
        bpy.ops.ptcache.free_bake_all()
        if verbose:
            log("ptcache.free_bake_all → OK")
    except Exception as e:
        if verbose:
            warn(f"ptcache.free_bake_all échoué (non bloquant) : {e}")

    # Free fluids
    for obj in scene.objects:
        for mod in obj.modifiers:
            if mod.type == "FLUID" and getattr(mod, "fluid_type", None) == "DOMAIN":
                try:
                    activate_object(scene, obj)
                    bpy.ops.fluid.free_all()
                    if verbose:
                        log(f"fluid.free_all → OK ({obj.name})")
                except Exception as e:
                    if verbose:
                        warn(f"fluid.free_all échoué ({obj.name}) : {e}")


# ─────────────────────────────────────────────
# Bake : Point Caches (Cloth, Particles, SoftBody, RigidBody, DynPaint)
# ─────────────────────────────────────────────

def bake_point_caches(
    scene: bpy.types.Scene,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Bake tous les point caches de la scène.

    Stratégie :
    1. Essayer ptcache.bake_all (rapide, une seule opération)
    2. Si échec → fallback objet par objet

    Retourne {"baked": int, "errors": list, "ok": bool}.
    """
    result: Dict[str, Any] = {"baked": 0, "errors": [], "ok": False, "method": "none"}

    if _interrupted:
        result["errors"].append("Interrompu avant bake ptcache")
        return result

    activate_scene(scene)

    # ── Tentative 1 : bake_all global ──
    try:
        bpy.ops.ptcache.bake_all(bake=True)
        result["baked"] = 1
        result["ok"] = True
        result["method"] = "ptcache.bake_all"
        if verbose:
            log("ptcache.bake_all(bake=True) → OK")
        return result
    except Exception as e:
        if verbose:
            warn(f"ptcache.bake_all échoué : {e}")
            warn("Fallback → bake individuel par objet")

    # ── Tentative 2 : bake individuel ──
    result["method"] = "individual"
    errors: List[str] = []
    baked = 0

    for obj in scene.objects:
        # Vérifier si l'objet a des physics
        has_physics = False
        for mod in obj.modifiers:
            if mod.type in ("CLOTH", "SOFT_BODY", "DYNAMIC_PAINT"):
                has_physics = True
                break
        if not has_physics and len(getattr(obj, "particle_systems", [])) == 0:
            continue

        if _interrupted:
            errors.append("Interrompu pendant bake individuel")
            break

        try:
            activate_object(scene, obj)
            bpy.ops.ptcache.bake_all(bake=True)
            baked += 1
            if verbose:
                log(f"  ptcache bake OK : {obj.name}")
        except Exception as e:
            error_msg = f"ptcache bake '{obj.name}' : {e}"
            errors.append(error_msg)
            if verbose:
                warn(f"  {error_msg}")
        finally:
            try:
                obj.select_set(False)
            except Exception:
                pass

    result["baked"] = baked
    result["errors"] = errors
    result["ok"] = baked > 0 or len(errors) == 0
    return result


# ─────────────────────────────────────────────
# Bake : Fluid Domains (Mantaflow)
# ─────────────────────────────────────────────

def bake_fluid_domains(
    scene: bpy.types.Scene,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Bake tous les fluid domains de la scène.

    Chaque domain doit être activé individuellement avant bake.

    Retourne {"domains_found": int, "baked": int, "errors": list, "ok": bool}.
    """
    result: Dict[str, Any] = {
        "domains_found": 0, "baked": 0,
        "errors": [], "ok": False,
    }

    activate_scene(scene)

    for obj in scene.objects:
        for mod in obj.modifiers:
            if mod.type != "FLUID":
                continue
            if getattr(mod, "fluid_type", None) != "DOMAIN":
                continue

            result["domains_found"] += 1

            if _interrupted:
                result["errors"].append("Interrompu pendant bake fluids")
                return result

            try:
                activate_object(scene, obj)
                bpy.ops.fluid.bake_all()
                result["baked"] += 1
                if verbose:
                    log(f"  fluid.bake_all → OK ({obj.name})")
            except Exception as e:
                error_msg = f"fluid bake '{obj.name}' : {e}"
                result["errors"].append(error_msg)
                warn(error_msg)
            finally:
                try:
                    obj.select_set(False)
                except Exception:
                    pass

    result["ok"] = (
        result["baked"] == result["domains_found"]
        or (result["domains_found"] == 0 and len(result["errors"]) == 0)
    )
    return result


# ─────────────────────────────────────────────
# Récupération de fichiers orphelins
# ─────────────────────────────────────────────

def collect_orphan_caches(cache_root: Path, verbose: bool = False) -> int:
    """Copie les fichiers ptcache orphelins si le symlink n'a pas fonctionné.

    Blender peut écrire dans blendcache_<stem>/ même sans symlink.
    On récupère ces fichiers vers cache_root/ptcache/.

    Retourne le nombre de fichiers récupérés.
    """
    blend_path = Path(bpy.data.filepath)
    if not blend_path.exists():
        return 0

    blendcache_dir = blend_path.parent / f"blendcache_{blend_path.stem}"
    target = cache_root / "ptcache"

    # Si c'est un symlink (fonctionnel), rien à faire
    if blendcache_dir.is_symlink():
        return 0

    if not blendcache_dir.is_dir():
        return 0

    count = 0
    for f in blendcache_dir.rglob("*"):
        if not f.is_file():
            continue
        try:
            dest = target / f.relative_to(blendcache_dir)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(f), str(dest))
            count += 1
        except Exception as e:
            if verbose:
                warn(f"Erreur copie orpheline {f} : {e}")

    if count > 0:
        log(f"{count} fichiers ptcache orphelins récupérés → {target}")
    return count


# ─────────────────────────────────────────────
# Manifest de cache
# ─────────────────────────────────────────────

def count_cache_files(cache_root: Path) -> Tuple[int, int]:
    """Compte les fichiers de cache et leur taille totale.

    Retourne (nb_fichiers, taille_totale_bytes).
    """
    file_count = 0
    total_bytes = 0
    for f in cache_root.rglob("*"):
        if f.is_file() and f.name != "cache_manifest.json":
            try:
                total_bytes += f.stat().st_size
                file_count += 1
            except OSError:
                pass
    return file_count, total_bytes


def write_manifest(
    cache_root: Path,
    scenes_info: List[Dict[str, Any]],
    status: str,
    frame_start: Optional[int] = None,
    frame_end: Optional[int] = None,
) -> Path:
    """Écrit cache_manifest.json au format attendu par le projet.

    Format :
    {
      "blender_version": "3.6.x",
      "scene": "Scene",
      "frame_range": [1, 250],
      "cache_dir": "/path/to/cache",
      "timestamp": "2025-...",
      "files": [{"path": "ptcache/file.bphys", "size": 12345, "timestamp": "..."}],
      "status": "complete"
    }
    """
    # Nom de scène principal
    if scenes_info:
        scene_name = scenes_info[0].get("scene", "unknown")
        if len(scenes_info) > 1:
            scene_name = ", ".join(s.get("scene", "?") for s in scenes_info)
    else:
        scene_name = "unknown"

    # Frame range
    if frame_start is None or frame_end is None:
        # Déduire des scènes traitées
        starts = [s.get("frame_start", 1) for s in scenes_info if "frame_start" in s]
        ends = [s.get("frame_end", 250) for s in scenes_info if "frame_end" in s]
        if starts:
            frame_start = min(starts)
        else:
            frame_start = 1
        if ends:
            frame_end = max(ends)
        else:
            frame_end = 250

    # Collecte des fichiers
    files: List[Dict[str, Any]] = []
    for f in cache_root.rglob("*"):
        if f.is_file() and f.name != "cache_manifest.json":
            try:
                stat = f.stat()
                files.append({
                    "path": str(f.relative_to(cache_root)),
                    "size": stat.st_size,
                    "timestamp": datetime.datetime.fromtimestamp(
                        stat.st_mtime, tz=datetime.timezone.utc
                    ).isoformat(),
                })
            except OSError:
                pass

    manifest = {
        "blender_version": bpy.app.version_string,
        "scene": scene_name,
        "frame_range": [frame_start, frame_end],
        "cache_dir": str(cache_root),
        "timestamp": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
        "files": files,
        "status": status,
        "scenes_detail": scenes_info,
    }

    manifest_path = cache_root / "cache_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest_path


# ─────────────────────────────────────────────
# Point d'entrée principal
# ─────────────────────────────────────────────

def main() -> int:
    """Fonction principale. Retourne le code de sortie."""
    # ── Signaux ──
    install_signal_handlers()

    # ── Arguments CLI ──
    args = parse_args()
    cache_root = Path(args.cache_dir).expanduser().resolve()
    cache_root.mkdir(parents=True, exist_ok=True)

    # ── Bannière ──
    log("=" * 70)
    log("Démarrage bake_all.py")
    log(f"  Blender       : {bpy.app.version_string}")
    log(f"  Fichier .blend: {bpy.data.filepath or '(aucun)'}")
    log(f"  Cache dir     : {cache_root}")
    log(f"  Bake fluids   : {args.bake_fluids}")
    log(f"  Bake particles: {args.bake_particles}")
    log(f"  Bake cloth    : {args.bake_cloth}")
    log(f"  Clear existing: {args.clear_existing}")
    log(f"  Strict        : {args.strict}")
    log(f"  All scenes    : {args.all_scenes}")
    log("=" * 70)

    start_time = time.time()

    # ── Vérification .blend ──
    if not verify_blend_loaded():
        write_manifest(cache_root, [], "error")
        return 1

    # ── Arborescence cache ──
    cache_dirs = setup_cache_directories(cache_root)
    log(f"Sous-dossiers créés : {', '.join(CACHE_SUBDIRS)}")

    # ── Symlink ptcache ──
    symlink_ok = setup_ptcache_symlink(cache_root, verbose=args.verbose)
    if not symlink_ok:
        warn("Symlink ptcache non disponible — les fichiers seront récupérés après bake")

    # ── Choix des scènes ──
    if args.all_scenes:
        scenes = list(bpy.data.scenes)
    else:
        scenes = [bpy.context.scene]

    log(f"Scènes à baker : {', '.join(s.name for s in scenes)}")

    # ── Optimisation performances (une seule fois) ──
    perf_report = optimize_performance(scenes[0])
    if args.verbose:
        log(f"Performance : {perf_report}")

    # ── Boucle principale par scène ──
    scenes_info: List[Dict[str, Any]] = []
    all_errors: List[str] = []
    total_ptcache_baked = 0
    total_fluid_baked = 0

    for scene in scenes:
        if _interrupted:
            warn("Interruption détectée, arrêt entre les scènes")
            break

        scene_result: Dict[str, Any] = {
            "scene": scene.name,
            "ok": True,
            "errors": [],
            "ptcache": {},
            "fluids": {},
        }

        try:
            # Activer la scène
            activate_scene(scene)

            # Override frame range
            if args.frame_start is not None:
                scene.frame_start = args.frame_start
            if args.frame_end is not None:
                scene.frame_end = args.frame_end

            scene_result["frame_start"] = scene.frame_start
            scene_result["frame_end"] = scene.frame_end
            log(f"[{scene.name}] Frames : {scene.frame_start} → {scene.frame_end}")

            # Configurer disk caches (ptcache)
            n_caches = configure_disk_caches(scene, verbose=args.verbose)
            scene_result["disk_caches_configured"] = n_caches
            log(f"[{scene.name}] {n_caches} point caches configurés pour le disque")

            # Configurer fluid domains
            if args.bake_fluids:
                n_fluids = configure_fluid_domains(
                    scene, cache_dirs["fluids"], verbose=args.verbose,
                )
                scene_result["fluid_domains_configured"] = n_fluids
                if n_fluids > 0:
                    log(f"[{scene.name}] {n_fluids} fluid domains → {cache_dirs['fluids']}")

            # Clear caches si demandé
            if args.clear_existing:
                log(f"[{scene.name}] Nettoyage des caches existants…")
                clear_all_caches(scene, verbose=args.verbose)

            # ── Bake point caches (cloth, particles, softbody, rigidbody) ──
            if args.bake_cloth or args.bake_particles:
                log(f"[{scene.name}] Bake point caches…")
                ptcache_result = bake_point_caches(scene, verbose=args.verbose)
                scene_result["ptcache"] = ptcache_result
                total_ptcache_baked += ptcache_result.get("baked", 0)

                if ptcache_result.get("ok"):
                    log(f"[{scene.name}] Point caches OK "
                        f"(méthode={ptcache_result.get('method')}, "
                        f"baked={ptcache_result.get('baked')})")
                else:
                    for e in ptcache_result.get("errors", []):
                        scene_result["errors"].append(e)
                    warn(f"[{scene.name}] Point caches : erreurs détectées")
            else:
                log(f"[{scene.name}] Bake cloth/particles désactivé")

            # ── Bake fluids ──
            if _interrupted:
                scene_result["errors"].append("Interrompu avant bake fluids")
            elif args.bake_fluids:
                log(f"[{scene.name}] Bake fluid domains…")
                fluid_result = bake_fluid_domains(scene, verbose=args.verbose)
                scene_result["fluids"] = fluid_result
                total_fluid_baked += fluid_result.get("baked", 0)

                if fluid_result.get("domains_found", 0) > 0:
                    if fluid_result.get("ok"):
                        log(f"[{scene.name}] Fluids OK "
                            f"(baked={fluid_result.get('baked')}/"
                            f"{fluid_result.get('domains_found')})")
                    else:
                        for e in fluid_result.get("errors", []):
                            scene_result["errors"].append(e)
                        warn(f"[{scene.name}] Fluids : erreurs détectées")
                else:
                    if args.verbose:
                        log(f"[{scene.name}] Aucun fluid domain trouvé")
            else:
                log(f"[{scene.name}] Bake fluids désactivé")

            # ── Récupération fichiers orphelins ──
            if not symlink_ok:
                n_orphans = collect_orphan_caches(cache_root, verbose=args.verbose)
                if n_orphans > 0:
                    scene_result["orphans_recovered"] = n_orphans

        except Exception as e:
            scene_result["ok"] = False
            scene_result["errors"].append(str(e))
            err(f"[{scene.name}] Erreur critique : {e}")
            if args.verbose:
                traceback.print_exc()

        # Évaluation scène
        if scene_result["errors"]:
            scene_result["ok"] = False
            all_errors.extend(scene_result["errors"])

        scenes_info.append(scene_result)

        # Mode strict : arrêt au premier échec
        if args.strict and not scene_result["ok"]:
            err(f"[{scene.name}] Mode strict → arrêt immédiat")
            break

    # ─────────────────────────────────────────
    # Post-bake
    # ─────────────────────────────────────────

    duration = round(time.time() - start_time, 2)
    file_count, total_bytes = count_cache_files(cache_root)

    # Déterminer le statut final
    if _interrupted:
        status = "interrupted"
    elif not all_errors:
        status = "complete"
    else:
        # Vérifier si au moins un bake a réussi
        any_success = any(s.get("ok") for s in scenes_info)
        status = "partial" if any_success else "error"

    # Écriture du manifest (TOUJOURS)
    manifest_path = write_manifest(
        cache_root, scenes_info, status,
        frame_start=args.frame_start,
        frame_end=args.frame_end,
    )
    log(f"Manifest écrit : {manifest_path}")

    # ── Résumé final ──
    log("=" * 70)
    log(f"RÉSUMÉ — statut: {status.upper()}")
    log(f"  Durée           : {duration}s")
    log(f"  Scènes traitées : {len(scenes_info)}")
    log(f"  Point caches    : {total_ptcache_baked} baked")
    log(f"  Fluid domains   : {total_fluid_baked} baked")
    log(f"  Fichiers cache  : {file_count} ({total_bytes} bytes)")
    log(f"  Erreurs         : {len(all_errors)}")
    if all_errors:
        for i, e in enumerate(all_errors[:10], 1):
            err(f"  [{i}] {e}")
        if len(all_errors) > 10:
            err(f"  … et {len(all_errors) - 10} autres erreurs")
    log("=" * 70)

    # ── Codes de sortie ──
    if _interrupted:
        log("Arrêt suite à interruption")
        return 1

    if not all_errors:
        log("BAKE COMPLET — succès")
        return 0

    if args.strict:
        err("BAKE ÉCHOUÉ — mode strict")
        return 1

    warn("BAKE PARTIEL — certaines simulations en erreur")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())