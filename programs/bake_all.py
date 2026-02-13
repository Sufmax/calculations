#!/usr/bin/env python3
"""
bake_all.py — Script Blender 4.2 LTS (exécution en mode background)

Objectif :
- Bake natif des Simulation Nodes (GeoNodes) — multi-threadé
- Bake classique ptcache (particules, cloth, rigid body)
- Bake Mantaflow (fluides/fumée)
- Export Alembic chunked optionnel pour transfert vers machine de rendu
- Tout dans un répertoire unique (--cache-dir)

Interface :
  blender --background fichier.blend --python bake_all.py -- \
    --cache-dir /path/to/cache [options]
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import shutil
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import bpy

# ═══════════════════════════════════════════
# Constantes
# ═══════════════════════════════════════════

CACHE_SUBDIRS = ("ptcache", "fluids", "rigidbody", "alembic", "geonodes")
RESERVE_THREADS = 2

CACHE_EXTENSIONS = {
    '.bphys', '.vdb', '.uni', '.gz',
    '.png', '.exr', '.abc', '.obj', '.ply',
}

ALEMBIC_CHUNK_FRAMES = int(os.environ.get('ALEMBIC_CHUNK_FRAMES', '10'))

# ═══════════════════════════════════════════
# État global interruption
# ═══════════════════════════════════════════

_interrupted = False
_interrupt_count = 0


# ═══════════════════════════════════════════
# Logging (stdout — lu par blender_runner.py)
# ═══════════════════════════════════════════

def log(msg: str) -> None:
    print(f"[BAKE_ALL] {msg}", flush=True)


def warn(msg: str) -> None:
    print(f"[BAKE_ALL][WARN] {msg}", flush=True)


def err(msg: str) -> None:
    print(f"[BAKE_ALL][ERROR] {msg}", flush=True)


# ═══════════════════════════════════════════
# Signal handlers
# ═══════════════════════════════════════════

def _signal_handler(signum: int, frame: Any) -> None:
    global _interrupted, _interrupt_count
    _interrupted = True
    _interrupt_count += 1
    sig_name = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
    warn(f"Signal {sig_name} reçu (#{_interrupt_count})")
    if _interrupt_count >= 3:
        err("3 interruptions → arrêt immédiat")
        sys.exit(1)


def install_signal_handlers() -> None:
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _signal_handler)
        except (OSError, ValueError):
            pass


# ═══════════════════════════════════════════
# Parsing arguments
# ═══════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    argv = sys.argv
    if "--" in argv:
        argv = argv[argv.index("--") + 1:]
    else:
        argv = []

    parser = argparse.ArgumentParser(description="Blender 4.2 bake-all helper")

    parser.add_argument("--cache-dir", required=True,
                        help="Répertoire racine des caches")

    parser.add_argument("--frame-start", "--start-frame",
                        dest="frame_start", type=int, default=None)
    parser.add_argument("--frame-end", "--end-frame",
                        dest="frame_end", type=int, default=None)

    parser.add_argument("--clear-existing", action="store_true",
                        help="Supprime les caches avant bake")

    # Types de bake
    parser.add_argument("--bake-fluids", action="store_true", default=True)
    parser.add_argument("--no-bake-fluids", dest="bake_fluids", action="store_false")

    parser.add_argument("--bake-particles", action="store_true", default=True)
    parser.add_argument("--no-bake-particles", dest="bake_particles", action="store_false")

    parser.add_argument("--bake-cloth", action="store_true", default=True)
    parser.add_argument("--no-bake-cloth", dest="bake_cloth", action="store_false")

    parser.add_argument("--bake-geonodes", action="store_true", default=True,
                        help="Bake natif des Simulation Nodes (GeoNodes)")
    parser.add_argument("--no-bake-geonodes", dest="bake_geonodes", action="store_false")

    # Export Alembic (optionnel, pour transfert vers machine de rendu)
    parser.add_argument("--export-alembic", action="store_true", default=False,
                        help="Exporter aussi en Alembic (.abc) pour transfert")
    parser.add_argument("--alembic-objects", type=str, default=None,
                        help="Objets à exporter en Alembic (séparés par virgule)")
    parser.add_argument("--alembic-chunk", type=int, default=ALEMBIC_CHUNK_FRAMES,
                        help=f"Frames par chunk Alembic (défaut: {ALEMBIC_CHUNK_FRAMES})")

    parser.add_argument("--bake-threads", type=int, default=None)
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--all-scenes", action="store_true")
    parser.add_argument("--verbose", action="store_true")

    return parser.parse_args(argv)


# ═══════════════════════════════════════════
# Vérifications
# ═══════════════════════════════════════════

def verify_blend_loaded() -> bool:
    if not bpy.data.filepath:
        err("Aucun fichier .blend chargé")
        return False
    log(f"Fichier .blend chargé : {bpy.data.filepath}")
    return True


def verify_blender_version() -> bool:
    """Vérifie que Blender est en version 4.2+."""
    major, minor = bpy.app.version[:2]
    if major < 4 or (major == 4 and minor < 2):
        err(f"Blender {bpy.app.version_string} détecté — version 4.2+ requise")
        return False
    log(f"Blender {bpy.app.version_string} — OK")
    return True


# ═══════════════════════════════════════════
# Création répertoires de cache
# ═══════════════════════════════════════════

def setup_cache_directories(cache_root: Path) -> Dict[str, Path]:
    dirs: Dict[str, Path] = {}
    for name in CACHE_SUBDIRS:
        d = cache_root / name
        d.mkdir(parents=True, exist_ok=True)
        dirs[name] = d
    return dirs


# ═══════════════════════════════════════════
# Symlink ptcache
# ═══════════════════════════════════════════

def setup_ptcache_symlink(cache_root: Path) -> bool:
    blend_path = Path(bpy.data.filepath)
    if not blend_path.exists():
        return False

    blendcache_dir = blend_path.parent / f"blendcache_{blend_path.stem}"
    target = cache_root / "ptcache"

    if blendcache_dir.is_symlink():
        try:
            if blendcache_dir.resolve() == target.resolve():
                return True
        except OSError:
            pass
        try:
            blendcache_dir.unlink()
        except OSError:
            return False

    if blendcache_dir.is_dir() and not blendcache_dir.is_symlink():
        try:
            shutil.rmtree(str(blendcache_dir), ignore_errors=True)
        except OSError:
            return False

    if blendcache_dir.exists() and not blendcache_dir.is_dir():
        try:
            blendcache_dir.unlink()
        except OSError:
            return False

    try:
        blendcache_dir.symlink_to(target, target_is_directory=True)
        log(f"Symlink créé : {blendcache_dir} → {target}")
        return True
    except OSError as e:
        warn(f"Échec symlink : {e}")
        return False


# ═══════════════════════════════════════════
# Configuration threading
# ═══════════════════════════════════════════

def configure_threading(scene: bpy.types.Scene, n_threads: int) -> None:
    os.environ["OMP_NUM_THREADS"] = str(n_threads)
    os.environ.pop("OMP_PROC_BIND", None)
    try:
        scene.render.threads_mode = "FIXED"
        scene.render.threads = n_threads
    except Exception:
        pass
    log(f"Threading configuré : {n_threads} threads, mode=FIXED")


# ═══════════════════════════════════════════
# Configuration des caches — Fluid Domains
# ═══════════════════════════════════════════

def configure_fluid_domains(scene: bpy.types.Scene, fluids_dir: Path) -> int:
    count = 0
    for obj in scene.objects:
        for mod in obj.modifiers:
            if mod.type != "FLUID":
                continue
            if getattr(mod, "fluid_type", None) != "DOMAIN":
                continue
            ds = getattr(mod, "domain_settings", None)
            if ds is None:
                continue
            try:
                ds.cache_directory = str(fluids_dir)
                if hasattr(ds, "cache_data_format"):
                    ds.cache_data_format = "OPENVDB"
                if hasattr(ds, "openvdb_cache_compress_type"):
                    ds.openvdb_cache_compress_type = "BLOSC"
                count += 1
                log(f"  Fluid domain '{obj.name}' → {fluids_dir}")
            except Exception as e:
                warn(f"Erreur config fluid '{obj.name}' : {e}")
    return count


# ═══════════════════════════════════════════
# Configuration des caches — Point Caches
# ═══════════════════════════════════════════

def _configure_single_point_cache(pc: Any) -> bool:
    try:
        if hasattr(pc, "use_disk_cache"):
            pc.use_disk_cache = True
        if hasattr(pc, "use_external"):
            pc.use_external = False
        if hasattr(pc, "use_library_path"):
            pc.use_library_path = False
        return True
    except Exception as e:
        warn(f"Erreur configuration point_cache : {e}")
        return False


def configure_disk_caches(scene: bpy.types.Scene) -> int:
    count = 0
    rbw = getattr(scene, "rigidbody_world", None)
    if rbw and rbw.point_cache:
        if _configure_single_point_cache(rbw.point_cache):
            count += 1
    for obj in scene.objects:
        for psys in getattr(obj, "particle_systems", []):
            if psys.point_cache:
                if _configure_single_point_cache(psys.point_cache):
                    count += 1
        for mod in obj.modifiers:
            if mod.type in ("CLOTH", "SOFT_BODY"):
                if mod.point_cache:
                    if _configure_single_point_cache(mod.point_cache):
                        count += 1
            elif mod.type == "DYNAMIC_PAINT":
                canvas = getattr(mod, "canvas_settings", None)
                if canvas and hasattr(canvas, "canvas_surfaces"):
                    for surf in canvas.canvas_surfaces:
                        if surf.point_cache:
                            if _configure_single_point_cache(surf.point_cache):
                                count += 1
            elif mod.type not in ("FLUID", "NODES") and hasattr(mod, "point_cache") and mod.point_cache:
                if _configure_single_point_cache(mod.point_cache):
                    count += 1
    log(f"  {count} caches disque configurés")
    return count


# ═══════════════════════════════════════════
# Clear caches existants
# ═══════════════════════════════════════════

def clear_all_caches(scene: bpy.types.Scene) -> None:
    try:
        bpy.ops.ptcache.free_bake_all()
        log("  ptcache.free_bake_all() → OK")
    except Exception as e:
        warn(f"  ptcache.free_bake_all() échoué : {e}")

    for obj in scene.objects:
        for mod in obj.modifiers:
            if mod.type == "FLUID" and getattr(mod, "fluid_type", None) == "DOMAIN":
                try:
                    bpy.context.view_layer.objects.active = obj
                    bpy.ops.fluid.free_all()
                    log(f"  fluid.free_all() → OK ({obj.name})")
                except Exception as e:
                    warn(f"  fluid.free_all() échoué ({obj.name}) : {e}")

    # Clear les caches Simulation Nodes (Blender 4.2)
    for obj in scene.objects:
        for mod in obj.modifiers:
            if mod.type == 'NODES' and mod.node_group:
                try:
                    bpy.context.view_layer.objects.active = obj
                    obj.select_set(True)
                    bpy.ops.object.simulation_nodes_cache_delete('INVOKE_DEFAULT')
                    log(f"  GeoNodes cache supprimé : {obj.name}")
                except Exception:
                    pass


# ═══════════════════════════════════════════
# Context helper
# ═══════════════════════════════════════════

def _ensure_context(scene: bpy.types.Scene, obj: bpy.types.Object) -> bool:
    try:
        bpy.context.window.scene = scene
        bpy.context.view_layer.objects.active = obj
        obj.select_set(True)
        return True
    except Exception as e:
        warn(f"Impossible de configurer le contexte pour '{obj.name}' : {e}")
        return False


# ═══════════════════════════════════════════
# Bake — Point Caches
# ═══════════════════════════════════════════

def bake_point_caches(scene: bpy.types.Scene) -> Tuple[int, int]:
    if _interrupted:
        return 0, 0
    successes = 0
    failures = 0
    try:
        log("  ptcache.bake_all(bake=True)...")
        bpy.ops.ptcache.bake_all(bake=True)
        for obj in scene.objects:
            for psys in getattr(obj, "particle_systems", []):
                if psys.point_cache and psys.point_cache.is_baked:
                    successes += 1
            for mod in obj.modifiers:
                if mod.type not in ("FLUID", "NODES") and hasattr(mod, "point_cache") and mod.point_cache:
                    if mod.point_cache.is_baked:
                        successes += 1
        rbw = getattr(scene, "rigidbody_world", None)
        if rbw and rbw.point_cache and rbw.point_cache.is_baked:
            successes += 1
        log(f"  ptcache.bake_all → {successes} caches baked")
    except Exception as e:
        warn(f"  ptcache.bake_all échoué : {e}")
        failures += 1
    return successes, failures


# ═══════════════════════════════════════════
# Bake — Fluid Domains (Mantaflow)
# ═══════════════════════════════════════════

def bake_fluid_domains(scene: bpy.types.Scene) -> Tuple[int, int]:
    successes = 0
    failures = 0
    for obj in scene.objects:
        for mod in obj.modifiers:
            if mod.type != "FLUID" or getattr(mod, "fluid_type", None) != "DOMAIN":
                continue
            if _interrupted:
                return successes, failures
            try:
                if _ensure_context(scene, obj):
                    bpy.ops.fluid.bake_all()
                    successes += 1
                    log(f"  Fluid domain '{obj.name}' → baked")
            except Exception as e:
                failures += 1
                warn(f"  Fluid domain '{obj.name}' → échec : {e}")
    return successes, failures


# ═══════════════════════════════════════════
# Bake — Simulation Nodes (GeoNodes, Blender 4.2)
# ═══════════════════════════════════════════

def find_simulation_nodes_objects(scene: bpy.types.Scene) -> List[bpy.types.Object]:
    """
    Trouve tous les objets avec des modifiers Geometry Nodes
    contenant des Simulation Zones (nœuds Simulation Input/Output).
    """
    result = []
    for obj in scene.objects:
        for mod in obj.modifiers:
            if mod.type != 'NODES' or not mod.node_group:
                continue
            # Chercher des nœuds de type Simulation dans le node tree
            has_sim = False
            for node in mod.node_group.nodes:
                # En 4.2, les simulation zones utilisent GeometryNodeSimulationInput/Output
                if node.type in ('SIMULATION_INPUT', 'SIMULATION_OUTPUT'):
                    has_sim = True
                    break
                # Fallback : chercher par bl_idname
                if hasattr(node, 'bl_idname'):
                    if 'Simulation' in node.bl_idname:
                        has_sim = True
                        break
            if has_sim:
                result.append(obj)
                break
    return result


def find_geonodes_objects(scene: bpy.types.Scene) -> List[bpy.types.Object]:
    """Trouve tous les objets avec au moins un modifier Geometry Nodes."""
    result = []
    for obj in scene.objects:
        for mod in obj.modifiers:
            if mod.type == 'NODES' and mod.node_group:
                result.append(obj)
                break
    return result


def configure_geonodes_cache(obj: bpy.types.Object, geonodes_dir: Path) -> int:
    """
    Configure le répertoire de cache pour les Simulation Nodes d'un objet.
    Blender 4.2 : chaque modifier GeoNodes avec simulation a son propre cache.
    """
    count = 0
    for mod in obj.modifiers:
        if mod.type != 'NODES' or not mod.node_group:
            continue
        # En Blender 4.2, le cache des simulation nodes est dans
        # bpy.types.NodesModifier.simulation_bake_directory
        if hasattr(mod, 'simulation_bake_directory'):
            mod.simulation_bake_directory = str(geonodes_dir)
            count += 1
        # Alternative : via bake_directory sur le modifier
        elif hasattr(mod, 'bake_directory'):
            mod.bake_directory = str(geonodes_dir)
            count += 1
    return count


def bake_simulation_nodes(scene: bpy.types.Scene, geonodes_dir: Path) -> Tuple[int, int]:
    """
    Bake natif des Simulation Nodes (Blender 4.2).
    Utilise bpy.ops.object.simulation_nodes_cache_bake qui est
    multi-threadé et exploite tous les CPU disponibles.
    """
    if _interrupted:
        return 0, 0

    successes = 0
    failures = 0

    # Trouver les objets avec Simulation Nodes
    sim_objects = find_simulation_nodes_objects(scene)
    if not sim_objects:
        log("  Aucun objet avec Simulation Nodes trouvé")
        return 0, 0

    log(f"  {len(sim_objects)} objet(s) avec Simulation Nodes :")
    for obj in sim_objects:
        gn_mods = [m.name for m in obj.modifiers if m.type == 'NODES']
        log(f"    - {obj.name} ({', '.join(gn_mods)})")

    for obj in sim_objects:
        if _interrupted:
            return successes, failures

        log(f"  Bake Simulation Nodes '{obj.name}'...")

        # Configurer le répertoire de cache
        n_configured = configure_geonodes_cache(obj, geonodes_dir)
        if n_configured > 0:
            log(f"    Cache dir → {geonodes_dir}")

        try:
            if not _ensure_context(scene, obj):
                failures += 1
                continue

            # Désélectionner tout, sélectionner uniquement cet objet
            bpy.ops.object.select_all(action='DESELECT')
            obj.select_set(True)
            bpy.context.view_layer.objects.active = obj

            # Bake natif — multi-threadé dans Blender 4.2
            # Cette opération utilise le scheduler parallèle de GeoNodes
            bpy.ops.object.simulation_nodes_cache_bake(selected=True)

            successes += 1
            log(f"    ✓ '{obj.name}' → baked")

            # Lister les fichiers de cache générés
            cache_count = 0
            for f in geonodes_dir.rglob("*"):
                if f.is_file() and f.suffix.lower() in CACHE_EXTENSIONS:
                    cache_count += 1
            if cache_count > 0:
                log(f"    {cache_count} fichiers de cache générés")

        except Exception as e:
            failures += 1
            warn(f"    ✗ '{obj.name}' → échec : {e}")

    return successes, failures


# ═══════════════════════════════════════════
# Export Alembic — optionnel, pour transfert vers rendu
# ═══════════════════════════════════════════

def _safe_alembic_export(filepath: str, start: int, end: int, selected: bool) -> None:
    """Appelle alembic_export avec paramètres compatibles Blender 4.2."""
    bpy.ops.wm.alembic_export(
        filepath=filepath,
        start=start,
        end=end,
        selected=selected,
        visible_objects_only=False,
        flatten=False,
        uvs=True,
        packuv=True,
        face_sets=False,
        subdiv_schema=False,
        global_scale=1.0,
        triangulate=False,
        export_hair=False,
        export_particles=False,
        export_custom_properties=True,
    )


def export_alembic_chunked(
    scene: bpy.types.Scene,
    objects: List[bpy.types.Object],
    alembic_dir: Path,
    frame_start: int,
    frame_end: int,
    chunk_size: int,
) -> Tuple[int, int]:
    """Exporte chaque objet en chunks Alembic de N frames."""
    successes = 0
    failures = 0
    total_frames = frame_end - frame_start + 1

    for obj in objects:
        if _interrupted:
            return successes, failures

        obj_safe_name = obj.name.replace(" ", "_").replace("/", "_")
        log(f"  Export Alembic '{obj.name}' par chunks de {chunk_size} frames")

        bpy.ops.object.select_all(action='DESELECT')
        obj.select_set(True)
        bpy.context.view_layer.objects.active = obj

        chunk_start = frame_start
        chunk_index = 0

        while chunk_start <= frame_end:
            if _interrupted:
                return successes, failures

            chunk_end = min(chunk_start + chunk_size - 1, frame_end)
            abc_name = f"{obj_safe_name}_{chunk_start:04d}-{chunk_end:04d}.abc"
            abc_path = str(alembic_dir / abc_name)

            log(f"    chunk {chunk_index + 1}: frames {chunk_start}→{chunk_end}")
            print(f"bake: frame {chunk_start} :: {total_frames}", flush=True)

            try:
                _safe_alembic_export(
                    filepath=abc_path,
                    start=chunk_start,
                    end=chunk_end,
                    selected=True,
                )

                abc_file = Path(abc_path)
                if abc_file.exists() and abc_file.stat().st_size > 0:
                    size_mb = abc_file.stat().st_size / (1024 * 1024)
                    successes += 1
                    log(f"    ✓ {abc_name} ({size_mb:.1f} MB)")
                else:
                    failures += 1
                    warn(f"    ✗ Fichier non créé : {abc_name}")

            except Exception as e:
                failures += 1
                warn(f"    ✗ Échec chunk {chunk_start}-{chunk_end} : {e}")

            chunk_start = chunk_end + 1
            chunk_index += 1

    return successes, failures


def export_alembic_all(
    scene: bpy.types.Scene,
    alembic_dir: Path,
    frame_start: int,
    frame_end: int,
    chunk_size: int,
    specific_objects: Optional[List[str]] = None,
) -> Tuple[int, int]:
    """Point d'entrée pour l'export Alembic."""
    if specific_objects:
        objects = []
        for name in specific_objects:
            obj = scene.objects.get(name.strip())
            if obj:
                objects.append(obj)
            else:
                warn(f"  Objet '{name}' introuvable dans la scène")
    else:
        objects = find_geonodes_objects(scene)

    if not objects:
        log("  Aucun objet avec Geometry Nodes trouvé pour export Alembic")
        return 0, 0

    log(f"  {len(objects)} objet(s) à exporter en Alembic")
    return export_alembic_chunked(
        scene, objects, alembic_dir,
        frame_start, frame_end, chunk_size,
    )


# ═══════════════════════════════════════════
# Manifest de cache
# ═══════════════════════════════════════════

def collect_cache_files(cache_root: Path) -> List[Dict[str, Any]]:
    files = []
    for f in sorted(cache_root.rglob("*")):
        if not f.is_file():
            continue
        if f.suffix.lower() not in CACHE_EXTENSIONS:
            continue
        if f.name == "cache_manifest.json":
            continue
        try:
            stat = f.stat()
            files.append({
                "path": str(f.relative_to(cache_root)),
                "size": stat.st_size,
                "timestamp": datetime.datetime.fromtimestamp(stat.st_mtime).isoformat(),
            })
        except OSError:
            pass
    return files


def write_manifest(
    cache_root: Path,
    scene_name: str,
    frame_start: int,
    frame_end: int,
    status: str,
    errors: List[str],
    duration: float,
    bake_stats: Dict[str, int],
) -> None:
    cache_files = collect_cache_files(cache_root)
    total_size = sum(f["size"] for f in cache_files)
    manifest = {
        "blender_version": bpy.app.version_string,
        "scene": scene_name,
        "frame_range": [frame_start, frame_end],
        "cache_dir": str(cache_root),
        "timestamp": datetime.datetime.now().isoformat(),
        "duration_seconds": round(duration, 2),
        "status": status,
        "bake_stats": bake_stats,
        "errors": errors,
        "total_cache_size": total_size,
        "file_count": len(cache_files),
        "files": cache_files,
    }
    manifest_path = cache_root / "cache_manifest.json"
    try:
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        log(f"Manifest écrit : {manifest_path} ({len(cache_files)} fichiers, {total_size} octets)")
    except Exception as e:
        warn(f"Impossible d'écrire le manifest : {e}")


# ═══════════════════════════════════════════
# Point d'entrée principal
# ═══════════════════════════════════════════

def main() -> int:
    install_signal_handlers()
    args = parse_args()
    start_time = time.time()

    cache_root = Path(args.cache_dir).expanduser().resolve()
    cache_root.mkdir(parents=True, exist_ok=True)

    cpu_count = os.cpu_count() or 1
    n_threads = args.bake_threads if args.bake_threads else max(1, cpu_count - RESERVE_THREADS)

    log("=" * 70)
    log("Démarrage bake_all.py (Blender 4.2 LTS)")
    log(f"  Fichier .blend  : {bpy.data.filepath}")
    log(f"  Cache dir       : {cache_root}")
    log(f"  CPU             : {cpu_count} threads")
    log(f"  Bake threads    : {n_threads}")
    log(f"  Frame start     : {args.frame_start}")
    log(f"  Frame end       : {args.frame_end}")
    log(f"  Bake GeoNodes   : {args.bake_geonodes}")
    log(f"  Export Alembic  : {args.export_alembic}")
    log("=" * 70)

    if not verify_blend_loaded():
        return 1

    if not verify_blender_version():
        return 1

    cache_dirs = setup_cache_directories(cache_root)
    setup_ptcache_symlink(cache_root)

    scenes = list(bpy.data.scenes) if args.all_scenes else [bpy.context.scene]

    all_errors: List[str] = []
    total_successes = 0
    total_failures = 0
    last_scene_name = ""
    frame_start = 1
    frame_end = 250

    for scene in scenes:
        if _interrupted:
            break

        last_scene_name = scene.name
        log(f"─── Scène : {scene.name} ───")

        try:
            configure_threading(scene, n_threads)

            if args.frame_start is not None:
                scene.frame_start = args.frame_start
            if args.frame_end is not None:
                scene.frame_end = args.frame_end
            frame_start = scene.frame_start
            frame_end = scene.frame_end
            log(f"  Frame range : {frame_start} → {frame_end}")

            configure_disk_caches(scene)

            if args.bake_fluids:
                n_fluids = configure_fluid_domains(scene, cache_dirs["fluids"])
                log(f"  {n_fluids} fluid domain(s) configuré(s)")

            if args.clear_existing:
                warn("clear-existing activé : suppression des caches existants")
                clear_all_caches(scene)

            # ── 1. Bake Point Caches (particules, cloth, rigid body) ──
            if args.bake_cloth or args.bake_particles:
                log(f"[{scene.name}] Bake point caches…")
                pc_ok, pc_fail = bake_point_caches(scene)
                total_successes += pc_ok
                total_failures += pc_fail
                if pc_fail > 0:
                    all_errors.append(f"[{scene.name}] {pc_fail} point cache(s) échoué(s)")

            # ── 2. Bake Fluids (Mantaflow) ──
            if args.bake_fluids and not _interrupted:
                log(f"[{scene.name}] Bake fluid domains…")
                fl_ok, fl_fail = bake_fluid_domains(scene)
                total_successes += fl_ok
                total_failures += fl_fail
                if fl_fail > 0:
                    all_errors.append(f"[{scene.name}] {fl_fail} fluid domain(s) échoué(s)")

            # ── 3. Bake Simulation Nodes (GeoNodes natif, multi-threadé) ──
            if args.bake_geonodes and not _interrupted:
                log(f"[{scene.name}] Bake Simulation Nodes (GeoNodes)…")
                gn_ok, gn_fail = bake_simulation_nodes(scene, cache_dirs["geonodes"])
                total_successes += gn_ok
                total_failures += gn_fail
                if gn_fail > 0:
                    all_errors.append(f"[{scene.name}] {gn_fail} bake(s) GeoNodes échoué(s)")

            # ── 4. Export Alembic (optionnel, pour transfert vers rendu) ──
            if args.export_alembic and not _interrupted:
                log(f"[{scene.name}] Export Alembic…")
                specific = None
                if args.alembic_objects:
                    specific = [s.strip() for s in args.alembic_objects.split(",")]
                abc_ok, abc_fail = export_alembic_all(
                    scene=scene,
                    alembic_dir=cache_dirs["alembic"],
                    frame_start=frame_start,
                    frame_end=frame_end,
                    chunk_size=args.alembic_chunk,
                    specific_objects=specific,
                )
                total_successes += abc_ok
                total_failures += abc_fail
                if abc_fail > 0:
                    all_errors.append(f"[{scene.name}] {abc_fail} export(s) Alembic échoué(s)")

        except Exception as e:
            error_msg = f"[{scene.name}] Erreur inattendue : {e}"
            err(error_msg)
            all_errors.append(error_msg)
            total_failures += 1

    # ── Statut final ──
    duration = time.time() - start_time

    if _interrupted:
        final_status = "interrupted"
    elif total_failures > 0 and total_successes > 0:
        final_status = "partial"
    elif total_failures > 0 and total_successes == 0:
        final_status = "failed"
    else:
        final_status = "complete"

    bake_stats = {
        "successes": total_successes,
        "failures": total_failures,
        "total": total_successes + total_failures,
    }

    write_manifest(
        cache_root=cache_root,
        scene_name=last_scene_name or "unknown",
        frame_start=frame_start,
        frame_end=frame_end,
        status=final_status,
        errors=all_errors,
        duration=duration,
        bake_stats=bake_stats,
    )

    cache_files = collect_cache_files(cache_root)
    total_size = sum(f["size"] for f in cache_files)

    log("=" * 70)
    log(f"RÉSUMÉ — statut: {final_status.upper()}")
    log(f"  Durée          : {duration:.1f}s")
    log(f"  Bakes réussis  : {total_successes}")
    log(f"  Bakes échoués  : {total_failures}")
    log(f"  Fichiers cache : {len(cache_files)}")
    log(f"  Taille totale  : {total_size} octets")
    if all_errors:
        log("  Erreurs :")
        for e in all_errors:
            log(f"    - {e}")
    log("=" * 70)

    if _interrupted:
        return 1
    if total_failures > 0 and total_successes == 0:
        return 1
    if total_failures > 0 and total_successes > 0:
        if args.strict:
            return 1
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())