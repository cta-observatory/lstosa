import logging
import re
import shutil
from dataclasses import dataclass
from pathlib import Path

from astropy.table import Table

log = logging.getLogger("dvr")


class StageError(RuntimeError):
    pass


@dataclass
class Context:
    cfg: object
    start: str
    end: str
    outdir: Path
    slurm: object
    dry_run: bool = False
    force: bool = False


def list_dates(root: Path, start: str, end: str, subdir: str = ""):
    if not root.is_dir():
        return []
    return sorted(
        d.name for d in root.iterdir()
        if d.is_dir() and d.name.isdigit() and len(d.name) == 8
        and start <= d.name <= end and (d / subdir).is_dir()
    )


def run_id_from_text(s: str):
    m = re.search(r"Run(\d{5})", s)
    return int(m.group(1)) if m else None


def read_run_types(cfg, date: str):
    """{run_id: run_type} from RunSummary, or None if the file is missing."""
    f = cfg.path("run_summary_dir") / f"RunSummary_{date}.ecsv"
    if not f.exists():
        return None
    return {int(r["run_id"]): str(r["run_type"]) for r in Table.read(f)}


def find_subruns(indir: Path) -> dict:
    """{run: {subrun: [files of every stream]}}"""
    out = {}
    for f in sorted(indir.glob("LST-1.?.Run?????.????.fits.fz")):
        p = f.name.split(".")
        out.setdefault(int(p[2][3:]), {}).setdefault(int(p[3]), []).append(f)
    return out


def copy_files(files, outdir: Path, dry_run=False) -> int:
    n = 0
    for f in files:
        dest = outdir / f.name
        if dest.exists():
            continue
        log.info("copy %s -> %s", f, outdir)
        n += 1
        if dry_run:
            continue
        tmp = dest.with_name(dest.name + ".part")   # never leave half-copied files as valid
        shutil.copy2(f, tmp)
        tmp.rename(dest)
    return n


def mask_dirs(cfg):
    return [cfg.path("pixmask_dir")] + [Path(p) for p in cfg.raw["paths"].get("pixmask_extra_dirs", [])]


def has_pixmask(cfg, run: int) -> bool:
    return any(next(d.glob(f"Pixel_selection_LST-1.Run{run:05d}.*.h5"), None) for d in mask_dirs(cfg))


def find_pixmask(cfg, run: int, subrun: int):
    for d in mask_dirs(cfg):
        f = d / f"Pixel_selection_LST-1.Run{run:05d}.{subrun:04d}.h5"
        if f.exists():
            return f
    return None


def has_marker(path: Path, marker: str) -> bool:
    return path.exists() and marker.lower() in path.read_text(errors="ignore").lower()
