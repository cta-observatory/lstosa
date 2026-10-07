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
    """Copy files safely (via a .part temp file). Creates outdir if needed.
    Skips files already present at the destination. Returns how many were
    actually copied (0 in dry_run, since nothing is written)."""
    n = 0
    files = list(files)
    if files and not dry_run:
        outdir.mkdir(parents=True, exist_ok=True)   # was missing: verify.py could
                                                      # call this before outdir existed
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


def find_pixmask(cfg, run: int, subrun: int):
    """Path to the mask of a specific (run, subrun), or None if missing."""
    for d in mask_dirs(cfg):
        f = d / f"Pixel_selection_LST-1.Run{run:05d}.{subrun:04d}.h5"
        if f.exists():
            return f
    return None


def dl1_subruns(pat: str) -> set:
    """Subrun numbers of the DL1 files that actually match an all_runs.txt
    glob pattern (".../dl1_LST-1.Run<run>.????.h5")."""
    p = Path(pat)
    return {int(f.name.split(".")[-2]) for f in p.parent.glob(p.name)}


def missing_pixmask_subruns(cfg, run: int, pat: str):
    """Subrun numbers of `run` (per its DL1 pattern) that still have no mask.
    Replaces the old run-level has_pixmask(): that one returned True as soon
    as a single mask file existed for the run, so a partially-generated run
    (e.g. 25 of 28 subrun masks) was wrongly treated as "already done" and
    never retried. This checks every expected subrun individually."""
    return sorted(sr for sr in dl1_subruns(pat) if find_pixmask(cfg, run, sr) is None)


def has_marker(path: Path, marker: str) -> bool:
    return path.exists() and marker.lower() in path.read_text(errors="ignore").lower()
