import argparse
import logging
import sys
from datetime import datetime

from .common import Context, StageError, list_dates
from .config import Config
from .pipeline import ORDER, run_pipeline
from .slurm import Slurm


def valid_date(s):
    datetime.strptime(s, "%Y%m%d")
    return s


def main(argv=None):
    p = argparse.ArgumentParser(prog="dvr-auto", description="Automatic DVR pipeline (R0G -> R0V)")
    p.add_argument("start", type=valid_date)
    p.add_argument("end", nargs="?", type=valid_date, help="default: latest date available in the input")
    p.add_argument("--config", default="config.yaml")
    p.add_argument("--profile", help="cp02 | lstanalyzer (default: the one in config.yaml)")
    p.add_argument("--only", choices=ORDER)
    p.add_argument("--from-stage", choices=ORDER)
    p.add_argument("--dry-run", action="store_true", help="write scripts, do not submit or copy")
    p.add_argument("--force", action="store_true", help="ignore existing masks/outputs")
    a = p.parse_args(argv)

    cfg = Config.load(a.config, a.profile)
    end = a.end or (list_dates(cfg.path("r0v_input_root"), a.start, "99999999") or [a.start])[-1]
    outdir = cfg.path("workdir") / f"{a.start}_{end}"
    outdir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(), logging.FileHandler(outdir / "pipeline.log")])
    log = logging.getLogger("dvr")
    log.info("profile=%s range=%s..%s outdir=%s dry_run=%s", cfg.profile, a.start, end, outdir, a.dry_run)

    selected = [a.only] if a.only else ORDER[ORDER.index(a.from_stage or "dates"):]
    ctx = Context(cfg, a.start, end, outdir, Slurm(cfg, a.dry_run), a.dry_run, a.force)
    try:
        run_pipeline(ctx, selected)
    except StageError as e:
        log.error("PIPELINE FAILED: %s", e)
        return 1
    log.info("PIPELINE DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
