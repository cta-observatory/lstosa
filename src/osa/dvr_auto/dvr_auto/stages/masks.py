"""Stages 3-4: DVR settings and PixMask creation (one sbatch per run)."""
import shutil

from ..common import StageError, has_marker, has_pixmask, log, run_id_from_text

SETTINGS_CMD = 'lstchain_dvr_pixselector -n 1 -f "{pat}"'
PIXMASK_CMD = 'lstchain_dvr_pixselector --action create_pixel_masks -f "{pat}"'


def _work(ctx):
    w = ctx.outdir / "pixmask_work"      # settings h5 and masks share the execution dir
    w.mkdir(parents=True, exist_ok=True)
    return w


def _pending(ctx):
    f = ctx.outdir / "all_runs.txt"
    if not f.exists():
        raise StageError(f"{f} not found (run stage find_runs first)")
    out = []
    for pat in f.read_text().split():
        run = run_id_from_text(pat)
        if run is None:
            log.warning("cannot parse run id from %s", pat)
        elif ctx.force or not has_pixmask(ctx.cfg, run):
            out.append((run, pat))
    return out


def _run_jobs(ctx, name, template, pending):
    work, cfg = _work(ctx), ctx.cfg
    s = cfg.raw["slurm"][name]
    jobs = {}
    for run, pat in pending:
        script = work / f"{name}_{run:05d}.sh"
        script.write_text(cfg.job_preamble() + template.format(pat=pat) + "\n")
        jid = ctx.slurm.submit(script, f"dvr_{name}_{run:05d}",
                               work / f"slurm_{name}_{run:05d}_%j.out", work,
                               s["partition"], s.get("extra", []))
        jobs[jid] = (run, work / f"slurm_{name}_{run:05d}_{jid}.out")
    states = ctx.slurm.wait(list(jobs))
    if ctx.dry_run:
        return
    marker = cfg.raw["logs"]["success_marker"]
    check = cfg.raw["logs"].get("check_masks", False)
    bad = []
    for jid, (run, out) in jobs.items():
        st = states.get(jid, "UNKNOWN")
        if st != "COMPLETED":
            bad.append(f"run {run}: job {jid} {st} (log: {out})")
        elif check and not has_marker(out, marker):
            bad.append(f"run {run}: '{marker}' not found in {out}")
    if bad:
        raise StageError(f"{name}: {len(bad)} job(s) failed:\n  " + "\n  ".join(bad))
    log.info("%s: %d jobs OK", name, len(jobs))


def run_settings(ctx):
    pending = _pending(ctx)
    log.info("settings: %d runs without PixMask", len(pending))
    _run_jobs(ctx, "settings", SETTINGS_CMD, pending)


def run_pixmask(ctx):
    pending = _pending(ctx)
    log.info("pixmask: %d runs without PixMask", len(pending))
    _run_jobs(ctx, "pixmask", PIXMASK_CMD, pending)
    if ctx.dry_run:
        return
    dest = ctx.cfg.path("pixmask_dir")
    dest.mkdir(parents=True, exist_ok=True)
    files = sorted(_work(ctx).glob("Pixel_selection_LST*.h5"))
    for f in files:
        shutil.move(str(f), str(dest / f.name))
    log.info("pixmask: moved %d files to %s", len(files), dest)
