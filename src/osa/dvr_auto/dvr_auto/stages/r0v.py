"""Stage 5: R0G -> R0V. DATA subruns with PixMask are reduced via sbatch;
everything else (calibration runs, subruns without mask) is copied.
Idempotent: subruns already fully present in the output are skipped."""
import time

from ..common import (copy_files, find_pixmask, find_subruns, has_marker, list_dates,
                      log, read_run_types)


def _script(cfg, run, chunk, outdir, logdir):
    lines = [cfg.job_preamble(), "rc=0"]
    for sr, files, mask in chunk:
        input_file = files[0]        # one representative stream; lstchain_r0g_to_r0v
                                      # produces the output for every stream of the subrun
        lines.append(f"lstchain_r0g_to_r0v -f {input_file} -o {outdir} --pixselection-file {mask} "
                     f"--log {logdir}/dvr_{run:05d}_{sr:04d}.log || rc=1")
    lines.append("exit $rc")          # job fails if any subrun failed
    return "\n".join(lines) + "\n"


def run(ctx):
    cfg = ctx.cfg
    in_root, out_root = cfg.path("r0v_input_root"), cfg.path("r0v_output_root")
    thr, size = cfg.raw["r0v"]["chunk_threshold"], cfg.raw["r0v"]["chunk_size"]
    s = cfg.raw["slurm"]["r0v"]
    marker = cfg.raw["logs"]["success_marker"]
    strict = cfg.raw["logs"].get("check_r0v", True)

    dates = list_dates(in_root, ctx.start, ctx.end)
    ctx.outdir.mkdir(parents=True, exist_ok=True)
    (ctx.outdir / "r0v_dates.txt").write_text("".join(d + "\n" for d in dates))
    t0 = time.time()
    jobs, ncopy = {}, 0                    # jid -> (run, outdir, logdir, chunk)

    for date in dates:
        types = read_run_types(cfg, date)
        if types is None:
            log.warning("%s: no RunSummary, date skipped", date)
            continue
        outd, logd = out_root / date, out_root / "log" / date
        scripts = logd if not ctx.dry_run else ctx.outdir / "dry_run" / date
        for d in {outd, logd, scripts}:
            if not ctx.dry_run or d == scripts:
                d.mkdir(parents=True, exist_ok=True)

        for run, subs in sorted(find_subruns(in_root / date).items()):
            if run not in types:
                log.warning("%s run %d not in RunSummary -> copied as non-DATA", date, run)
            is_data = types.get(run) == "DATA"
            todo = []
            for sr, files in sorted(subs.items()):
                # Require ALL streams of the subrun to already be present to
                # call it done; a single matching stream (e.g. left over from
                # an interrupted copy or reduction) is not enough.
                if not ctx.force and all((outd / f.name).exists() for f in files):
                    continue
                mask = find_pixmask(cfg, run, sr) if is_data else None
                if mask:
                    todo.append((sr, files, mask))   # keep the full file list, not just files[0]
                else:
                    ncopy += copy_files(files, outd, ctx.dry_run)   # no DATA or no mask
            chunks = [todo] if len(todo) < thr else [todo[i:i + size] for i in range(0, len(todo), size)]
            for i, chunk in enumerate(c for c in chunks if c):
                tag = f"{run:05d}" + (f"_{i}" if len(chunks) > 1 else "")
                script = scripts / f"dvr_reduction_{tag}.sh"
                script.write_text(_script(cfg, run, chunk, outd, logd))
                jid = ctx.slurm.submit(script, f"pixel_selection_{tag}",
                                       logd / f"pixel_selection_{tag}_%j.log", logd,
                                       s["partition"], s.get("extra", []))
                jobs[jid] = (run, outd, logd, chunk)

    log.info("r0v: %d jobs submitted, %d files copied", len(jobs), ncopy)
    states = ctx.slurm.wait(list(jobs))
    if ctx.dry_run:
        return []

    problems = []
    for jid, (run, outd, logd, chunk) in jobs.items():
        for sr, files, _ in chunk:
            lg = logd / f"dvr_{run:05d}_{sr:04d}.log"
            fresh = lg.exists() and lg.stat().st_mtime >= t0
            ok = states.get(jid) == "COMPLETED" or (fresh and has_marker(lg, marker))
            if ok and strict:
                ok = fresh and has_marker(lg, marker)
            if not ok:
                # Drop ALL stream outputs of this subrun, not just the
                # representative one, so a retry redoes the whole subrun
                # instead of being fooled by leftover partial files.
                for f in files:
                    (outd / f.name).unlink(missing_ok=True)
                problems.append(f"run {run} subrun {sr}: job {jid} {states.get(jid)} / log {lg}")
    for p in problems:
        log.error("r0v failure: %s", p)
    return problems
