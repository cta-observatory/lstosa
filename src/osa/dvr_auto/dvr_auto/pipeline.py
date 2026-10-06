from .common import StageError, log
from .stages import dates, find_runs, masks, r0v, verify

ORDER = ["dates", "find_runs", "settings", "pixmask", "r0v", "verify"]
SIMPLE = {
    "dates": dates.run,
    "find_runs": find_runs.run,
    "settings": masks.run_settings,
    "pixmask": masks.run_pixmask,
    "verify": verify.run,
}


def _r0v_with_verify(ctx):
    retries = ctx.cfg.raw["pipeline"].get("max_retries", 0)
    for attempt in range(retries + 1):
        r0v.run(ctx)
        if ctx.dry_run:
            return
        missing, unclassified = verify.run(ctx)
        if not (missing or unclassified):
            return
        if attempt < retries:
            log.warning("retry %d/%d for missing subruns", attempt + 1, retries)
    raise StageError("R0V incomplete after verification (see errors above)")


def run_pipeline(ctx, selected):
    for name in selected:
        log.info("=== stage: %s ===", name)
        if name == "r0v" and "verify" in selected:
            _r0v_with_verify(ctx)
        elif name == "verify" and "r0v" in selected:
            continue                      # already done inside the r0v retry loop
        elif name == "r0v":
            problems = r0v.run(ctx)
            if problems:
                raise StageError(f"R0V failed for {len(problems)} subrun(s) (see errors above)")
        else:
            SIMPLE[name](ctx)
