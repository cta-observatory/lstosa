"""Stage 1: date_list.txt (dates with DL1) + R0G/DL1 mismatch report."""
from ..common import list_dates, read_run_types, log


def run(ctx):
    cfg = ctx.cfg
    ctx.outdir.mkdir(parents=True, exist_ok=True)
    dl1 = list_dates(cfg.path("dl1_root"), ctx.start, ctx.end, cfg.dl1_version)
    src = list_dates(cfg.path("r0v_input_root"), ctx.start, ctx.end)
    (ctx.outdir / "date_list.txt").write_text("".join(d + "\n" for d in dl1))

    for d in sorted(set(src) - set(dl1)):
        types = read_run_types(cfg, d)
        n = None if types is None else sum(t == "DATA" for t in types.values())
        if n == 0:
            log.info("%s: input without DL1 and no DATA runs (expected)", d)
        else:
            log.warning("%s: input without DL1 but DATA runs=%s (None = no RunSummary) -> check", d, n)
    log.info("dates: %d dates with DL1", len(dl1))
