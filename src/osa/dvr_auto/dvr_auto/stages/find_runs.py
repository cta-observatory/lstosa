"""Stage 2: all_runs.txt (one DL1 glob per DATA run, same format as before)."""
from astropy.table import Table

from ..common import log


def run(ctx):
    cfg = ctx.cfg
    date_file = ctx.outdir / "date_list.txt"
    patterns = []
    for date in date_file.read_text().split():
        base = cfg.path("dl1_root") / date / cfg.dl1_version
        rs = cfg.path("run_summary_dir") / f"RunSummary_{date}.ecsv"
        if not rs.exists():
            log.warning("%s: no RunSummary, skipped", date)
            continue
        tailcuts = sorted(p for p in base.iterdir() if "tailcut" in p.name)
        table = Table.read(rs)
        for row in table[table["run_type"] == "DATA"]:
            glob = f"dl1_LST-1.Run{int(row['run_id']):05d}.????.h5"
            for tc in tailcuts:                     # first tailcut dir that has this run
                if next(tc.glob(glob), None):
                    patterns.append(str(tc / glob))
                    break
    (ctx.outdir / "all_runs.txt").write_text("".join(p + "\n" for p in patterns))
    log.info("find_runs: %d runs", len(patterns))
