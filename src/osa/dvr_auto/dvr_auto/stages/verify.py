"""Stage 6: compare input vs R0V per (run, subrun), using RunSummary to classify.
Missing non-DATA files are copied (legit). Missing DATA subruns are real failures."""
from ..common import (Context, StageError, copy_files, find_subruns, list_dates, log,
                      read_run_types)


def run(ctx: Context):
    cfg = ctx.cfg
    in_root, out_root = cfg.path("r0v_input_root"), cfg.path("r0v_output_root")
    missing_data, unclassified, fixed = [], [], 0

    for date in list_dates(in_root, ctx.start, ctx.end):
        src, dst = find_subruns(in_root / date), find_subruns(out_root / date)
        types = read_run_types(cfg, date)
        for run, subs in src.items():
            for sr, files in subs.items():
                if sr in dst.get(run, {}):
                    continue
                if types is None:
                    unclassified.append((date, run, sr))
                elif types.get(run) == "DATA":
                    missing_data.append((date, run, sr))
                else:
                    fixed += copy_files(files, out_root / date, ctx.dry_run)
    log.info("verify: %d non-DATA files copied", fixed)
    if unclassified:
        log.error("verify: %d subruns missing and no RunSummary to classify them: %s",
                  len(unclassified), unclassified[:10])
    if missing_data:
        log.error("verify: %d DATA subruns missing in R0V: %s", len(missing_data), missing_data[:10])
    if not (missing_data or unclassified):
        log.info("verify: input == R0V")
    return missing_data, unclassified
