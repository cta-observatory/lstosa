"""Unit tests for dvr_auto: the shared helpers (common.py), every pipeline
stage (dates, find_runs, masks, r0v, verify), and stage orchestration
(pipeline.py).

Everything runs against a throwaway directory tree built by the fixtures
below (via pytest's tmp_path); SLURM is replaced by an in-memory
FakeSlurm, so no real sbatch/sacct, filesystem outside tmp_path, or
network access is needed. astropy and pyyaml are still required, since
dvr_auto itself needs them.

Run with:  pytest test_DVR.py -v
"""
import logging
from pathlib import Path

import pytest
from astropy.table import Table

from dvr_auto import pipeline
from dvr_auto.common import (Context, StageError, copy_files, dl1_subruns,
                              find_pixmask, find_subruns, has_marker,
                              list_dates, missing_pixmask_subruns,
                              read_run_types, run_id_from_text)
from dvr_auto.stages import dates as dates_stage
from dvr_auto.stages import find_runs as find_runs_stage
from dvr_auto.stages import masks as masks_stage
from dvr_auto.stages import r0v as r0v_stage
from dvr_auto.stages import verify as verify_stage


# =====================================================================
# Shared fixtures / fakes
# =====================================================================

class FakeConfig:
    """Stand-in for dvr_auto.config.Config, rooted at a pytest tmp_path
    so every stage can run against a disposable directory tree."""

    def __init__(self, root: Path):
        self.root = root
        self.raw = {
            "paths": {
                "dl1_root": str(root / "DL1"), "dl1_version": "v0.12",
                "run_summary_dir": str(root / "RunSummary"),
                "r0v_input_root": str(root / "R0G"),
                "r0v_output_root": str(root / "R0V"),
                "pixmask_dir": str(root / "PixelMasks"),
                "pixmask_extra_dirs": [], "workdir": str(root / "work"),
            },
            "env": {"conda_sh": "/bin/true", "lstchain_env": "lstchain-test", "extra_lines": []},
            "slurm": {"account": "test", "poll_seconds": 0, "max_queued_jobs": 9999,
                      "settings": {"partition": "short", "extra": []},
                      "pixmask": {"partition": "long", "extra": []},
                      "r0v": {"partition": "long", "extra": []}},
            "logs": {"success_marker": "success", "check_masks": False, "check_r0v": True},
            "r0v": {"chunk_threshold": 190, "chunk_size": 100},
            "pipeline": {"max_retries": 0},
        }

    def path(self, key: str) -> Path:
        return Path(self.raw["paths"][key])

    @property
    def dl1_version(self) -> str:
        return self.raw["paths"]["dl1_version"]

    def job_preamble(self) -> str:
        return "#!/bin/bash\necho fake-preamble\n"


class FakeSlurm:
    """In-memory stand-in for dvr_auto.slurm.Slurm. Scripts are recorded
    but never actually executed. `states` lets a test control what
    wait() reports per job id (default: everything COMPLETED).
    `out_contents` optionally writes text into a job's output/log path
    at submit time, to simulate what the real job would have written."""

    def __init__(self, states=None, out_contents=None):
        self.submitted = []            # (job_id, script, name, partition)
        self._next_id = 1
        self.states = states or {}
        self.out_contents = out_contents or {}

    def submit(self, script, name, output, workdir, partition, extra=()):
        jid = str(self._next_id)
        self._next_id += 1
        self.submitted.append((jid, script, name, partition))
        if jid in self.out_contents:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(self.out_contents[jid])
        return jid

    def wait(self, job_ids):
        return {j: self.states.get(j, "COMPLETED") for j in job_ids}


def write_run_summary(cfg, date, runs):
    """runs: list of (run_id, run_type)."""
    p = cfg.path("run_summary_dir") / f"RunSummary_{date}.ecsv"
    p.parent.mkdir(parents=True, exist_ok=True)
    Table({"run_id": [r for r, _ in runs], "run_type": [t for _, t in runs]}).write(
        p, format="ascii.ecsv", overwrite=True)


def write_dl1(cfg, date, tailcut, run, subruns):
    d = cfg.path("dl1_root") / date / cfg.dl1_version / f"tailcut{tailcut}"
    d.mkdir(parents=True, exist_ok=True)
    for sr in subruns:
        (d / f"dl1_LST-1.Run{run:05d}.{sr:04d}.h5").write_text("x")


def write_raw(cfg, date, run, subrun_streams, root_key="r0v_input_root"):
    """subrun_streams: {subrun: [stream numbers]}"""
    d = cfg.path(root_key) / date
    d.mkdir(parents=True, exist_ok=True)
    for sr, streams in subrun_streams.items():
        for st in streams:
            (d / f"LST-1.{st}.Run{run:05d}.{sr:04d}.fits.fz").write_text("x")


def write_mask(cfg, run, subrun):
    d = cfg.path("pixmask_dir")
    d.mkdir(parents=True, exist_ok=True)
    (d / f"Pixel_selection_LST-1.Run{run:05d}.{subrun:04d}.h5").write_text("x")


def make_ctx(cfg, slurm=None, dry_run=False, force=False,
             start="20260101", end="20260101"):
    outdir = cfg.path("workdir") / f"{start}_{end}"
    return Context(cfg=cfg, start=start, end=end, outdir=outdir,
                    slurm=slurm or FakeSlurm(), dry_run=dry_run, force=force)


@pytest.fixture
def cfg(tmp_path):
    return FakeConfig(tmp_path)


# =====================================================================
# common.py
# =====================================================================

def test_list_dates_filters_by_range_and_subdir(cfg):
    (cfg.path("r0v_input_root") / "20260101").mkdir(parents=True)
    (cfg.path("r0v_input_root") / "20260103").mkdir(parents=True)
    (cfg.path("r0v_input_root") / "not_a_date").mkdir(parents=True)
    assert list_dates(cfg.path("r0v_input_root"), "20260101", "20260102") == ["20260101"]


def test_run_id_from_text():
    assert run_id_from_text("...Run25071.0000.h5") == 25071
    assert run_id_from_text("nothing here") is None


def test_read_run_types(cfg):
    write_run_summary(cfg, "20260101", [(100, "DATA"), (101, "PEDCALIB")])
    assert read_run_types(cfg, "20260101") == {100: "DATA", 101: "PEDCALIB"}
    assert read_run_types(cfg, "20269999") is None


def test_find_subruns_groups_by_run_and_subrun(cfg):
    write_raw(cfg, "20260101", 200, {0: [1, 2, 3, 4], 1: [1, 2]})
    subs = find_subruns(cfg.path("r0v_input_root") / "20260101")
    assert set(subs[200].keys()) == {0, 1}
    assert len(subs[200][0]) == 4
    assert len(subs[200][1]) == 2


def test_copy_files_creates_dest_skips_existing_and_respects_dry_run(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    f1 = src / "a.txt"
    f1.write_text("hi")

    dest = tmp_path / "newdir" / "sub"
    assert copy_files([f1], dest) == 1
    assert (dest / "a.txt").read_text() == "hi"
    assert copy_files([f1], dest) == 0          # already there -> skipped

    dry_dest = tmp_path / "drydir"
    assert copy_files([f1], dry_dest, dry_run=True) == 1
    assert not dry_dest.exists()                 # dry_run must not touch disk


def test_find_pixmask_searches_primary_then_extra_dirs(cfg, tmp_path):
    write_mask(cfg, 300, 0)
    extra = tmp_path / "extra_masks"
    extra.mkdir()
    (extra / "Pixel_selection_LST-1.Run00300.0001.h5").write_text("x")
    cfg.raw["paths"]["pixmask_extra_dirs"] = [str(extra)]

    assert find_pixmask(cfg, 300, 0) is not None   # primary dir
    assert find_pixmask(cfg, 300, 1) is not None   # extra dir
    assert find_pixmask(cfg, 300, 2) is None


def test_missing_pixmask_subruns_catches_partial_run(cfg):
    """Regression test: has_pixmask() used to return True as soon as ANY
    mask existed for a run, so a partially-generated run (e.g. 2 of 4
    subrun masks) was wrongly treated as complete and never retried."""
    write_dl1(cfg, "20260101", "84", 400, [0, 1, 2, 3])
    pat = str(cfg.path("dl1_root") / "20260101" / cfg.dl1_version /
              "tailcut84" / "dl1_LST-1.Run00400.????.h5")
    assert dl1_subruns(pat) == {0, 1, 2, 3}

    write_mask(cfg, 400, 0)
    write_mask(cfg, 400, 1)
    assert missing_pixmask_subruns(cfg, 400, pat) == [2, 3]

    write_mask(cfg, 400, 2)
    write_mask(cfg, 400, 3)
    assert missing_pixmask_subruns(cfg, 400, pat) == []


def test_has_marker(tmp_path):
    lg = tmp_path / "x.log"
    lg.write_text("something\nALL SUCCESS\n")
    assert has_marker(lg, "success")           # case-insensitive
    assert not has_marker(lg, "failure")
    assert not has_marker(tmp_path / "nope.log", "success")


# =====================================================================
# dates.py
# =====================================================================

def test_dates_stage(cfg, caplog):
    write_dl1(cfg, "20260101", "84", 100, [0, 1])                 # has DL1
    write_raw(cfg, "20260102", 200, {0: [1]})                     # raw only
    write_run_summary(cfg, "20260102", [(200, "PEDCALIB")])       # no DATA -> expected
    write_raw(cfg, "20260103", 300, {0: [1]})                     # raw only
    write_run_summary(cfg, "20260103", [(300, "DATA")])           # DATA -> should warn

    ctx = make_ctx(cfg, start="20260101", end="20260103")
    ctx.outdir.mkdir(parents=True, exist_ok=True)

    with caplog.at_level(logging.WARNING, logger="dvr"):
        dates_stage.run(ctx)

    assert (ctx.outdir / "date_list.txt").read_text().split() == ["20260101"]
    warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert any("20260103" in w for w in warnings)
    assert not any("20260102" in w for w in warnings)


# =====================================================================
# find_runs.py
# =====================================================================

def test_find_runs_stage(cfg):
    write_run_summary(cfg, "20260101", [(100, "DATA"), (101, "PEDCALIB")])
    write_dl1(cfg, "20260101", "84", 100, [0, 1, 2])   # only the DATA run has DL1

    ctx = make_ctx(cfg, start="20260101", end="20260101")
    ctx.outdir.mkdir(parents=True, exist_ok=True)
    (ctx.outdir / "date_list.txt").write_text("20260101\n")

    find_runs_stage.run(ctx)
    lines = (ctx.outdir / "all_runs.txt").read_text().split()

    assert len(lines) == 1
    assert "tailcut84" in lines[0] and "Run00100" in lines[0]
    assert lines[0].endswith("dl1_LST-1.Run00100.????.h5")


def test_find_runs_stage_skips_date_without_run_summary(cfg):
    ctx = make_ctx(cfg, start="20269999", end="20269999")
    ctx.outdir.mkdir(parents=True, exist_ok=True)
    (ctx.outdir / "date_list.txt").write_text("20269999\n")

    find_runs_stage.run(ctx)   # must not crash

    assert (ctx.outdir / "all_runs.txt").read_text() == ""


# =====================================================================
# masks.py
# =====================================================================

def test_pending_raises_if_all_runs_file_missing(cfg):
    ctx = make_ctx(cfg)
    ctx.outdir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(StageError):
        masks_stage._pending(ctx)


def test_pending_tracks_partial_and_complete_masks(cfg):
    write_dl1(cfg, "20260101", "84", 500, [0, 1, 2])
    pat = str(cfg.path("dl1_root") / "20260101" / cfg.dl1_version /
              "tailcut84" / "dl1_LST-1.Run00500.????.h5")
    ctx = make_ctx(cfg)
    ctx.outdir.mkdir(parents=True, exist_ok=True)
    (ctx.outdir / "all_runs.txt").write_text(pat + "\n")

    assert [r for r, _ in masks_stage._pending(ctx)] == [500]       # no masks yet

    write_mask(cfg, 500, 0)
    write_mask(cfg, 500, 1)
    assert [r for r, _ in masks_stage._pending(ctx)] == [500]       # still partial

    write_mask(cfg, 500, 2)
    assert masks_stage._pending(ctx) == []                          # complete now

    ctx_force = make_ctx(cfg, force=True)
    ctx_force.outdir = ctx.outdir
    assert [r for r, _ in masks_stage._pending(ctx_force)] == [500]  # --force ignores it


def test_run_settings_submits_correct_job(cfg):
    write_dl1(cfg, "20260101", "84", 600, [0])
    pat = str(cfg.path("dl1_root") / "20260101" / cfg.dl1_version /
              "tailcut84" / "dl1_LST-1.Run00600.????.h5")
    ctx = make_ctx(cfg)
    ctx.outdir.mkdir(parents=True, exist_ok=True)
    (ctx.outdir / "all_runs.txt").write_text(pat + "\n")
    ctx.slurm = FakeSlurm()

    masks_stage.run_settings(ctx)

    assert len(ctx.slurm.submitted) == 1
    _, script, _, partition = ctx.slurm.submitted[0]
    content = script.read_text()
    assert "fake-preamble" in content
    assert "lstchain_dvr_pixselector -n 1" in content
    assert partition == "short"


def test_run_settings_raises_on_failed_job(cfg):
    write_dl1(cfg, "20260101", "84", 700, [0])
    pat = str(cfg.path("dl1_root") / "20260101" / cfg.dl1_version /
              "tailcut84" / "dl1_LST-1.Run00700.????.h5")
    ctx = make_ctx(cfg)
    ctx.outdir.mkdir(parents=True, exist_ok=True)
    (ctx.outdir / "all_runs.txt").write_text(pat + "\n")
    ctx.slurm = FakeSlurm(states={"1": "FAILED"})

    with pytest.raises(StageError, match="run 700"):
        masks_stage.run_settings(ctx)


def test_run_settings_check_masks_requires_marker(cfg):
    cfg.raw["logs"]["check_masks"] = True
    write_dl1(cfg, "20260101", "84", 800, [0])
    pat = str(cfg.path("dl1_root") / "20260101" / cfg.dl1_version /
              "tailcut84" / "dl1_LST-1.Run00800.????.h5")
    ctx = make_ctx(cfg)
    ctx.outdir.mkdir(parents=True, exist_ok=True)
    (ctx.outdir / "all_runs.txt").write_text(pat + "\n")
    ctx.slurm = FakeSlurm(out_contents={"1": "no marker here\n"})   # COMPLETED, no "success"

    with pytest.raises(StageError, match="not found"):
        masks_stage.run_settings(ctx)


def test_run_pixmask_moves_generated_masks(cfg):
    write_dl1(cfg, "20260101", "84", 900, [0, 1])
    pat = str(cfg.path("dl1_root") / "20260101" / cfg.dl1_version /
              "tailcut84" / "dl1_LST-1.Run00900.????.h5")
    ctx = make_ctx(cfg)
    ctx.outdir.mkdir(parents=True, exist_ok=True)
    (ctx.outdir / "all_runs.txt").write_text(pat + "\n")
    ctx.slurm = FakeSlurm()

    work = ctx.outdir / "pixmask_work"
    work.mkdir(parents=True, exist_ok=True)
    (work / "Pixel_selection_LST-1.Run00900.0000.h5").write_text("x")
    (work / "Pixel_selection_LST-1.Run00900.0001.h5").write_text("x")

    masks_stage.run_pixmask(ctx)

    moved = sorted(p.name for p in cfg.path("pixmask_dir").glob("*.h5"))
    assert len(moved) == 2
    assert not any(work.glob("*.h5"))


# =====================================================================
# verify.py
# =====================================================================

def test_verify_classifies_and_copies_correctly(cfg):
    write_run_summary(cfg, "20260101", [(100, "DATA"), (101, "PEDCALIB")])

    # DATA run: subrun 0 fully present, subrun 1 only 2 of 4 streams
    write_raw(cfg, "20260101", 100, {0: [1, 2, 3, 4], 1: [1, 2, 3, 4]}, "r0v_input_root")
    write_raw(cfg, "20260101", 100, {0: [1, 2, 3, 4], 1: [1, 2]}, "r0v_output_root")
    # non-DATA run: fully missing from output -> must be copied
    write_raw(cfg, "20260101", 101, {0: [1, 2]}, "r0v_input_root")
    # date with no RunSummary at all -> unclassified
    write_raw(cfg, "20269998", 999, {0: [1]}, "r0v_input_root")

    ctx = make_ctx(cfg, start="20260101", end="20269998")
    missing_data, unclassified = verify_stage.run(ctx)

    assert ("20260101", 100, 1) in missing_data        # partial streams -> real failure
    assert ("20260101", 100, 0) not in missing_data    # complete -> fine
    assert ("20269998", 999, 0) in unclassified

    copied = sorted(p.name for p in (cfg.path("r0v_output_root") / "20260101")
                     .glob("LST-1.*.Run00101.*"))
    assert len(copied) == 2


def test_verify_creates_missing_output_dir(cfg):
    """Regression test: copy_files() used to assume the destination
    directory already existed, so running `verify` on its own (without
    `r0v` having run first) crashed instead of failing cleanly."""
    write_run_summary(cfg, "20260101", [(555, "PEDCALIB")])
    write_raw(cfg, "20260101", 555, {0: [1]}, "r0v_input_root")
    assert not (cfg.path("r0v_output_root") / "20260101").exists()

    ctx = make_ctx(cfg, start="20260101", end="20260101")
    verify_stage.run(ctx)   # must not raise

    assert (cfg.path("r0v_output_root") / "20260101" /
            "LST-1.1.Run00555.0000.fits.fz").exists()


# =====================================================================
# r0v.py
# =====================================================================

def test_r0v_reduces_data_with_mask_and_copies_the_rest(cfg):
    write_run_summary(cfg, "20260101", [(100, "DATA"), (101, "PEDCALIB")])
    write_raw(cfg, "20260101", 100, {0: [1, 2, 3, 4]}, "r0v_input_root")
    write_raw(cfg, "20260101", 101, {0: [1, 2]}, "r0v_input_root")
    write_mask(cfg, 100, 0)
    cfg.raw["logs"]["check_r0v"] = False   # strict mode is tested separately below

    ctx = make_ctx(cfg, start="20260101", end="20260101")
    ctx.slurm = FakeSlurm()
    problems = r0v_stage.run(ctx)

    assert len(ctx.slurm.submitted) == 1
    _, script, _, _ = ctx.slurm.submitted[0]
    content = script.read_text()
    assert "LST-1.1.Run00100.0000.fits.fz" in content     # stream-1 used as -f input
    assert "lstchain_r0g_to_r0v" in content and "--pixselection-file" in content

    copied = sorted(p.name for p in (cfg.path("r0v_output_root") / "20260101")
                     .glob("LST-1.*.Run00101.*"))
    assert len(copied) == 2
    assert problems == []


def test_r0v_skips_complete_subrun_but_redoes_partial_one(cfg):
    """Regression test: the old `any(...)` check treated a subrun as done
    as soon as ONE of its stream files existed, so an interrupted copy or
    reduction (leaving e.g. 1 of 4 streams) was never retried."""
    write_run_summary(cfg, "20260101", [(200, "DATA")])
    write_raw(cfg, "20260101", 200, {0: [1, 2, 3, 4], 1: [1, 2, 3, 4]}, "r0v_input_root")
    write_mask(cfg, 200, 0)
    write_mask(cfg, 200, 1)
    write_raw(cfg, "20260101", 200, {0: [1, 2, 3, 4]}, "r0v_output_root")   # subrun 0: complete
    outd = cfg.path("r0v_output_root") / "20260101"
    outd.mkdir(parents=True, exist_ok=True)
    (outd / "LST-1.1.Run00200.0001.fits.fz").write_text("x")               # subrun 1: only 1/4

    ctx = make_ctx(cfg, start="20260101", end="20260101")
    ctx.slurm = FakeSlurm()
    r0v_stage.run(ctx)

    assert len(ctx.slurm.submitted) == 1   # only the incomplete subrun gets reprocessed


def test_r0v_splits_into_chunks_above_threshold(cfg):
    cfg.raw["r0v"] = {"chunk_threshold": 3, "chunk_size": 2}
    write_run_summary(cfg, "20260101", [(300, "DATA")])
    write_raw(cfg, "20260101", 300, {sr: [1] for sr in range(5)}, "r0v_input_root")
    for sr in range(5):
        write_mask(cfg, 300, sr)

    ctx = make_ctx(cfg, start="20260101", end="20260101")
    ctx.slurm = FakeSlurm()
    r0v_stage.run(ctx)

    assert len(ctx.slurm.submitted) == 3   # 5 subruns, chunks of 2 -> 3 jobs


def test_r0v_cleans_up_all_streams_of_a_failed_subrun(cfg):
    """Regression test: cleanup used to delete only the stream-1 file
    (the one used as the job's -f argument), leaving streams 2-4 behind
    if the job had already written them before failing. Those leftovers
    then fooled the next run's "already done" check."""
    write_run_summary(cfg, "20260101", [(401, "DATA")])
    write_raw(cfg, "20260101", 401, {0: [1, 2, 3, 4]}, "r0v_input_root")
    write_mask(cfg, 401, 0)
    outd = cfg.path("r0v_output_root") / "20260101"
    outd.mkdir(parents=True, exist_ok=True)

    class DyingSlurm(FakeSlurm):
        """Simulates a job that dies after writing streams 2-4 but
        before (or without) finishing stream 1."""
        def submit(self, script, name, output, workdir, partition, extra=()):
            jid = super().submit(script, name, output, workdir, partition, extra)
            for st in (2, 3, 4):
                (outd / f"LST-1.{st}.Run00401.0000.fits.fz").write_text("partial")
            return jid

    ctx = make_ctx(cfg, start="20260101", end="20260101")
    ctx.slurm = DyingSlurm(states={"1": "FAILED"})
    problems = r0v_stage.run(ctx)

    assert sorted(outd.glob("LST-1.*.Run00401.*")) == []   # nothing left behind
    assert len(problems) == 1


def test_r0v_strict_mode_requires_per_subrun_log(cfg):
    write_run_summary(cfg, "20260101", [(500, "DATA")])
    write_raw(cfg, "20260101", 500, {0: [1]}, "r0v_input_root")
    write_mask(cfg, 500, 0)
    # check_r0v defaults to True: job reports COMPLETED, but no
    # dvr_<run>_<subrun>.log was ever written -> must still be flagged.

    ctx = make_ctx(cfg, start="20260101", end="20260101")
    ctx.slurm = FakeSlurm(states={"1": "COMPLETED"})
    problems = r0v_stage.run(ctx)

    assert len(problems) == 1


# =====================================================================
# pipeline.py (stage orchestration)
# =====================================================================

class FakeStageConfig:
    def __init__(self, max_retries=0):
        self.raw = {"pipeline": {"max_retries": max_retries}}


def make_pipeline_ctx(max_retries=0, dry_run=False):
    return Context(cfg=FakeStageConfig(max_retries), start="20260101", end="20260101",
                    outdir=None, slurm=None, dry_run=dry_run, force=False)


@pytest.fixture
def calls():
    return []


@pytest.fixture
def fake_simple_stages(monkeypatch, calls):
    """SIMPLE is built at import time (SIMPLE = {"dates": dates.run, ...}),
    so patching dvr_auto.stages.dates.run afterwards would NOT affect it;
    patch the dict entries directly instead."""
    def make(name):
        def _f(ctx):
            calls.append(name)
        return _f
    for name in ("dates", "find_runs", "settings", "pixmask"):
        monkeypatch.setitem(pipeline.SIMPLE, name, make(name))


@pytest.fixture
def fake_r0v(monkeypatch, calls):
    def set_fake(side_effect=None):
        def _f(ctx):
            calls.append("r0v")
            if side_effect:
                side_effect(ctx)
        monkeypatch.setattr(pipeline.r0v, "run", _f)
    return set_fake


@pytest.fixture
def fake_verify(monkeypatch, calls):
    def set_fake(results):
        state = {"n": 0}
        def _f(ctx):
            calls.append("verify")
            state["n"] += 1
            return results(state["n"]) if callable(results) else results
        monkeypatch.setattr(pipeline.verify, "run", _f)
    return set_fake


def test_full_pipeline_runs_every_stage_once_in_order(
        fake_simple_stages, fake_r0v, fake_verify, calls):
    fake_r0v()
    fake_verify(([], []))

    pipeline.run_pipeline(make_pipeline_ctx(), pipeline.ORDER)

    assert calls == ["dates", "find_runs", "settings", "pixmask", "r0v", "verify"]


def test_r0v_retries_until_verify_is_clean(fake_r0v, fake_verify, calls):
    fake_r0v()
    fake_verify(lambda n: ([("20260101", 1, 0)], []) if n == 1 else ([], []))

    pipeline.run_pipeline(make_pipeline_ctx(max_retries=2), ["r0v", "verify"])

    assert calls == ["r0v", "verify", "r0v", "verify"]


def test_r0v_retry_exhaustion_raises_stage_error(fake_r0v, fake_verify, calls):
    fake_r0v()
    fake_verify(([("20260101", 1, 0)], []))   # always something missing

    with pytest.raises(StageError, match="R0V incomplete"):
        pipeline.run_pipeline(make_pipeline_ctx(max_retries=2), ["r0v", "verify"])

    assert calls.count("r0v") == 3
    assert calls.count("verify") == 3


def test_dry_run_never_calls_verify(fake_r0v, fake_verify, calls):
    fake_r0v()
    fake_verify(([], []))

    pipeline.run_pipeline(make_pipeline_ctx(dry_run=True), ["r0v", "verify"])

    assert calls == ["r0v"]


def test_r0v_selected_alone_skips_the_retry_wrapper(fake_r0v, fake_verify, calls):
    fake_r0v()
    fake_verify(([("20260101", 1, 0)], []))   # would fail retries if this were used

    pipeline.run_pipeline(make_pipeline_ctx(max_retries=5), ["r0v"])

    assert calls == ["r0v"]


def test_standalone_stage_failure_stops_the_pipeline(fake_simple_stages, calls):
    def failing_settings(ctx):
        calls.append("settings")
        raise StageError("boom")
    pipeline.SIMPLE["settings"] = failing_settings

    with pytest.raises(StageError, match="boom"):
        pipeline.run_pipeline(make_pipeline_ctx(), ["dates", "settings", "find_runs"])

    assert calls == ["dates", "settings"]   # find_runs never reached
