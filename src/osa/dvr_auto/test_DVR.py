"""Tests for dvr_auto/pipeline.py: stage orchestration and r0v/verify retries.

These tests never touch disk, SLURM or astropy: every stage function is
replaced with a fake that just records that it was called (and, for
r0v/verify, what it returns). They only check *orchestration*: which
functions get called, in what order, how many times, and whether failures
propagate -- not what any individual stage actually does internally
(that belongs in stage-specific tests).

Run with:  pytest test_pipeline.py -v
(needs the project on PYTHONPATH, and the dvr_auto package's own
dependencies -- astropy, pyyaml -- installed, since pipeline.py imports
dvr_auto.stages, and those import dvr_auto.common, which imports astropy.)

NOTE on how the patching works: pipeline.py builds, at import time,
    SIMPLE = {"dates": dates.run, "find_runs": find_runs.run, ...}
so monkeypatching dvr_auto.stages.dates.run afterwards would NOT affect
pipeline.SIMPLE (it already holds the original function object). These
tests patch pipeline.SIMPLE[...] directly for those stages instead.
r0v and verify are different: _r0v_with_verify() calls r0v.run(ctx) and
verify.run(ctx) as plain attribute lookups on the module, performed at
call time, so patching dvr_auto.stages.r0v.run / verify.run does take
effect there.
"""
import pytest

from dvr_auto import pipeline
from dvr_auto.common import Context, StageError


class FakeConfig:
    """Minimal stand-in for dvr_auto.config.Config: only what
    _r0v_with_verify actually reads (pipeline.max_retries)."""

    def __init__(self, max_retries=0):
        self.raw = {"pipeline": {"max_retries": max_retries}}


def make_ctx(max_retries=0, dry_run=False):
    return Context(cfg=FakeConfig(max_retries), start="20260101", end="20260101",
                    outdir=None, slurm=None, dry_run=dry_run, force=False)


@pytest.fixture
def calls():
    """Shared list every fake stage appends its name to, so tests can
    assert both on *which* stages ran and on the *order*/*count*."""
    return []


@pytest.fixture
def fake_simple_stages(monkeypatch, calls):
    """Replace the SIMPLE-dict stages (dates, find_runs, settings, pixmask)
    with no-op fakes that just record a call. Restored automatically by
    monkeypatch after the test."""
    def make(name):
        def _f(ctx):
            calls.append(name)
        return _f

    for name in ("dates", "find_runs", "settings", "pixmask"):
        monkeypatch.setitem(pipeline.SIMPLE, name, make(name))


@pytest.fixture
def fake_r0v(monkeypatch, calls):
    """Replace r0v.run with a fake; returns a setter to control what it does."""
    def set_fake(side_effect=None):
        def _f(ctx):
            calls.append("r0v")
            if side_effect:
                side_effect(ctx)
        monkeypatch.setattr(pipeline.r0v, "run", _f)
    return set_fake


@pytest.fixture
def fake_verify(monkeypatch, calls):
    """Replace verify.run with a fake; returns a setter to control what
    (missing, unclassified) it reports back."""
    def set_fake(results):
        """`results` is either a fixed (missing, unclassified) tuple, or a
        callable taking the call count (1-based) and returning one."""
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
    fake_verify(([], []))          # verify finds nothing missing

    ctx = make_ctx()
    pipeline.run_pipeline(ctx, pipeline.ORDER)

    assert calls == ["dates", "find_runs", "settings", "pixmask", "r0v", "verify"]


def test_r0v_retries_until_verify_is_clean(fake_r0v, fake_verify, calls):
    fake_r0v()
    # first call: something's missing; second call: all good
    fake_verify(lambda n: ([("20260101", 1, 0)], []) if n == 1 else ([], []))

    ctx = make_ctx(max_retries=2)
    pipeline.run_pipeline(ctx, ["r0v", "verify"])

    assert calls == ["r0v", "verify", "r0v", "verify"]


def test_r0v_retry_exhaustion_raises_stage_error(fake_r0v, fake_verify, calls):
    fake_r0v()
    fake_verify(([("20260101", 1, 0)], []))   # always something missing

    ctx = make_ctx(max_retries=2)
    with pytest.raises(StageError, match="R0V incomplete"):
        pipeline.run_pipeline(ctx, ["r0v", "verify"])

    # ran the initial attempt plus both retries, no more
    assert calls.count("r0v") == 3
    assert calls.count("verify") == 3


def test_dry_run_never_calls_verify(fake_r0v, fake_verify, calls):
    fake_r0v()
    fake_verify(([], []))

    ctx = make_ctx(dry_run=True)
    pipeline.run_pipeline(ctx, ["r0v", "verify"])

    assert calls == ["r0v"]


def test_r0v_selected_alone_skips_the_retry_wrapper(fake_r0v, fake_verify, calls):
    fake_r0v()
    fake_verify(([("20260101", 1, 0)], []))   # would fail retries if used

    ctx = make_ctx(max_retries=5)
    pipeline.run_pipeline(ctx, ["r0v"])        # "verify" NOT selected

    assert calls == ["r0v"]                    # no retry loop, no verify call


def test_standalone_stage_failure_stops_the_pipeline(fake_simple_stages, calls):
    def failing_settings(ctx):
        calls.append("settings")
        raise StageError("boom")
    pipeline.SIMPLE["settings"] = failing_settings

    ctx = make_ctx()
    with pytest.raises(StageError, match="boom"):
        pipeline.run_pipeline(ctx, ["dates", "settings", "find_runs"])

    # dates ran, settings raised, find_runs was never reached
    assert calls == ["dates", "settings"]
