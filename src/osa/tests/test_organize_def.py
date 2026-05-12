import datetime
import os
from pathlib import Path

import pytest

from osa.scripts.organize_def import (
    clean_path,
    load_config,
    compress_logs,
    compress_history,
    compress_gainsel,
    _is_stable_gainsel_log,
)


# =========================================================
# clean_path
# =========================================================
def test_clean_path_replace_base(tmp_path):
    base = str(tmp_path)

    result = clean_path("%(BASE)s/data", base)

    assert result == tmp_path / "data"


def test_clean_path_without_base(tmp_path):
    base = str(tmp_path)

    result = clean_path("/my/path", base)

    assert result == Path("/my/path")


# =========================================================
# load_config
# =========================================================
def test_load_config_ok(tmp_path):
    cfg = tmp_path / "test.cfg"

    cfg.write_text(
        f"""
[LST1]
BASE={tmp_path}
ANALYSIS_DIR=%(BASE)s/analysis
OSA_DIR=%(BASE)s/osa
PROD_ID=v1
"""
    )

    running_analysis, gainsel, prod_id = load_config(cfg)

    assert running_analysis == tmp_path / "analysis"
    assert gainsel == tmp_path / "osa" / "GainSel_log"
    assert prod_id == "v1"


def test_load_config_missing_file():
    with pytest.raises(FileNotFoundError):
        load_config("missing.cfg")


def test_load_config_missing_section(tmp_path):
    cfg = tmp_path / "bad.cfg"

    cfg.write_text(
        """
[OTHER]
BASE=/tmp
"""
    )

    with pytest.raises(ValueError):
        load_config(cfg)


# =========================================================
# compress_logs
# =========================================================
def test_compress_logs_no_log_dir(tmp_path):
    compress_logs(tmp_path, simulate=True)


def test_compress_logs_real(tmp_path):
    log_dir = tmp_path / "log"
    log_dir.mkdir()

    err_file = log_dir / "a.err"
    out_file = log_dir / "b.out"

    err_file.write_text("error")
    out_file.write_text("output")

    compress_logs(tmp_path, simulate=False)

    assert (log_dir / "logs_err.tar.gz").exists()
    assert (log_dir / "logs_out.tar.gz").exists()

    assert not err_file.exists()
    assert not out_file.exists()


def test_compress_logs_simulate(tmp_path):
    log_dir = tmp_path / "log"
    log_dir.mkdir()

    err_file = log_dir / "a.err"
    err_file.write_text("error")

    compress_logs(tmp_path, simulate=True)

    assert err_file.exists()
    assert not (log_dir / "logs_err.tar.gz").exists()


# =========================================================
# compress_history
# =========================================================
def test_compress_history_real(tmp_path):
    history_file = tmp_path / "test.history"
    history_file.write_text("history")

    compress_history(tmp_path, simulate=False)

    assert (tmp_path / "all_history.tar.gz").exists()
    assert not history_file.exists()


def test_compress_history_empty(tmp_path):
    compress_history(tmp_path, simulate=False)

    assert not (tmp_path / "all_history.tar.gz").exists()


# =========================================================
# _is_stable_gainsel_log
# =========================================================
def test_is_stable_gainsel_log_false_for_non_log(tmp_path):
    file = tmp_path / "file.txt"
    file.write_text("x")

    assert _is_stable_gainsel_log(file) is False


def test_is_stable_gainsel_log_false_for_today_file(tmp_path):
    log = tmp_path / "today.log"
    log.write_text("x")

    assert _is_stable_gainsel_log(log) is False


def test_is_stable_gainsel_log_true_for_old_file(tmp_path):
    log = tmp_path / "old.log"
    log.write_text("x")

    old_time = datetime.datetime.now().timestamp() - (2 * 86400)

    os.utime(log, (old_time, old_time))

    assert _is_stable_gainsel_log(log) is True


# =========================================================
# compress_gainsel
# =========================================================
def test_compress_gainsel_missing_path(tmp_path):
    missing = tmp_path / "missing"

    compress_gainsel(missing, simulate=False)


def test_compress_gainsel_real(tmp_path):
    check_log = tmp_path / "check_test.log"
    normal_log = tmp_path / "normal.log"

    check_log.write_text("check")
    normal_log.write_text("normal")

    old_time = datetime.datetime.now().timestamp() - (2 * 86400)

    os.utime(check_log, (old_time, old_time))
    os.utime(normal_log, (old_time, old_time))

    compress_gainsel(tmp_path, simulate=False)

    assert (tmp_path / "check_logs.tar.gz").exists()
    assert (tmp_path / "normal_logs.tar.gz").exists()

    assert not check_log.exists()
    assert not normal_log.exists()


def test_compress_gainsel_simulate(tmp_path):
    log = tmp_path / "normal.log"
    log.write_text("normal")

    old_time = datetime.datetime.now().timestamp() - (2 * 86400)

    os.utime(log, (old_time, old_time))

    compress_gainsel(tmp_path, simulate=True)

    assert log.exists()
    assert not (tmp_path / "normal_logs.tar.gz").exists()
