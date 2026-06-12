import os
from pathlib import Path
import pytest
from osa.utils.interleaved_date import (
    clean_path,
    load_config,
    summary_dates,
    info_dates,
)


# =========================================================
# clean_path
# =========================================================

def test_clean_path_replace_base(tmp_path):
    base = str(tmp_path)

    result = clean_path("%(BASE)s/data", base)

    assert result == str(tmp_path / "data")


def test_clean_path_without_base(tmp_path):
    base = str(tmp_path)

    result = clean_path("/my/path", base)

    assert result == "/my/path"


# =========================================================
# load_config
# =========================================================

def test_load_config_ok(tmp_path):
    cfg = tmp_path / "test.cfg"

    cfg.write_text(
        f"""
[LST1]
BASE={tmp_path}
RUN_SUMMARY_DIR=%(BASE)s/RunSummary
RUN_CATALOG=%(BASE)s/RunCatalog
OSA_DIR=%(BASE)s/OSA
"""
    )

    summary, catalog, osa = load_config(cfg)

    assert summary == str(tmp_path / "RunSummary")
    assert catalog == str(tmp_path / "RunCatalog")
    assert osa == str(tmp_path / "OSA")


def test_load_config_missing_file():
    with pytest.raises(Exception):
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
# summary_dates
# =========================================================

def test_summary_dates_ok(tmp_path):
    summary_dir = tmp_path
    file = summary_dir / "RunSummary_20250101.ecsv"

    file.write_text(
        """
# comment
12345,xxx,DATA
12346,xxx,CALIB
"""
    )

    result = summary_dates("20250101", summary_dir)

    assert 12345 in result["run_id"]
    assert "DATA" in result["run_type"]


def test_summary_dates_missing_file(tmp_path):
    result = summary_dates("20250101", tmp_path)

    assert result == {"run_id": [], "run_type": []}


# =========================================================
# info_dates
# =========================================================

def test_info_dates_crab_and_other(tmp_path):
    catalog_dir = tmp_path
    file = catalog_dir / "RunCatalog_20250101.ecsv"

    file.write_text(
        """
# comment
12345,Crab
12346,Mrk421
12347,crab
"""
    )

    runs = [12345, 12346, 12347]

    result = info_dates("20250101", catalog_dir, runs)

    assert 12345 in result["crab"]
    assert 12347 in result["crab"]
    assert 12346 in result["other_source"]


def test_info_dates_no_file(tmp_path):
    result = info_dates("20250101", tmp_path, [1, 2])

    assert result == {"crab": [], "other_source": []}


def test_info_dates_filter_runs(tmp_path):
    catalog_dir = tmp_path
    file = catalog_dir / "RunCatalog_20250101.ecsv"

    file.write_text(
        """
12345,Crab
12346,Mrk421
"""
    )

    result = info_dates("20250101", catalog_dir, [12345])

    assert result["crab"] == [12345]
    assert result["other_source"] == []
