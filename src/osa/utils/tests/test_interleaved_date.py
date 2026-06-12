import pytest

from osa.utils.interleaved_date import (
    clean_path,
    load_config,
    summary_dates,
    info_dates,
    find_interleaved,
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
# summary_dates
# =========================================================

def test_summary_dates_ok(tmp_path):
    file = tmp_path / "RunSummary_20250101.ecsv"

    file.write_text(
        """
# comment
12345,xxx,DATA
12346,xxx,CALIB
"""
    )

    result = summary_dates("20250101", tmp_path)

    assert 12345 in result["run_id"]
    assert "DATA" in result["run_type"]


def test_summary_dates_missing_file(tmp_path):
    result = summary_dates("20250101", tmp_path)

    assert result == {"run_id": [], "run_type": []}


# =========================================================
# info_dates
# =========================================================

def test_info_dates_crab_and_other(tmp_path):
    file = tmp_path / "RunCatalog_20250101.ecsv"

    file.write_text(
        """
# comment
12345,Crab
12346,Mrk421
12347,crab
"""
    )

    runs = [12345, 12346, 12347]

    result = info_dates("20250101", tmp_path, runs)

    assert 12345 in result["crab"]
    assert 12347 in result["crab"]
    assert 12346 in result["other_source"]


def test_info_dates_no_file(tmp_path):
    result = info_dates("20250101", tmp_path, [1, 2])

    assert result == {"crab": [], "other_source": []}


def test_info_dates_filter_runs(tmp_path):
    file = tmp_path / "RunCatalog_20250101.ecsv"

    file.write_text(
        """
12345,Crab
12346,Mrk421
"""
    )

    result = info_dates("20250101", tmp_path, [12345])

    assert result["crab"] == [12345]
    assert result["other_source"] == []


# =========================================================
# find_interleaved
# =========================================================

def test_find_interleaved_basic(tmp_path):

    base = tmp_path / "DL1"
    base.mkdir()

    date_dir = base / "20250115"
    inter_dir = date_dir / "v1" / "interleaved"
    inter_dir.mkdir(parents=True)

    from osa.utils import interleaved_date
    interleaved_date.data_dirs = [str(base)]

    paths, dates = find_interleaved("20250120")

    assert len(paths) == 1
    assert "interleaved" in paths[0]
    assert dates == ["20250115"]


def test_find_interleaved_outside_range(tmp_path):

    base = tmp_path / "DL1"
    base.mkdir()

    date_dir = base / "20230101"
    inter_dir = date_dir / "v1" / "interleaved"
    inter_dir.mkdir(parents=True)

    from osa.utils import interleaved_date
    interleaved_date.data_dirs = [str(base)]

    paths, dates = find_interleaved("20250120")

    assert paths == []
    assert dates == []


def test_find_interleaved_invalid_date():

    with pytest.raises(SystemExit):
        find_interleaved("bad_date")


# =========================================================
# INTEGRATION TEST (MUY IMPORTANTE PARA COVERAGE)
# =========================================================

def test_summary_and_info_integration(tmp_path):

    # RunSummary
    (tmp_path / "RunSummary_20250101.ecsv").write_text(
        "12345,xxx,DATA\n12346,xxx,DATA\n"
    )

    # RunCatalog
    (tmp_path / "RunCatalog_20250101.ecsv").write_text(
        "12345,Crab\n12346,Mrk421\n"
    )

    from osa.utils.interleaved_date import summary_dates, info_dates

    summary = summary_dates("20250101", tmp_path)

    data_runs = [
        r for r, t in zip(summary["run_id"], summary["run_type"])
        if t == "DATA"
    ]

    entry = info_dates("20250101", tmp_path, data_runs)

    assert entry["crab"] == [12345]
    assert entry["other_source"] == [12346]
