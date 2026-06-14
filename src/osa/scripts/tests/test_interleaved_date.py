import pytest

from osa.scripts.interleaved_date import (
    clean_path,
    load_config,
    summary_dates,
    info_dates,
    find_interleaved,
)


def test_clean_path_replace_base(tmp_path):
    base = str(tmp_path)
    result = clean_path("%(BASE)s/data", base)
    assert result == str(tmp_path / "data")


def test_clean_path_without_base(tmp_path):
    base = str(tmp_path)
    result = clean_path("/my/path", base)
    assert result == "/my/path"


def test_load_config_ok(tmp_path):
    cfg = tmp_path / "test.cfg"

    cfg.write_text(
        f"""
[LST1]
BASE={tmp_path}
DL1_DIR=%(BASE)s/DL1
RUN_SUMMARY_DIR=%(BASE)s/RunSummary
RUN_CATALOG=%(BASE)s/RunCatalog
OSA_DIR=%(BASE)s/OSA
"""
    )

    dl1_dir, summary, catalog, osa = load_config(cfg)

    assert dl1_dir == str(tmp_path / "DL1")
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


def test_summary_dates_ok(tmp_path):
    file = tmp_path / "RunSummary_20260115.ecsv"

    file.write_text(
        """
# comment
12345,xxx,DATA
12346,xxx,CALIB
"""
    )

    result = summary_dates("20260115", tmp_path)

    assert 12345 in result["run_id"]
    assert "DATA" in result["run_type"]


def test_summary_dates_missing_file(tmp_path):
    result = summary_dates("20260115", tmp_path)
    assert result == {"run_id": [], "run_type": []}


def test_summary_dates_bad_rows(tmp_path):
    file = tmp_path / "RunSummary_20260115.ecsv"
    file.write_text("badline\n12345\n")

    result = summary_dates("20260115", tmp_path)
    assert isinstance(result, dict)


def test_info_dates_crab_and_other(tmp_path):
    file = tmp_path / "RunCatalog_20260115.ecsv"

    file.write_text(
        """
# comment
12345,Crab
12346,Other
"""
    )

    runs = [12345, 12346]

    result = info_dates("20260115", tmp_path, runs)

    assert result["crab"] == [12345]
    assert result["other_source"] == [12346]


def test_info_dates_no_file(tmp_path):
    result = info_dates("20260115", tmp_path, [1, 2])
    assert result == {"crab": [], "other_source": []}


def test_info_dates_bad_rows(tmp_path):
    file = tmp_path / "RunCatalog_20260115.ecsv"
    file.write_text("badline\n12345\n")

    result = info_dates("20260115", tmp_path, [12345])
    assert isinstance(result, dict)


def test_find_interleaved_basic(tmp_path):

    base = tmp_path / "DL1"
    base.mkdir()

    date_dir = base / "20260215"
    (date_dir / "v1" / "interleaved").mkdir(parents=True)

    paths, dates = find_interleaved("20260220", str(base))

    assert len(paths) == 1
    assert dates == ["20260215"]


def test_find_interleaved_outside_range(tmp_path):

    base = tmp_path / "DL1"
    base.mkdir()

    date_dir = base / "20240101"
    (date_dir / "v1" / "interleaved").mkdir(parents=True)

    paths, dates = find_interleaved("20260220", str(base))

    assert paths == []
    assert dates == []


def test_find_interleaved_invalid_date():
    with pytest.raises(SystemExit):
        find_interleaved("bad_date", str(tmp_path))


def test_find_interleaved_no_dirs(tmp_path):

    base = tmp_path / "DL1"
    base.mkdir()

    paths, dates = find_interleaved("20260201", str(base))

    assert paths == []
    assert dates == []


def test_summary_and_info_integration(tmp_path):

    (tmp_path / "RunSummary_20260115.ecsv").write_text(
        "12345,xxx,DATA\n12346,xxx,DATA\n"
    )

    (tmp_path / "RunCatalog_20260115.ecsv").write_text(
        "12345,Crab\n12346,Other\n"
    )

    summary = summary_dates("20260115", tmp_path)

    data_runs = [
        r for r, t in zip(summary["run_id"], summary["run_type"])
        if t == "DATA"
    ]

    entry = info_dates("20260115", tmp_path, data_runs)

    assert entry["crab"] == [12345]
    assert entry["other_source"] == [12346]


def test_main_execution(tmp_path, monkeypatch):

    import sys
    import runpy
    from osa.scripts import interleaved_date

    cfg = tmp_path / "test.cfg"
    cfg.write_text(
        f"""
[LST1]
BASE={tmp_path}
DL1_DIR=%(BASE)s/DL1
RUN_SUMMARY_DIR=%(BASE)s/RunSummary
RUN_CATALOG=%(BASE)s/RunCatalog
OSA_DIR=%(BASE)s/OSA
"""
    )

    (tmp_path / "DL1").mkdir()
    (tmp_path / "RunSummary").mkdir()
    (tmp_path / "RunCatalog").mkdir()

    monkeypatch.setattr(
        sys, "argv",
        ["script", "20260220", "-c", str(cfg)]
    )

    monkeypatch.setattr(
        interleaved_date,
        "find_interleaved",
        lambda date, path: ([], [])
    )

    runpy.run_module(interleaved_date.__name__, run_name="__main__")

    recordfile = tmp_path / "OSA" / "interleaved_cleanup_sh" / "entries_rm_202602.sh"

    assert recordfile.exists()
    assert recordfile.read_text().startswith("#!/bin/bash")
