from pathlib import Path


def test_failed_history():
    from osa.veto import failed_history
    good_history_file = Path('./extra/history_files/sequence_LST1_04183.history')
    bad_history_file = Path('./extra/history_files/sequence_LST1_04183_failed.history')
    good_history_failed = failed_history(good_history_file)
    bad_history_failed = failed_history(bad_history_file)
    assert good_history_failed is False
    assert bad_history_failed is True


def test_get_veto_list(sequence_list):
    from osa.veto import get_veto_list
    veto_list = get_veto_list(sequence_list)
    assert not veto_list
