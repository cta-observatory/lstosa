from osa.utils.iofile import append_to_file
from pathlib import Path


def test_write_to_file(txt_file_test):
    assert txt_file_test.exists()
    assert isinstance(txt_file_test, Path)


def test_append_to_file(txt_file_test):
    append_to_file(txt_file_test, '\nAnother line')
    assert txt_file_test.read_text() == 'This is a test\nAnother line'
