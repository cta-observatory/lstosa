from osa.utils.iofile import read_from_file, append_to_file
from pathlib import Path


def test_write_to_file(txt_file_test):
    assert txt_file_test.exists()
    assert isinstance(txt_file_test, Path)


def test_read_from_file(txt_file_test):
    assert read_from_file(txt_file_test) == 'This is a test'


def test_append_to_file(txt_file_test):
    append_to_file(txt_file_test, '\nAnother line')
    assert read_from_file(txt_file_test) == 'This is a test\nAnother line'


