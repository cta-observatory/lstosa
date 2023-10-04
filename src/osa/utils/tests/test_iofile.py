from osa.utils.iofile import append_to_file
from osa.configs import options


def test_write_and_append_to_file(txt_file_test):
    assert txt_file_test.exists()
    assert txt_file_test.read_text() == "This is a test"
    options.simulate = False
    append_to_file(txt_file_test, "\nAnother line")
    options.simulate = True
    assert txt_file_test.read_text() == "This is a test\nAnother line"
