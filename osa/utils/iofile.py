"""Handle the writing, reading and appending strings to files."""

import filecmp
import logging
import pathlib
from os import remove, rename

from osa.configs import options
from osa.utils.logging import myLogger

log = myLogger(logging.getLogger(__name__))

__all__ = [
    "write_to_file",
    "append_to_file",
]


def write_to_file(file: pathlib.Path, content: str):
    """Check if the file already exists and write the content in it."""
    file_temp = f"{file}.tmp"
    try:
        with open(file_temp, "w") as file_handle:
            file_handle.write(f"{content}")
    except (IOError, OSError) as e:
        log.exception(f"{e.strerror} {e.filename}")

    if file.exists() and file.is_file():
        if filecmp.cmp(file, file_temp):
            remove(file_temp)
            return False

        if options.simulate:
            remove(file_temp)
            log.debug(f"SIMULATE File {file_temp} would replace {file}." f"Deleting {file_temp}")
        else:
            try:
                rename(file_temp, file)
            except (IOError, OSError) as e:
                log.exception(f"{e.strerror} {e.filename}")
    elif options.simulate:
        log.debug(f"SIMULATE File {file_temp} would be written as {file}. Deleting {file_temp}")
    else:
        rename(file_temp, file)
    return True


def append_to_file(file: pathlib.Path, content: str) -> None:
    """
    Check if the file already exists and write the content in it.

    Parameters
    ----------
    file: pathlib.Path
        The file to write in.
    content: str
        The content to write in the file.
    """
    if file.exists() and file.is_file():
        if options.simulate:
            log.debug(f"SIMULATE File {file} would be appended")
        else:
            with open(file, "a") as file_handle:
                try:
                    file_handle.write(content)
                except IOError as e:
                    log.exception(f"{e.strerror} {e.filename}")
    else:
        write_to_file(file, content)
