import sys
from typing import TextIO

from hpcrocket.core.slurmbatchjob import SlurmJobStatus
from hpcrocket.ui import UI


class PrintLoggingUI(UI):
    def __init__(self) -> None:
        self._file: TextIO = None  # type: ignore[assignment]
        self._stdout_backup = sys.stdout

    def __enter__(self) -> "PrintLoggingUI":
        self._file = open("test/test_output.txt", "w+")
        sys.stdout = self._file
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        sys.stdout = self._stdout_backup
        self._file.close()

    def launch(self, text: str) -> None:
        print(text, file=self._file)

    def info(self, text: str) -> None:
        print(text, file=self._file)

    def error(self, text: str) -> None:
        print(text, file=self._file)

    def success(self, text: str) -> None:
        print(text, file=self._file)

    def update(self, job: SlurmJobStatus) -> None:
        print(job, file=self._file)
