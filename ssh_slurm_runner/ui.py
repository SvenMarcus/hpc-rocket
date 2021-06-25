from abc import ABC, abstractmethod

from rich.live import Live
from rich.spinner import Spinner
from rich.table import Table

from ssh_slurm_runner.slurmrunner import SlurmJob


class UI(ABC):

    @abstractmethod
    def update(self, job: SlurmJob) -> None:
        pass


class RichUI(UI):

    def __init__(self) -> None:
        self._rich_live: Live = None

    def __enter__(self):
        self._rich_live = Live(
            Spinner("bouncingBar", "Launching job"),
            refresh_per_second=8)

        self._rich_live.start()
        return self

    def __exit__(self, *args, **kwargs):
        self._rich_live.stop()

    def update(self, job: SlurmJob) -> None:
        self._rich_live.update(self._make_table(job))

    def _make_table(self, job: SlurmJob) -> Table:
        title = f"Job {job.id}"

        table = Table(title=title, style="bold")
        table.add_column("ID")
        table.add_column("Name")
        table.add_column("State")

        for task in job.tasks:
            last_column = task.state
            if task.state == "RUNNING":
                last_column = Spinner("bouncingBar", task.state)
            table.add_row(str(task.id), task.name, last_column)

        return table
