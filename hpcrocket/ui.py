from abc import ABC, abstractmethod
from typing import Any
from rich import box

from rich.console import RenderableType
from rich.live import Live
from rich.spinner import Spinner
from rich.table import Table

from hpcrocket.core.slurmbatchjob import SlurmJobStatus


class UI(ABC):

    @abstractmethod
    def update(self, job: SlurmJobStatus) -> None:
        pass

    @abstractmethod
    def error(self, text: str) -> None:
        pass

    @abstractmethod
    def info(self, text: str) -> None:
        pass

    @abstractmethod
    def success(self, text: str) -> None:
        pass

    @abstractmethod
    def launch(self, text: str) -> None:
        pass


class NullUI(UI):

    def update(self, job: SlurmJobStatus) -> None:
        pass

    def error(self, text: str) -> None:
        pass

    def info(self, text: str) -> None:
        pass

    def success(self, text: str) -> None:
        pass

    def launch(self, text: str) -> None:
        pass


class RichUI(UI):

    def __init__(self) -> None:
        self._rich_live: Live

    def __enter__(self) -> 'RichUI':
        self._rich_live = Live(
            Spinner("bouncingBar", ""),
            refresh_per_second=16)

        self._rich_live.start()
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        self._rich_live.stop()

    def update(self, job: SlurmJobStatus) -> None:
        self._rich_live.update(self._make_table(job))

    def error(self, text: str) -> None:
        self._rich_live.console.print(
            ":cross_mark:", text, style="bold red", emoji=True)

    def info(self, text: str) -> None:
        self._rich_live.console.print(
            ":information_source:", text, style="bold blue", emoji=True)

    def success(self, text: str) -> None:
        self._rich_live.console.print(
            ":heavy_check_mark: ", text, style="bold green", emoji=True)

    def launch(self, text: str) -> None:
        self._rich_live.console.print(":rocket: ", text, style="bold yellow", emoji=True)

    def _make_table(self, job: SlurmJobStatus) -> Table:
        table = Table(style="bold", box=box.MINIMAL)
        table.add_column("ID")
        table.add_column("Name")
        table.add_column("State")

        for task in job.tasks:
            last_column: RenderableType = task.state
            color = "grey42"
            if task.state == "RUNNING":
                color = "blue"
                last_column = Spinner("arc", task.state)
            elif task.state == "COMPLETED":
                color = "green"
                last_column = f":heavy_check_mark: {task.state}"
            elif task.state == "FAILED":
                color = "red"
                last_column = f":cross_mark: {task.state}"

            table.add_row(str(task.id), task.name, last_column, style=color)

        return table
