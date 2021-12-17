from typing import List, Optional

from hpcrocket.typesafety import get_or_raise
from hpcrocket.ui import UI

try:
    from typing import Protocol
except ImportError:  # pragma: no cover
    from typing_extensions import Protocol  # type: ignore


class Stage(Protocol):
    """
    An isolated step that is part of a larger Workflow
    """

    def __call__(self, ui: UI) -> bool:
        """
        Starts running the stage. Returns true if the stage completed successfully.

        Args:
            ui (UI): The ui to send output to.

        Returns:
            bool
        """
        pass

    def cancel(self, ui: UI) -> None:
        """
        Cancels the stage.

        Args:
            ui (UI): The ui to send output to.
        """
        pass


class Workflow:
    """
    Represents a series of isolated steps that are executed in order
    """

    def __init__(self, stages: List[Stage]) -> None:
        self._stages = stages
        self._active_stage: Optional[Stage] = None
        self._canceled = False

    def run(self, ui: UI) -> bool:
        """
        Runs the workflow. Returns true if all stages completed successfully.

        Args:
            ui (UI): The ui to send output to.

        Returns:
            bool
        """
        for stage in self._stages:
            self._active_stage = stage

            if self._canceled:
                break

            result = stage(ui)
            if not result:
                return False

        return True

    def cancel(self, ui: UI) -> None:
        """
        Cancels the workflow.

        Args:
            ui (UI): The ui to send output to.

        Raises:
            WorkflowNotStartedError: If the workflow is canceled before it was started.
        """
        active_stage = get_or_raise(self._active_stage, WorkflowNotStartedError)
        active_stage.cancel(ui)
        self._canceled = True


class WorkflowNotStartedError(Exception):
    pass
