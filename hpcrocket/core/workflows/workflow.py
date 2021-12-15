from typing import List, Optional

from hpcrocket.ui import UI

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol  # type: ignore


class Stage(Protocol):
    """
    An isolated step that is part of a larger Workflow
    """

    def __call__(self, ui: UI) -> bool:
        pass

    def cancel(self, ui: UI) -> None:
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
        for stage in self._stages:
            self._active_stage = stage

            if self._canceled:
                break

            result = stage(ui)
            if not result:
                return False

        return True

    def cancel(self, ui: UI) -> None:
        self._raise_if_not_started()
        self._active_stage.cancel(ui) # type: ignore
        self._canceled = True

    def _raise_if_not_started(self) -> None:
        if not self._active_stage:
            raise WorkflowNotStartedError()


class WorkflowNotStartedError(Exception):
    pass
