from typing import Callable, List

from hpcrocket.ui import UI


Stage = Callable[[UI], bool]


class Workflow:

    def __init__(self, stages: List[Stage]) -> None:
        self._stages = stages

    def run(self, ui: UI) -> bool:
        for stage in self._stages:
            result = stage(ui)
            if not result:
                return False

        return True