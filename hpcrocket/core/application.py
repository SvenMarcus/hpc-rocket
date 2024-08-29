from hpcrocket.core.errors import get_error_message
from hpcrocket.core.executor import CommandExecutor
from hpcrocket.core.filesystem import FilesystemFactory
from hpcrocket.core.launchoptions import Options
from hpcrocket.core.workflowfactory import make_workflow
from hpcrocket.core.workflows.workflow import Workflow
from hpcrocket.ui import UI


class Application:
    def __init__(
        self, executor: CommandExecutor, filesystem_factory: FilesystemFactory, ui: UI
    ) -> None:
        self._executor = executor
        self.fs_factory = filesystem_factory
        self._ui = ui
        self._workflow = Workflow([])

    def run(self, options: Options) -> int:
        try:
            return self._run_workflow(options)
        except Exception as err:
            self._ui.error(get_error_message(err))
            return 1

    def _run_workflow(self, options: Options) -> int:
        with self._executor as executor:
            self._workflow = make_workflow(self.fs_factory, executor, options)
            success = self._workflow.run(self._ui)
            return 0 if success else 1

    def cancel(self) -> int:
        self._workflow.cancel(self._ui)
        return 130
