from typing import Union

import hpcrocket.core.workflows as workflows
from hpcrocket.core.errors import get_error_message
from hpcrocket.core.executor import CommandExecutor
from hpcrocket.core.filesystem import FilesystemFactory
from hpcrocket.core.launchoptions import JobBasedOptions, LaunchOptions
from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.ui import UI


class WorkflowFactory:

    def __init__(self, filesystem_factory: FilesystemFactory) -> None:
        self._fs_factory = filesystem_factory

    def __call__(self, controller: SlurmController,
                 options: Union[LaunchOptions, JobBasedOptions]) -> workflows.Workflow:

        if isinstance(options, JobBasedOptions):
            return workflows.statusworkflow(controller, options)

        return workflows.launchworkflow(self._fs_factory, controller, options)


class Application:

    def __init__(self, executor: CommandExecutor, filesystem_factory: FilesystemFactory, ui: UI) -> None:
        self._executor = executor
        self._workflow_factory = WorkflowFactory(filesystem_factory)
        self._ui = ui

    def run(self, options: Union[LaunchOptions, JobBasedOptions]) -> int:
        try:
            return self._run_workflow(options)
        except Exception as err:
            self._ui.error(get_error_message(err))
            return 1

    def _run_workflow(self, options: Union[LaunchOptions, JobBasedOptions]) -> int:
        with self._executor as executor:
            controller = SlurmController(executor)
            workflow = self._workflow_factory(controller, options)
            success = workflow.run(self._ui)
            return 0 if success else 1

    def cancel(self) -> int:
        pass
