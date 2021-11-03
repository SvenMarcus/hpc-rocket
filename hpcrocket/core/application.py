from typing import Union

import hpcrocket.core.workflows as workflows
from hpcrocket.core.errors import get_error_message
from hpcrocket.core.executor import CommandExecutor
from hpcrocket.core.filesystem import FilesystemFactory
from hpcrocket.core.launchoptions import JobBasedOptions, LaunchOptions
from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.core.workflows import workflow
from hpcrocket.ui import UI


ApplicationOptions = Union[LaunchOptions, JobBasedOptions]


class WorkflowFactory:

    def __init__(self, filesystem_factory: FilesystemFactory) -> None:
        self._fs_factory = filesystem_factory

    def __call__(self, controller: SlurmController, options: ApplicationOptions) -> workflows.Workflow:
        if isinstance(options, JobBasedOptions):
            return workflows.statusworkflow(controller, options)

        return workflows.launchworkflow(self._fs_factory, controller, options)


class Application:

    def __init__(self, executor: CommandExecutor, filesystem_factory: FilesystemFactory, ui: UI) -> None:
        self._executor = executor
        self._workflow_factory = WorkflowFactory(filesystem_factory)
        self._ui = ui
        self._workflow: workflows.Workflow

    def run(self, options: ApplicationOptions) -> int:
        try:
            return self._run_workflow(options)
        except Exception as err:
            self._ui.error(get_error_message(err))
            return 1

    def _run_workflow(self, options: ApplicationOptions) -> int:
        with self._executor as executor:
            self._workflow = self._get_workflow(executor, options)
            success = self._workflow.run(self._ui)
            return 0 if success else 1

    def _get_workflow(self, executor: CommandExecutor, options: ApplicationOptions) -> workflows.Workflow:
        controller = SlurmController(executor)
        return self._workflow_factory(controller, options)

    def cancel(self) -> int:
        self._workflow.cancel(self._ui)
        return 130
