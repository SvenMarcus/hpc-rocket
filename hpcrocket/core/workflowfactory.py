from typing import Any, Callable, Dict, Type

import hpcrocket.core.workflows as workflows
from hpcrocket.core.filesystem import FilesystemFactory
from hpcrocket.core.launchoptions import (JobBasedOptions, LaunchOptions,
                                          Options, SimpleJobOptions,
                                          WatchOptions)
from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.core.workflows.workflow import Workflow


def _simple_option_workflow_builder(controller: SlurmController,
                                    simple_options: SimpleJobOptions) -> Workflow:
    if simple_options.action == SimpleJobOptions.Action.status:
        return workflows.statusworkflow(controller, simple_options)

    return workflows.cancelworkflow(controller, simple_options)


class WorkflowFactory:

    SimpleWorkflowBuilder = Callable[[SlurmController, Any], Workflow]
    SimpleWorkflows: Dict[Type[JobBasedOptions],
                          SimpleWorkflowBuilder] = {
        SimpleJobOptions: _simple_option_workflow_builder,
        WatchOptions: workflows.watchworkflow
    }

    def __init__(self, filesystem_factory: FilesystemFactory) -> None:
        self._fs_factory = filesystem_factory

    def __call__(self, controller: SlurmController, options: Options) -> Workflow:
        if isinstance(options, LaunchOptions):
            return workflows.launchworkflow(self._fs_factory, controller, options)

        option_type = type(options)
        monitoring_workflow_builder = self.SimpleWorkflows[option_type]
        return monitoring_workflow_builder(controller, options)
