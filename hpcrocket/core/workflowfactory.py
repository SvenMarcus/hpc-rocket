from typing import Any, Callable, Dict, Type

import hpcrocket.core.workflows as workflows
from hpcrocket.core.filesystem import FilesystemFactory
from hpcrocket.core.launchoptions import (
    FinalizeOptions,
    JobBasedOptions,
    LaunchOptions,
    Options,
    ImmediateCommandOptions,
    WatchOptions,
)
from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.core.workflows.workflow import Workflow


def _immediate_cmd_workflow(
    controller: SlurmController,
    immediate_cmd_options: ImmediateCommandOptions,
) -> Workflow:
    immediate_workflows = {
        ImmediateCommandOptions.Action.status: workflows.statusworkflow,
        ImmediateCommandOptions.Action.cancel: workflows.cancelworkflow,
    }

    workflow = immediate_workflows[immediate_cmd_options.action]
    return workflow(controller, immediate_cmd_options)


_SimpleWorkflowBuilder = Callable[[SlurmController, Any], Workflow]
_SimpleWorkFlowRegistry = Dict[Type[JobBasedOptions], _SimpleWorkflowBuilder]

_SimpleWorkflows: _SimpleWorkFlowRegistry = {
    ImmediateCommandOptions: _immediate_cmd_workflow,
    WatchOptions: workflows.watchworkflow,
}


def make_workflow(
    filesystem_factory: FilesystemFactory,
    controller: SlurmController,
    options: Options,
) -> Workflow:
    if isinstance(options, LaunchOptions):
        return workflows.launchworkflow(filesystem_factory, controller, options)
    elif isinstance(options, FinalizeOptions):
        return workflows.finalizeworkflow(filesystem_factory, options)

    option_type = type(options)
    monitoring_workflow_builder = _SimpleWorkflows[option_type]
    return monitoring_workflow_builder(controller, options)
