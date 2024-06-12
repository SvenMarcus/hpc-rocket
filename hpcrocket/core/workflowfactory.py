from typing import Any, Callable, Dict, Type

import hpcrocket.core.workflows as workflows
from hpcrocket.core.executor import CommandExecutor
from hpcrocket.core.filesystem import FilesystemFactory
from hpcrocket.core.launchoptions import (
    FinalizeOptions,
    ImmediateCommandOptions,
    JobBasedOptions,
    LaunchOptions,
    Options,
    WatchOptions,
)
from hpcrocket.core.workflows.workflow import Workflow


def _immediate_cmd_workflow(
    immediate_cmd_options: ImmediateCommandOptions,
    executor: CommandExecutor,
) -> Workflow:
    immediate_workflows = {
        ImmediateCommandOptions.Action.status: workflows.statusworkflow,
        ImmediateCommandOptions.Action.cancel: workflows.cancelworkflow,
    }

    workflow = immediate_workflows[immediate_cmd_options.action]
    return workflow(immediate_cmd_options, executor)


_SimpleWorkflowBuilder = Callable[[Any, CommandExecutor], Workflow]
_SimpleWorkFlowRegistry = Dict[Type[JobBasedOptions], _SimpleWorkflowBuilder]

_SimpleWorkflows: _SimpleWorkFlowRegistry = {
    ImmediateCommandOptions: _immediate_cmd_workflow,
    WatchOptions: workflows.watchworkflow,
}


def make_workflow(
    filesystem_factory: FilesystemFactory, executor: CommandExecutor, options: Options
) -> Workflow:
    if isinstance(options, FinalizeOptions):
        return workflows.finalizeworkflow(filesystem_factory, options)

    if isinstance(options, LaunchOptions):
        return workflows.launchworkflow(filesystem_factory, options, executor)
    else:
        option_type = type(options)
        monitoring_workflow_builder = _SimpleWorkflows[option_type]
        return monitoring_workflow_builder(options, executor)
