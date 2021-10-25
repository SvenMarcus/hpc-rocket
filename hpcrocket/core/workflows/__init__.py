from typing import List

from hpcrocket.core.filesystem import FilesystemFactory
from hpcrocket.core.launchoptions import LaunchOptions
from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.core.workflows.workflow import Stage, Workflow
from hpcrocket.core.workflows.stages import FinalizeStage, PrepareStage, LaunchStage


def launchworkflow(filesystem_factory: FilesystemFactory, controller: SlurmController, options: LaunchOptions) -> Workflow:
    stages: List[Stage] = [PrepareStage(filesystem_factory, options.copy_files), LaunchStage(controller, options)]
    if options.watch:
        stages.append(FinalizeStage(filesystem_factory, options.collect_files, options.clean_files))

    return Workflow(stages)
