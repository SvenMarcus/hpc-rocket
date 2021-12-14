from typing import List

from hpcrocket.core.filesystem import FilesystemFactory
from hpcrocket.core.launchoptions import JobBasedOptions, LaunchOptions
from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.core.workflows.workflow import Stage, Workflow
from hpcrocket.core.workflows.stages import FinalizeStage, PrepareStage, LaunchStage, StatusStage, WatchStage


def launchworkflow(filesystem_factory: FilesystemFactory, controller: SlurmController, options: LaunchOptions) -> Workflow:
    launch_stage = LaunchStage(controller, options)
    stages: List[Stage] = [
        PrepareStage(filesystem_factory, options.copy_files),
        launch_stage
    ]

    if options.watch:
        stages.append(WatchStage(launch_stage, options.poll_interval))
        stages.append(FinalizeStage(filesystem_factory,
                      options.collect_files, options.clean_files))

    return Workflow(stages)


def statusworkflow(controller: SlurmController, options: JobBasedOptions):
    return Workflow([StatusStage(controller, options.jobid)])
