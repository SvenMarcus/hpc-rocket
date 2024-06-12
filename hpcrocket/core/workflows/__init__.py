from pathlib import Path
from typing import List

from hpcrocket.core import schedulers
from hpcrocket.core.executor import CommandExecutor
from hpcrocket.core.filesystem import FilesystemFactory
from hpcrocket.core.launchoptions import (
    FinalizeOptions,
    ImmediateCommandOptions,
    LaunchOptions,
    WatchOptions,
)
from hpcrocket.core.schedulers.base import BatchJob
from hpcrocket.core.workflows.stages import (
    CancelStage,
    FinalizeStage,
    JobLoggingStage,
    LaunchStage,
    PrepareStage,
    StatusStage,
    WatchStage,
)
from hpcrocket.core.workflows.workflow import Stage, Workflow
from hpcrocket.ui import UI


def launchworkflow(
    filesystem_factory: FilesystemFactory,
    options: LaunchOptions,
    executor: CommandExecutor,
) -> Workflow:
    controller = schedulers.get(options.scheduler, executor)
    launch_stage = LaunchStage(controller, options.job)
    stages: List[Stage] = [
        PrepareStage(filesystem_factory, options.copy_files),
        launch_stage,
    ]

    if options.job_id_file:
        stages.append(JobLoggingStage(launch_stage, Path(options.job_id_file)))

    if options.watch:
        stages.append(
            WatchStage(
                launch_stage, options.poll_interval, options.continue_if_job_fails
            )
        )
        stages.append(
            FinalizeStage(
                filesystem_factory, options.collect_files, options.clean_files
            )
        )

    return Workflow(stages)


def statusworkflow(
    options: ImmediateCommandOptions, executor: CommandExecutor
) -> Workflow:
    controller = schedulers.get(options.scheduler, executor)
    return Workflow([StatusStage(controller, options.jobid)])


def cancelworkflow(
    options: ImmediateCommandOptions, executor: CommandExecutor
) -> Workflow:
    stage = CancelStage(
        schedulers.get(options.scheduler, executor), options.jobid
    )

    return Workflow([stage])


def watchworkflow(options: WatchOptions, executor: CommandExecutor) -> Workflow:
    class SimpleBatchJobProvider:
        def get_batch_job(self) -> BatchJob:
            return schedulers.job(options.scheduler, options.jobid, executor)

        def cancel(self, ui: UI) -> None:
            pass

    return Workflow([WatchStage(SimpleBatchJobProvider(), options.poll_interval)])


def finalizeworkflow(
    filesystem_factory: FilesystemFactory,
    options: FinalizeOptions,
) -> Workflow:
    return Workflow(
        [FinalizeStage(filesystem_factory, options.collect_files, options.clean_files)]
    )
