from hpcrocket.core.filesystem import FilesystemFactory
from hpcrocket.core.launchoptions import LaunchOptions
from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.core.workflows.workflow import Workflow
from hpcrocket.core.workflows.stages import PrepareStage, LaunchStage


def launchworkflow(filesystem_factory: FilesystemFactory, controller: SlurmController, options: LaunchOptions) -> Workflow:
    return Workflow([
        PrepareStage(filesystem_factory, options.copy_files),
        LaunchStage(controller, options)
    ])
