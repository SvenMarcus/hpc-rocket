from hpcrocket.core.executor import CommandExecutor

from hpcrocket.core.schedulers.pbscontroller import PbsController
from hpcrocket.core.schedulers.base import BatchJob, Scheduler

from hpcrocket.core.schedulers.slurmcontroller import SlurmController


def get(scheduler: str, executor: CommandExecutor) -> Scheduler:
    """
    Returns a scheduler based on the given scheduler name.
    """
    if scheduler == "slurm":
        return SlurmController(executor)
    elif scheduler == "pbs":
        return PbsController(executor)
    else:
        raise ValueError(f"Unknown scheduler: {scheduler}")


def job(scheduler: str, jobid: str, executor: CommandExecutor) -> BatchJob:
    """
    Returns a Job object based on the given scheduler name and jobid.
    """
    controller = get(scheduler, executor)
    return BatchJob(controller, jobid)
