from typing import List

from hpcrocket.core.schedulers.base import TaskStatus
from hpcrocket.core.schedulers.slurmstatus import SlurmJobStatus

# This Job ID is used in the test slurm output files
DEFAULT_JOB_ID = "1603376"


def _get_lines(filename: str) -> List[str]:
    with open(filename, "r") as file:
        lines = file.readlines()
        return [line.strip() for line in lines]


def get_success_lines() -> List[str]:
    return _get_lines("test/slurmoutput/sacct_completed.txt")


def get_failed_lines() -> List[str]:
    return _get_lines("test/slurmoutput/sacct_completed_failed.txt")


def get_running_lines() -> List[str]:
    return _get_lines("test/slurmoutput/sacct_running.txt")


def running_slurm_job() -> SlurmJobStatus:
    return SlurmJobStatus(
        id=DEFAULT_JOB_ID,
        name="PyFluidsTest",
        state="RUNNING",
        tasks=[
            TaskStatus(f"{DEFAULT_JOB_ID}", "PyFluidsTest", "RUNNING"),
            TaskStatus(f"{DEFAULT_JOB_ID}.ext+", "extern", "RUNNING"),
            TaskStatus(f"{DEFAULT_JOB_ID}.0", "singularity", "COMPLETED"),
            TaskStatus(f"{DEFAULT_JOB_ID}.1", "singularity", "COMPLETED"),
            TaskStatus(f"{DEFAULT_JOB_ID}.2", "singularity", "RUNNING"),
        ],
    )


def completed_slurm_job() -> SlurmJobStatus:
    return SlurmJobStatus(
        id=DEFAULT_JOB_ID,
        name="PyFluidsTest",
        state="COMPLETED",
        tasks=[
            TaskStatus(f"{DEFAULT_JOB_ID}", "PyFluidsTest", "COMPLETED"),
            TaskStatus(f"{DEFAULT_JOB_ID}.bat+", "batch", "COMPLETED"),
            TaskStatus(f"{DEFAULT_JOB_ID}.ext+", "extern", "COMPLETED"),
            TaskStatus(f"{DEFAULT_JOB_ID}.0", "singularity", "COMPLETED"),
            TaskStatus(f"{DEFAULT_JOB_ID}.1", "singularity", "COMPLETED"),
            TaskStatus(f"{DEFAULT_JOB_ID}.2", "singularity", "COMPLETED"),
            TaskStatus(f"{DEFAULT_JOB_ID}.3", "singularity", "COMPLETED"),
        ],
    )
