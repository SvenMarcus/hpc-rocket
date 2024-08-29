from typing import List

from hpcrocket.core.schedulers.base import TaskStatus
from hpcrocket.core.schedulers.pbsstatus import PbsJobStatus

# This Job ID is used in the test pbs output files
DEFAULT_JOB_ID = "7"


def _get_lines(filename: str) -> List[str]:
    with open(filename, "r") as file:
        lines = file.readlines()
        return [line.strip() for line in lines]


def get_success_lines() -> List[str]:
    return _get_lines("test/pbsoutput/qstat_completed.txt")


def get_failed_lines() -> List[str]:
    return _get_lines("test/pbsoutput/qstat_completed_failed.txt")


def get_running_lines() -> List[str]:
    return _get_lines("test/pbsoutput/qstat_running.txt")


def running_pbs_job() -> PbsJobStatus:
    return PbsJobStatus(
        id=DEFAULT_JOB_ID,
        name="PyFluidsTest",
        state="RUNNING",
        tasks=[
            TaskStatus(f"{DEFAULT_JOB_ID}.pbs_master",
                       "checkout",
                       "R",
                   ),
        ],
    )


def completed_pbs_job() -> PbsJobStatus:
    return PbsJobStatus(
        id=DEFAULT_JOB_ID,
        name="PyFluidsTest",
        state="COMPLETED",
        tasks=[
            TaskStatus(f"{DEFAULT_JOB_ID}.pbs_master",
                       "checkout",
                       "F",
                   ),
        ],
    )
