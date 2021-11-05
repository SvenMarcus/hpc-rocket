from hpcrocket.core.slurmbatchjob import SlurmJobStatus, SlurmTaskStatus

# This Job ID is used in the test slurm output files
DEFAULT_JOB_ID = "1603376"


def get_success_lines():
    with open("test/slurmoutput/sacct_completed.txt", "r") as file:
        lines = file.readlines()
        return lines


def get_failed_lines():
    with open("test/slurmoutput/sacct_completed_failed.txt", "r") as file:
        error_lines = file.readlines()
        return error_lines


def get_running_lines():
    with open("test/slurmoutput/sacct_running.txt", "r") as file:
        lines = file.readlines()
        return lines


def running_slurm_job():
    return SlurmJobStatus(
        id=DEFAULT_JOB_ID,
        name="PyFluidsTest",
        state="RUNNING",
        tasks=[
            SlurmTaskStatus(f"{DEFAULT_JOB_ID}", "PyFluidsTest", "RUNNING"),
            SlurmTaskStatus(f"{DEFAULT_JOB_ID}.ext+",  "extern", "RUNNING"),
            SlurmTaskStatus(f"{DEFAULT_JOB_ID}.0", "singularity", "COMPLETED"),
            SlurmTaskStatus(f"{DEFAULT_JOB_ID}.1", "singularity", "COMPLETED"),
            SlurmTaskStatus(f"{DEFAULT_JOB_ID}.2", "singularity", "RUNNING"),
        ]
    )


def completed_slurm_job():
    return SlurmJobStatus(
        id=DEFAULT_JOB_ID,
        name="PyFluidsTest",
        state="COMPLETED",
        tasks=[
            SlurmTaskStatus(f"{DEFAULT_JOB_ID}", "PyFluidsTest", "COMPLETED"),
            SlurmTaskStatus(f"{DEFAULT_JOB_ID}.bat+", "batch", "COMPLETED"),
            SlurmTaskStatus(f"{DEFAULT_JOB_ID}.ext+",  "extern", "COMPLETED"),
            SlurmTaskStatus(f"{DEFAULT_JOB_ID}.0", "singularity", "COMPLETED"),
            SlurmTaskStatus(f"{DEFAULT_JOB_ID}.1", "singularity", "COMPLETED"),
            SlurmTaskStatus(f"{DEFAULT_JOB_ID}.2", "singularity", "COMPLETED"),
            SlurmTaskStatus(f"{DEFAULT_JOB_ID}.3", "singularity", "COMPLETED")
        ]
    )
