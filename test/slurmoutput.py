from hpcrocket.core.slurmbatchjob import SlurmJobStatus, SlurmTaskStatus


def get_success_lines():
    with open("test/slurmoutput/sacct_completed.txt", "r") as file:
        lines = file.readlines()
        return lines


def get_error_lines():
    with open("test/slurmoutput/sacct_completed_failed.txt", "r") as file:
        error_lines = file.readlines()
        return error_lines


def get_running_lines():
    with open("test/slurmoutput/sacct_running.txt", "r") as file:
        lines = file.readlines()
        return lines


def completed_slurm_job():
    return SlurmJobStatus(
        id="1603353",
        name="PyFluidsTest",
        state="COMPLETED",
        tasks=[
            SlurmTaskStatus("1603353", "PyFluidsTest", "COMPLETED"),
            SlurmTaskStatus("1603353.bat+", "batch", "COMPLETED"),
            SlurmTaskStatus("1603353.ext+",  "extern", "COMPLETED"),
            SlurmTaskStatus("1603353.0", "singularity", "COMPLETED"),
            SlurmTaskStatus("1603353.1", "singularity", "COMPLETED"),
            SlurmTaskStatus("1603353.2", "singularity", "COMPLETED"),
            SlurmTaskStatus("1603353.3", "singularity", "COMPLETED")
        ]
    )
