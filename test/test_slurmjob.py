from typing import List

from hpcrocket.core.slurmrunner import SlurmJob, SlurmTask


def job_with_state(state: str, sub_tasks: List[SlurmTask] = None) -> SlurmJob:
    if sub_tasks is None:
        sub_tasks = []

    tasks = [SlurmTask("123456", "MyJob", state)]
    tasks.extend(sub_tasks)
    return SlurmJob("123456", "MyJob", state, tasks=tasks)


def test__given_completed_job__completed_should_be_true():
    sut = job_with_state("COMPLETED")
    assert sut.is_completed == True


def test__given_running_slurm_job__running_should_be_true_and_pending_and_success_false():
    sut = job_with_state("RUNNING")
    assert sut.is_running == True
    assert sut.success == False


def test__given_canceled_slurm_job__running_and_pending_and_success_should_be_false():
    sut = job_with_state("CANCELED")

    assert sut.is_running == False
    assert sut.success == False


def test__given_pending_slurm_job__pending_should_be_true_and_running_and_success_should_be_false():
    sut = job_with_state("PENDING")

    assert sut.is_pending == True
    assert sut.is_running == False
    assert sut.success == False


def test__given_completed_slurm_job__pending_and_running_should_false_and_success_true():
    sut = job_with_state("COMPLETED")

    assert sut.is_pending == False
    assert sut.is_running == False
    assert sut.success == True


def test__given_failed_slurm_job__pending_and_running_and_success_should_be_false():
    sut = job_with_state("FAILED")

    assert sut.is_pending == False
    assert sut.is_running == False
    assert sut.success == False


def test__given_completed_job_with_failed_sub_task__success_should_be_false():
    sut = job_with_state("COMPLETED",
                         sub_tasks=[
                             SlurmTask("123457", "SubTask", "COMPLETED"),
                             SlurmTask("123458", "SubTask", "FAILED"),
                         ])

    assert sut.success == False
