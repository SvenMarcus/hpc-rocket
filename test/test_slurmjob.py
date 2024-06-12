from typing import List

from hpcrocket.core.schedulers.base import TaskStatus
from hpcrocket.core.schedulers.slurmstatus import SlurmJobStatus


def job_with_state(state: str, sub_tasks: List[TaskStatus] = None) -> SlurmJobStatus:
    if sub_tasks is None:
        sub_tasks = []

    tasks = [TaskStatus("123456", "MyJob", state)]
    tasks.extend(sub_tasks)
    return SlurmJobStatus("123456", "MyJob", state, tasks=tasks)


def test__given_completed_job__completed_should_be_true():
    sut = job_with_state("COMPLETED")
    assert sut.is_completed is True


def test__given_running_slurm_job__running_should_be_true_and_pending_and_success_false():
    sut = job_with_state("RUNNING")
    assert sut.is_running is True
    assert sut.success is False


def test__given_canceled_slurm_job__running_and_pending_and_success_should_be_false():
    sut = job_with_state("CANCELED")

    assert sut.is_running is False
    assert sut.success is False


def test__given_pending_slurm_job__pending_should_be_true_and_running_and_success_should_be_false():
    sut = job_with_state("PENDING")

    assert sut.is_pending is True
    assert sut.is_running is False
    assert sut.success is False


def test__given_completed_slurm_job__pending_and_running_should_false_and_success_true():
    sut = job_with_state("COMPLETED")

    assert sut.is_pending is False
    assert sut.is_running is False
    assert sut.success is True


def test__given_failed_slurm_job__pending_and_running_and_success_should_be_false():
    sut = job_with_state("FAILED")

    assert sut.is_pending is False
    assert sut.is_running is False
    assert sut.success is False


def test__given_completed_job_with_failed_sub_task__success_should_be_false():
    sut = job_with_state(
        "COMPLETED",
        sub_tasks=[
            TaskStatus("123457", "SubTask", "COMPLETED"),
            TaskStatus("123458", "SubTask", "FAILED"),
        ],
    )

    assert sut.success is False
