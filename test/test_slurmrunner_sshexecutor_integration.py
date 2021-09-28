from test.application.launchoptions import main_connection
from test.testdoubles.paramiko_sshclient_mockutil import (
    get_blocking_channel_exit_status_ready_func, make_close,
    make_get_transport)
from unittest.mock import Mock, patch

import pytest
from hpcrocket.core.slurmbatchjob import (SlurmBatchJob, SlurmJobStatus,
                                          SlurmTaskStatus)
from hpcrocket.ssh.sshexecutor import SSHExecutor

patcher = patch("paramiko.SSHClient")


@pytest.fixture
def sbatch_sshclient_fake():
    patched = patcher.start()
    configure_sshclient_fake(patched, "test/slurmoutput/sbatch_submit.txt")

    yield patched.return_value

    patcher.stop()


@pytest.fixture
def sshclient_poll_running_job_fake():
    patched = patcher.start()
    configure_sshclient_fake(patched, "test/slurmoutput/sacct_running.txt")

    yield patched.return_value

    patcher.stop()


def configure_sshclient_fake(patched, cmd_output_file: str):
    patched.return_value.configure_mock(
        get_transport=make_get_transport(patched.return_value, active=True),
        close=make_close(patched.return_value),
        exec_command=make_exec_command(cmd_output_file)
    )


def test__when_calling_sbatch__should_return_job_id(sbatch_sshclient_fake):
    executor = SSHExecutor(main_connection())
    executor.connect()
    sut = SlurmBatchJob(executor, "myjob.job")

    jobid = sut.submit()

    assert jobid == "123456"


def test__when_polling_job__should_return_slurm_job_with_matching_data(sshclient_poll_running_job_fake):
    executor = SSHExecutor(main_connection())
    executor.connect()
    sut = SlurmBatchJob(executor, "myjob.job")
    sut.submit()

    actual = sut.poll_status()

    assert actual == SlurmJobStatus(
        id="1603376",
        name="PyFluidsTest",
        state="RUNNING",
        tasks=[
            SlurmTaskStatus("1603376", "PyFluidsTest", "RUNNING"),
            SlurmTaskStatus("1603376.ext+",  "extern", "RUNNING"),
            SlurmTaskStatus("1603376.0", "singularity", "COMPLETED"),
            SlurmTaskStatus("1603376.1", "singularity", "COMPLETED"),
            SlurmTaskStatus("1603376.2", "singularity", "RUNNING"),
        ]
    )


def make_exec_command(file: str):
    def exec_command(cmd: str):
        return [
            Mock("paramiko.channel.ChannelStdinFile"),
            stdout_with_channel_from_file(file),
            stderr()
        ]

    return exec_command


def stdout_with_channel_from_file(file: str):
    def readlines():
        with open(file) as f:
            return f.readlines()

    stdout = Mock()
    stdout.configure_mock(
        channel=Mock(
            exit_status=-99,
            exit_status_ready=get_blocking_channel_exit_status_ready_func(
                stdout)
        ),
        readlines=readlines
    )

    return stdout


def stderr():
    return Mock(
        channel=Mock(readlines=["error1", "error2"])
    )
