from pathlib import Path
from typing import Iterator

import pytest
from hpcrocket.ui import NullUI
from test.testdoubles.executor import SlurmJobExecutorSpy
from test.workflows.test_watchstage import BatchJobProviderSpy

from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.core.workflows.stages import JobLoggingStage


LOG_FILE = Path("job_log_file.txt")


@pytest.fixture(autouse=True)
def clean_files() -> Iterator[None]:
    yield
    LOG_FILE.unlink(missing_ok=True)


def test__job_logging_stage__when_run__writes_job_id_to_file() -> None:
    jobid = "420"

    executor = SlurmJobExecutorSpy(jobid=jobid)
    controller = SlurmController(executor)
    job_provider = BatchJobProviderSpy(controller, jobid)
    sut = JobLoggingStage(job_provider, LOG_FILE)

    success = sut(NullUI())

    assert success is True
    assert LOG_FILE.read_text() == jobid
