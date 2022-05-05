import os
import subprocess

import pytest


@pytest.mark.acceptance_test
def test__running_on_remote_server__exits_successfully() -> None:
    os.environ["TARGET_USER"] = "y0054816"
    os.environ["TARGET_HOST"] = "phoenix.hlr.rz.tu-bs.de"
    os.environ["TARGET_KEY"] = "~/.ssh/id_ed25519"
    os.environ["PROXY_USER"] = "marcus"
    os.environ["PROXY_HOST"] = "faramir.irmb.bau.tu-bs.de"
    os.environ["PROXY_KEY"] = "~/.ssh/marcus_boromir_ed25519"
    os.environ["REMOTE_SLURM_SCRIPT"] = "test/acceptance/slurm.job"
    config_path = "test/acceptance/launch_config.yml"

    exit_code = subprocess.call(["hpc-rocket", "launch", "--watch", config_path])

    assert exit_code == 0
