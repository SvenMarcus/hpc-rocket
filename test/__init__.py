import pytest

pytest.register_assert_rewrite("test.testdoubles.sshclient")
pytest.register_assert_rewrite("test.testdoubles.executor")
pytest.register_assert_rewrite("test.slurm_assertions")
