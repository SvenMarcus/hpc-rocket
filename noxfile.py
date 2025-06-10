import nox


@nox.session
def lint(session: nox.Session) -> None:
    session.install("ruff")
    session.run("ruff", "check", "hpcrocket")


@nox.session(python=["3.9", "3.10", "3.11", "3.12", "3.13"])
def test(session: nox.Session) -> None:
    install_dev_dependencies(session)
    session.run("pytest", "-vv", "-m", "not integration and not acceptance", "test")


@nox.session(python=["3.9", "3.10", "3.11", "3.12", "3.13"])
def mypy(session: nox.Session) -> None:
    install_dev_dependencies(session)
    session.install("mypy")
    session.run("mypy", "--non-interactive", "--install-types", "hpcrocket")
    session.run("mypy", "--strict", "hpcrocket")


@nox.session(python=["3.9", "3.10", "3.11", "3.12", "3.13"])
def test_integration(session: nox.Session) -> None:
    install_dev_dependencies(session)
    session.run("pytest", "-vv", "-m", "integration", "test")


def install_dev_dependencies(session: nox.Session) -> None:
    pyproject = nox.project.load_toml("pyproject.toml")
    dev_dependencies = nox.project.dependency_groups(pyproject, "dev")
    session.install(*dev_dependencies, ".")


nox.options.sessions = ["lint", "test", "mypy"]
