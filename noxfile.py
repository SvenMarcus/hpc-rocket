import nox


@nox.session
def lint(session: nox.Session) -> None:
    session.install("flake8")
    session.run("flake8", "--max-line-length=125", "hpcrocket")


@nox.session(python=["3.7", "3.8", "3.9", "3.10"])
def test(session: nox.Session) -> None:
    session.install("-r", "requirements.txt")
    session.install("pytest")
    session.install("pytest-cov")
    session.install("pytest-timeout")

    session.run("pytest", "-vv", "-m", "not integration", "test")


@nox.session(python=["3.7", "3.8", "3.9", "3.10"])
def mypy(session: nox.Session) -> None:
    session.install("-r", "requirements.txt")
    session.install("mypy")
    session.run("mypy", "--non-interactive", "--install-types", "hpcrocket")
    session.run("mypy", "--strict", "hpcrocket")


@nox.session(python=["3.7", "3.8", "3.9", "3.10"])
def test_integration(session: nox.Session) -> None:
    session.install("-r", "requirements.txt")
    session.install("-r", "testrequirements.txt")

    session.run("pytest", "-vv", "-m", "integration", "test")

nox.options.sessions = ["lint", "test", "mypy"]