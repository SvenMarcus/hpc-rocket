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
