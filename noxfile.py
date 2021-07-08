import nox
@nox.session
@nox.parametrize("pyspark", ["2.4.5", "3.0.1"])
@nox.parametrize("pandas", ["0.25.3", "1.0.1"])
def tests(session,pandas, pyspark):
    session.run("python", "-m", "pip", "install", "--upgrade", "pip")
    session.install('cmake')
    session.install('pytest')
    session.install(f"pandas=={pandas}")
    session.install('numpy==1.19.5', "--no-deps")
    if pyspark=="2.4.5":
        session.install('pyarrow==0.14.1')
    else:
        session.install('pyarrow==2.0.0')
    session.install("networkx==2.5.1")
    session.install(f"pyspark=={pyspark}")
    session.run("pytest", "-v","-W", "ignore::DeprecationWarning")
