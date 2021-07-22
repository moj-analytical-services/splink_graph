import nox

@nox.session()
@nox.parametrize(
    "python,pyspark",
    [
        (python, pyspark)
        for python in ("3.7", "3.8")
        for pyspark in ("2.4.5", "3.0.1","3.1.1")
        if (python, pyspark) != ("3.8", "2.4.5")
    ],)
def tests(session, pyspark):

    session.run("python", "-m", "pip", "install", "--upgrade", "pip")
    session.install("cmake")
    session.install("pytest")
    session.install("pytest-order")
    session.install("pandas==0.25.3")
    session.install("numpy==1.19.5", "--no-deps")
    session.install("scipy==1.6.0", "--no-deps")
    session.install("node2vec==0.4.3")
    session.install("graphframes==0.6.0")

    if pyspark == "2.4.5":
        session.install("pyarrow==0.14.1", "--no-deps")
    else:
        session.install("pyarrow==2.0.0", "--no-deps")

    session.install("networkx==2.5.1")
    session.install(f"pyspark=={pyspark}")
    session.run("pytest", "-x","-v", "-W", "ignore::DeprecationWarning")