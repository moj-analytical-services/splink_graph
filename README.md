
![](https://img.shields.io/badge/spark-%3E%3D2.4.5-orange) ![](https://img.shields.io/badge/pyarrow-%3C%3D%200.14.1-blue)

# splink_graph




![](https://github.com/moj-analytical-services/splink_graph/raw/master/notebooks/splink_graph300x297.png)

---



`splink_graph` is a small graph utility library in the Apache Spark environment, that works with graph data structures based on the `graphframe` package,
such as the ones created from the outputs of data linking processes (candicate pair results) of ![splink](https://github.com/moj-analytical-services/splink)  



The main aim of `splink_graph` is to offer a small set of functions that work on top of established graph packages like `graphframes` and `networkx`  , that can help with
the process of data linkage




---


## Using Pandas UDFs in Python: prerequisites


This package uses Pandas UDFs for certain functionality.Pandas UDFs are built on top of Apache Arrow and bring 
the best of both worlds: the ability to define low-overhead, high-performance UDFs entirely in Python.

With Apache Arrow, it is possible to exchange data directly between JVM and Python driver/executors with near-zero (de)serialization cost.
However there are some things to be aware of if you want to use these functions.
Since Arrow 0.15.0, a change in the binary IPC format requires an environment variable to be compatible with previous versions of Arrow <= 0.14.1. This is only necessary to do for PySpark users with versions 2.3.x and 2.4.x that have manually upgraded PyArrow to 0.15.0. The following can be added to conf/spark-env.sh to use the legacy Arrow IPC format:

    ARROW_PRE_0_15_IPC_FORMAT=1`

Another way is to put the following on spark .config

    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT", "1")


This will instruct PyArrow >= 0.15.0 to use the legacy IPC format with the older Arrow Java that is in Spark 2.3.x and 2.4.x. Not setting this environment variable will lead to a similar error as described in [SPARK-29367](https://issues.apache.org/jira/browse/SPARK-29367) when running pandas_udfs or toPandas() with Arrow enabled.


So all in all : either PyArrow needs to be at most in version 0.14.1 or if that cannot happen the above settings need to be be active.






---


## Terminology

Like any discipline, graphs come with their own set of nomenclature. 
The following descriptions are intentionally simplified—more mathematically rigorous definitions can be found in any graph theory textbook.

`Graph` 

    — A data structure G = (V, E) where V and E are a set of vertices/nodes and edges.

`Vertex/Node` 

    — Represents a single entity such as a person or an object,

`Edge` 

    — Represents a relationship between two vertices (e.g., are these two vertices friends on a social network?).

`Directed Graph vs. Undirected Graph` 

    — Denotes whether the relationship represented by edges is symmetric or not 

`Weighted vs Unweighted Graph` 

     — In weighted graphs edges have a weight that could represent cost of traversing or a similarity score or a distance score

     — In unweighted graphs edges have no weight and simply show connections . example: course prerequisites

`Subgraph` 

    — A set of vertices and edges that are a subset of the full graph's vertices and edges.

`Degree` 
    
    — A vertex/node measurement quantifying the number of connected edges 

`Connected Component` 

    — A strongly connected subgraph, meaning that every vertex can reach the other vertices in the subgraph.

`Shortest Path` 
    
    — The lowest number of edges required to traverse between two specific vertices/nodes.





---

## Contributing

Feel free to contribute by 

 * Forking the repository to suggest a change, and/or
 * Starting an issue.