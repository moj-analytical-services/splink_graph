# splink_graph


![](https://img.shields.io/badge/spark-%3E%3D2.4.5-orange)

![](https://img.shields.io/badge/pyarrow-%3C%3D%200.14.1-blue)


![](https://github.com/moj-analytical-services/splink_graph/raw/master/notebooks/splink_graph300x297.png)

---







`splink_graph` is a small graph utility library in the Apache Spark environment, that works with graph data structures based on the `graphframe` package,
such as the ones created from the outputs of data linking processes (candicate pair results) of ![splink](https://github.com/moj-analytical-services/splink])  





The main aim of `splink_graph` is to offer a small set of functions that work on top of established graph packages like `graphframes` and `networkx`  , that can help with
the process of data linkage



---

## Contributing

Feel free to contribute by 

 * Forking the repository to suggest a change, and/or
 * Starting an issue.


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




