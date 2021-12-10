
## Terminology

Like any discipline, graphs come with their own set of nomenclature. 
The following descriptions are intentionally simplified—more mathematically rigorous definitions can be found in any graph theory textbook.

`Graph theory` 

a language for networks/graphs. In summary, graph theory gives us a language for networks/graphs. It allows us to define graphs exactly and to quantify graph 
properties at all different levels. This quantification is likely to improve further since new graph measures are described regularly. 

`Graph` 

A data structure G = (V, E) where V and E are a set of vertices/nodes and edges.

`Vertex/Node` 

Represents a single entity such as a person or an object,

`Edge` 

Represents a relationship between two vertices (e.g., are these two vertices/nodes friends on a social network?).

`Directed Graph vs. Undirected Graph` 

Denotes whether the relationship represented by edges is symmetric or not 

`Weighted vs Unweighted Graph` 

In weighted graphs edges have a weight that could represent cost of traversing or a similarity score or a distance score

In unweighted graphs edges have no weight and simply show connections . example: course prerequisites

`Subgraph` 

A set of vertices and edges that are a subset of the full graph's vertices and edges.

`Degree` 
    
A vertex/node measurement quantifying the number of connected edges 

`Connected Component` 

A strongly connected subgraph, meaning that every vertex can reach the other vertices in the subgraph.

`Shortest Path` 
    
unweighted: The lowest number of edges required to traverse between two specific vertices/nodes unweighted
    
weighted: The edges with the least traversal cost between two specific vertices/nodes  
    
`diameter` 

The diameter of a graph is the largest of the shortest distances in that graph

`Betweenness centrality`

In graph theory, betweenness centrality (or "betweeness centrality") is a measure of centrality in a graph based on shortest paths. For every pair of vertices in a connected graph, there exists at least one shortest path between the vertices such that either the number of edges that the path passes through (for unweighted graphs) or the sum of the weights of the edges (for weighted graphs) is minimized. The betweenness centrality for each vertex is the number of these shortest paths that pass through the vertex.

`transitivity` (or `Global Clustering Coefficient` in the related literature)

The global clustering coefficient is based on triplets of nodes in a graph. A triplet consists of three connected nodes. 
A triangle therefore includes three closed triplets, one centered on each of the nodes 
(n.b. this means the three triplets in a triangle come from overlapping selections of nodes). 
The global clustering coefficient is the number of closed triplets (or 3 x triangles) over the total number of triplets (both open and closed)
  
`triangle clustering coeff` (or `Local Clustering Coefficient` in the related literature)

The local clustering coefficient for a node is then given by the proportion of links 
between the vertices within its neighborhood divided by the number of links that could possibly exist between them. 
The local clustering coefficient for a graph is the average LCC for all the nodes in that graph

`square clustering coeff` 

quantifies the abundance of connected squares in a graph (useful for bipartite networks where nodes cannot be connected in triangles)  
   
`node connectivity`

Node connectivity of a graph gives for the minimum number of nodes that need to be removed to separate 
the remaining nodes into two or more isolated subgraphs.
    
`edge connectivity`

Edge connectivity of a graph gives for the minimum number of edges that need to be removed to separate 
the remaining nodes into two or more isolated subgraphs.
    
`weisfeiler lehman graph hash` 

A Weisfeiler Lehman graph hash offers a way to quickly test for graph isomorphisms. Hashes are identical for isomorphic graphs 
and there are strong guarantees that non-isomorphic graphs will get different hashes. 
Two graphs are considered isomorphic if there is a mapping between the nodes of the graphs that preserves node adjacencies. 
That is, a pair of nodes may be connected by an edge in the first graph if and only if the corresponding pair of nodes in 
the second graph is also connected by an edge in the same way. 

![](https://github.com/moj-analytical-services/splink_graph/raw/master/notebooks/graph-isomorphism.png)

Graph 1 and Graph 2 above are isomorphic. 
The correspondance between nodes is illustrated by the node colors and numbers.

`node2vec "shallow" graph embedding` 

This is a per-cluster method for creating low-dimensional embeddings for nodes in a graph.
The aim is to create similar embeddings for neighboring nodes, with respect to the graph structure.

![](https://github.com/moj-analytical-services/splink_graph/raw/master/notebooks/node2vec_detail.png)


However this is a ‘transductive’ task where embeddings are created for ‘one graph’, 
limited to the given graph and that cannot be generalised.



---

Cluster Basic Stats

Gives node count, edge count, density and enumerates nodes per cluster.	

```python
"cluster_basic_stats(df, src =""src"", dst = ""dst"", cluster_id_colname = ""cluster_id"", 
weight_colname = ""weight"")
```

e.g. 
```python
x = cluster_basic_stats(linkagedf, src = 'linkageID_1', dst = 'linkageID_2', cluster_id_colname = 'ONS_ID', 
weight_colname = 'probability_score')
```
