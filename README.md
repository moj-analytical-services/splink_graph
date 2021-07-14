
![](https://img.shields.io/badge/spark-%3E%3D2.4.x-orange) ![](https://img.shields.io/github/languages/top/moj-analytical-services/splink_graph) ![](https://img.shields.io/pypi/v/splink_graph) ![Downloads](https://pepy.tech/badge/splink-graph)

# splink_graph




![](https://github.com/moj-analytical-services/splink_graph/raw/master/notebooks/splink_graph300x297.png)

---


`splink_graph` is a small graph utility library meant to be used in the Apache Spark environment, that works with graph data structures 
such as the ones created from the outputs of data linking processes (candicate pair results) of ![splink](https://github.com/moj-analytical-services/splink) 

The main aim of `splink_graph` is to offer a small set of functions that work on top of established graph packages like `graphframes` and `networkx`  , that can help with the process of graph analysis of the output of probabilistic data linkage tools.

Calculations performed per cluster in a parallel manner thanks to the underlying help from `pyArrow`

---
## How to Install : 
For dependencies and other important info so you can run these functions without an issue please consult
`INSTALL.md` on this repo

## Functionality offered :

For a primer on the terminology used please look at `TERMINOLOGY.md` file in this repo


####  Cluster metrics

Cluster metrics usually have as an input a spark edgelist dataframe that also includes the component_id (cluster_id) where the edge is in.
The output is a row of one or more metrics per cluster


Cluster metrics currently offered: 

- diameter (largest shortest distance in a cluster)
- transitivity (or Global Clustering Coefficient in the related literature)
- cluster triangle clustering coeff (or Local Clustering Coefficient in the related literature)
- cluster square clustering coeff (useful for bipartite networks)
- cluster node connectivity 
- cluster edge connectivity
- cluster efficiency
- cluster modularity
- cluster avg edge betweenness
- cluster weisfeiler lehman graphhash (in order to quickly test for graph homomorphisms)

Cluster metrics are really helpful at finding the needle (of for example clusters with possible linking errors) in the 
haystack (whole set of clusters after the data linking process)

---

####  Node metrics

Node metrics  have as an input a spark edgelist dataframe that also includes the component_id (cluster_id) where the edge is in.
The output is a row of one or more metrics per node

Node metrics curretnly offered: 

- Eigenvector Centrality 
- Harmonic centrality

---

####  Edge metrics

Edge metrics  have as an input a spark edgelist dataframe that also includes the component_id (cluster_id) where the edge is in.
The output is a row of one or more metrics per edge

Edge metrics curretnly offered: 

- Edge Betweeness
- Bridge Edges


---


## Contributing

Feel free to contribute by 

 * Forking the repository to suggest a change, and/or
 * Starting an issue.
