
![](https://img.shields.io/badge/spark-%3E%3D2.4.x-orange) ![](https://img.shields.io/github/languages/top/moj-analytical-services/splink_graph) ![](https://img.shields.io/pypi/v/splink_graph) ![Downloads](https://pepy.tech/badge/splink-graph)

# splink_graph



![](https://github.com/moj-analytical-services/splink_graph/raw/master/notebooks/splink_graph300x297.png)

---


`splink_graph` is a small graph utility library meant to be used in the Apache Spark environment, that works with graph data structures 
such as the ones created from the outputs of data linking processes (candicate pair results) of ![splink](https://github.com/moj-analytical-services/splink) 

Calculations are performed per cluster/connected component/subgraph in a parallel manner thanks to the underlying help from `pyArrow`

---
## TL&DR :

Graph Database OLAP solutions are a few and far between. 
If you have spark data in a format that can be represented as a network/graph then with this package:

- Graph-theoretic metrics can be obtained efficiently using an already existing spark infrastucture without the need for a graph OLAP solution
- The results can be used as is for finding the needle (of interesting subgraphs) in the haystack (whole set of subgraphs)
- Or one can augment the available graph-compatible data as part of preprocessing step before the data-ingestion phase in an OLTP graph database (such as AWS Neptune etc) 
- Another use is to provide support for feature engineering from the subgraphs/clusters for supervised and unsupervised ML downstream uses.

## How to Install : 
For dependencies and other important technical info so you can run these functions without an issue please consult
`INSTALL.md` on this repo
 
## Functionality offered :

For a primer on the terminology used please look at `TERMINOLOGY.md` file in this repo


####  Cluster metrics

Cluster metrics usually have as an input a spark edgelist dataframe that also includes the component_id (cluster_id) where the edge is in.
The output is a row of one or more metrics per cluster


Cluster metrics currently offered: 

- diameter (largest shortest distance between nodes in a cluster)
- transitivity (or Global Clustering Coefficient in the related literature)
- cluster triangle clustering coeff (or Local Clustering Coefficient in the related literature)
- cluster square clustering coeff (useful for bipartite networks)
- cluster node connectivity 
- cluster edge connectivity
- cluster efficiency
- cluster modularity
- cluster avg edge betweenness
- cluster weisfeiler lehman graphhash (in order to quickly test for graph isomorphisms)

Cluster metrics are really helpful at finding the needles (of for example clusters with possible linking errors) in the 
haystack (whole set of clusters after the data linking process).

---

####  Node metrics

Node metrics  have as an input a spark edgelist dataframe that also includes the component_id (cluster_id) where the edge belongs.
The output is a row of one or more metrics per node

Node metrics curretnly offered: 

- Eigenvector Centrality 
- Harmonic centrality

---

####  Edge metrics

Edge metrics  have as an input a spark edgelist dataframe that also includes the component_id (cluster_id) where the edge belongs.
The output is a row of one or more metrics per edge

Edge metrics curretnly offered: 

- Edge Betweeness
- Bridge Edges


---
## Functionality coming soon

- [x] cluster modularity based on partitions created by edge-betweenness
- [ ] cluster modularity based on partitions created by spectral cut
- [x] cluster modularity based on partitions created by label propagation
- [ ] Node2Vec node embedding functionality per cluster
- [ ] Node2Vec edge embedding functionality per cluster


For upcoming functionality further down the line please consult the `TODO.md` file


## Contributing

Feel free to contribute by 

 * Starting an issue.
 
 * Forking the repository to suggest a change, and/or

 * Want a new metric implemented? Open an issue and ask. Probably it can be.
