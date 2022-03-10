  # Splink Graph Guide

This guide aims to act as both a user guide and terminology guide, to aid those wanting to use Splink-Graph to understand the theory behind it as well as practical knowledge on how to use it to interpret cluster metrics. 

## Contents:

* [Graph Theory](#graph_theory)
* [Terminology](#terminology)
* [Installation guide](#installation_guide)
* [Set-up Guide](#setupguide)
    * [Connected Components](#connectedcomponents)
* [Metrics](#metrics)
    * [Cluster Metrics](#clustermetrics)
    * [Node Metrics](#nodemetrics)
    * [Edge Metrics](#edgemetrics)
* [Resources](#resources)
* [References](#references)

## Graph Theory  <a name= 'graph_theory'/>

A language for networks/graphs. In summary, graph theory gives us a language for networks/graphs. It allows us to define graphs exactly and to quantify graph 
properties at all different levels. 

For more information on Graph Theory, [Network Science][networksciencebook] by Albert-László Barabási is a great resource. 

[networksciencebook]: http://networksciencebook.com/

## Terminology <a name= 'terminology'/>

Like any discipline, graphs come with their own set of nomenclature. 
The following descriptions are intentionally simplified—more mathematically rigorous definitions can be found in any graph theory textbook (e.g. [Network Science][networksciencebook]).

[networksciencebook]: http://networksciencebook.com/

`Graph` <br>
A data structure G = (V, E) where V and E are a set of vertices/nodes and edges.

`Vertex/Node` <br>
Represents a single entity such as a person or an object,

`Edge` <br>
Represents a relationship between two vertices (e.g., are these two vertices/nodes friends on a social network?).

`Directed Graph vs. Undirected Graph` <br>
Denotes whether the relationship represented by edges is symmetric or not 

`Weighted vs Unweighted Graph` <br>
In weighted graphs edges have a weight that could represent cost of traversing or a similarity score or a distance score

In unweighted graphs edges have no weight and simply show connections . example: course prerequisites

`Subgraph` <br>
A set of vertices and edges that are a subset of the full graph's vertices and edges.

`Degree` <br>
A vertex/node measurement quantifying the number of connected edges 

`Connected Component` <br>
A strongly connected subgraph, meaning that every vertex can reach the other vertices in the subgraph.

`Shortest Path` <br>
unweighted: The lowest number of edges required to traverse between two specific vertices/nodes unweighted
    
weighted: The edges with the least traversal cost between two specific vertices/nodes.

`Modularity` <br>
A metric that explores whether there is a true division of a cluster into subclusters or not. A positive value would suggest the existence of a sub-cluster structure, with a greater value providing more evidence. Oppositely, no change in this value suggests no underlying sub-cluster structure, or that by dividing the cluster, things are "getting worse respectively" ([Chatzoglou, Manassis, et al (2016)][chatzogloumanassisetal2016]).

[chatzogloumanassisetal2016]:[https://gss.civilservice.gov.uk/wp-content/uploads/2016/07/1.4.2-Christos-Chatzoglou-Use-of-graph-databases-to-improve-the-management-and-quality-of-linked-data.docx]

## Installation Guide <a name= 'installation_guide'/>

Make sure you are running this on a spark system/cluster system!

Firstly, make sure you have these packages installed:
* **PyArrow** ~ Version 0.15.0 or above
* **pandas** ~ Version 1.1.5 or above
* **scipy** ~ Version 1.5.4 or above
* **numpy** ~ Version 1.19.5 or above

To check use, ```pip show _packagename_```

Then, the easiest way to install splink_graph is:
```pip install --upgrade splink_graph```

For information regarding how to install splink_graph and its dependencies, please see [INSTALL.md][install]

[install]: https://github.com/moj-analytical-services/splink_graph/blob/master/INSTALL.md

For a ONS-DAP specific guide, please contact @EKenning

## Set-up Guide <a name= 'setupguide'/>

We recommend utilising Jupyter Notebooks when working with splink_graph, as this is easy to use and produces good visualisations.

Once installed, you will need to install all the packages mentioned previously into your session.
E.g.,

```python
import splink_graph
```

Then you will want to locate the utils folder

```python
from splink_graph.utils import _create_spark_jars_string
_create spark_jars_string()
```

And then use the output above to create a jar_string, e.g.:

```python
jar_string = '/abc/def/ghi/
```

You then must define your spark session.

Then you are able to read in the dataframe you wish to use, or alternatively, explore the test dataframe included in the package:

```python
test_df = spark.read.parquet('df_e.snappy.parquet')
```

### Connected Components <a name= 'connectedcomponents' />

Once you have set up the package and read in the dataframe you wish to use, you must run the connected components function. 

This function will provide the edges with scores as well as a cluster id, which are vital for producing the cluster metrics. 

Which connected components function you choose to use, is based on the size of your dataset. 

For a dataset with <2 million records, **nx_connected_components** will need to be used. <br>
For dataset with >2 milion records, **graphframes_connected_components** will need to be used.


For the test dataframe, nx_connected_components should be used. 

```python
from graphframes import GraphFrame
import networkx as nx
from splink_graph.cc import nx_connected_components
from splink_graph.cc import graphframes_connected_components
```

To learn more about connected_components, use the help() function. 

```help(graphframes_connected_components)``` <br>

```help(nx_connected_components)```

You may need to rename your variables before you run connected components, particularly if using graphframes.

```python
dataframe = dataframe.withColumnRenamed('linkage_id_l', 'src')\
                     .withColumnRenamed('linkage_id_r', 'dst')\
                     .withColumnRenamed('match_probability', 'tf_adjusted_match_prob')
```

**You can then run your connected components function!** <br>

_The cc_threshold decides the threshold for a valid linkage weight. Please adjust in-line with your data._


``` python
dataframe = graphframes_connected_components(dataframe, src='src', dst='dst', weight_colname='tf_adjusted_match_prob',\
                                             cc_threshold = 0.9)
dataframe.show()
```

## Metrics <a name= 'metrics'/>

Below are the metrics available through splink-graph, what they calculate and what they can tell you about your data.

They are arranged into 3 types:

- [Cluster Metrics](#clustermetrics)
- [Node Metrics](#nodemetrics)
- [Edge Metrics](#edgemetrics)

## Cluster Metrics <a name= 'clustermetrics' />

### **cluster_basic_stats**
This provides node count, edge count, number of nodes per cluster, and density. 

```python
cluster_basic_stats(df, src ="src", dst = "dst", cluster_id_colname = "cluster_id", weight_colname = "weight")
```

`Node count` <br>
Number of nodes.

`Edge count` <br>
Number of edges/vertices.

`Nodes per cluster` <br>
Number of nodes contained within each cluster.

`Density` <br>
"The density of a graph is a number ranging from 0 to 1 and reflecting how connected a graph is in regards to its maximum potential connectivity. The density can also be formulated as the ratio between actual connections, i.e. the number of existing edges in the graph, and the number of maximum potential connections between vertices" ([Croset et al (2015)][crosetetal2015]).

[crosetetal2015]: [https://academic.oup.com/bioinformatics/article/32/6/918/1743746]

        Actual Edges / Maximum Possible Edges

<span style="color:green">**The higher the density, the more connected the cluster is.**</span>

### **cluster_main_stats**
Calculates diameter, transivity, triangle clustering coefficient and square clustering coefficient.

```python
cluster_main_stats(sparkdf, src="src", dst="dst", cluster_id_colname="cluster_id")
```

`Diameter` <br>
The diameter of a graph is a measure of the longest distance between two nodes ([Randall et al (2014)][randalletal2014]).

`Transivity` (or `Global Clustering Coefficient` in the related literature) <br>
The global clustering coefficient is based on triplets of nodes in a graph. A triplet consists of three connected nodes. A triangle therefore includes three closed triplets, one centered on each of the nodes (n.b. this means the three triplets in a triangle come from overlapping selections of nodes). The global clustering coefficient is the number of closed triplets (or 3 x triangles) over the total number of triplets (both open and closed).

<span style="color:red">**A high diameter but low transivity means something might be wrong and your need to investigate. 
Especially if there are bridges that connect the clusters.**</span>


`Traingle Clustering Coefficient` (or `Local Clustering Coefficient` in the related literature) <br>
The local clustering coefficient for a node is then given by the proportion of links between the vertices within its neighborhood divided by the number of links that could possibly exist between them. The local clustering coefficient for a graph is the average LCC for all the nodes in that graph.
   
`Square Clustering Coefficient`<br>
Quantifies the abundance of connected squares in a graph (useful for bipartite networks where nodes cannot be connected in triangles).

[randalletal2014]: [https://espace.curtin.edu.au/bitstream/handle/20.500.11937/3205/199679_199679.pdf?sequence=2&isAllowed=y]

### **cluster_graph_hash**
Calculates Weisfeiler-Lehman graphhash of a cluster (in order to quickly test for graph isomorphisms).

```python
cluster_graph_hash(sparkdf, src="src", dst="dst", cluster_id_colname="cluster_id")
```

`weisfeiler lehman graph hash` <br>
A Weisfeiler Lehman graph hash offers a way to quickly test for graph isomorphisms. Hashes are identical for isomorphic graphs 
and there are strong guarantees that non-isomorphic graphs will get different hashes. 
Two graphs are considered isomorphic if there is a mapping between the nodes of the graphs that preserves node adjacencies. 
That is, a pair of nodes may be connected by an edge in the first graph if and only if the corresponding pair of nodes in 
the second graph is also connected by an edge in the same way. 

![](https://github.com/moj-analytical-services/splink_graph/raw/master/notebooks/graph-isomorphism.png)

Graph 1 and Graph 2 above are isomorphic. 
The correspondance between nodes is illustrated by the node colors and numbers.

### **cluster_connectivity_stats**
Calculates Node Connectivity and Edge Connectivity of the cluster.
```python
cluster_connectivity_stats(sparkdf, src="src", dst="dst", cluster_id_colname="cluster_id")
```

`node connectivity`<br>
Node connectivity of a graph gives for the minimum number of nodes that need to be removed to separate 
the remaining nodes into two or more isolated subgraphs.
    
`edge connectivity`<br>
Edge connectivity of a graph gives for the minimum number of edges that need to be removed to separate 
the remaining nodes into two or more isolated subgraphs.

<span style="color:green">**The larger these values are, the more connected the cluster is.**</span>

### **cluster_eb_modularity**
Caclulates the edgebetweeness modularity ie the modularity of a subgraph if partitioned into two parts 
at the point where the highest edge betweeness exists.

```python
cluster_eb_modularity(sparkdf, src="src", dst="dst", distance_colname = "distance", cluster_id_colname="cluster_id")
```

`Comp eb modularity` <br>
Modularity for cluster_id if it partitioned into two parts at the point where the highest edge betweeness exists.

### **cluster_lpg_modularity**
Calculates the cluster lpg modularity.

```python
cluster_lpg_modularity(sparkdf, src="src", dst="dst", distance_colname = "distance", cluster_id_colname="cluster_id")
```

`Cluster lpg modularity` <br>
Modularity for cluster_id if it partitioned into 2 parts based on label propagation.

### **cluster_avg_edge_betweeness**
Provides the average edge betweeness for each cluster id. 

``` python
cluster_avg_edge_betweeness(sparkdf, src="src", dst="dst", distance_colname = "distance", cluster_id_colname="cluster_id") 
```

`Average Edge Betweeness` <br>
“In order to get a measure for the robustness of a network we can take the average of the vertex/edge betweenness. The smaller this average, the more robust the network." - ([Ellens and Kooij (2013)][ellenskooij2013])

[ellenskooij2013]:[https://arxiv.org/pdf/1311.5064]

_See below for edge betweenness definition._

### **number_of_bridges**
Provides the number of bridges in the cluster. 

```python
number_of_bridges(sparkdf, src="src", dst="dst", cluster_id_colname="cluster_id")
```

`Number of Bridges`<br>
The number of edges that join two clusters together (a bridge). 

<span style="color:red">**A high bridge count could suggest there is something wrong!**</span>

### **cluster_assortativity**
Calculates the assortativity of a cluster. 

```python
cluster_assortativity(sparkdf, src="src", dst="dst", cluster_id_colname="cluster_id")
```
`Cluster Assortativity`<br>
“Assortativity is a graph metric. It represents to what extent nodes in a network associate with other nodes in the network, being of similar sort or being of opposing sort... A network is said to be assortative when high degree nodes are, on average, connected to other nodes with high degree and low degree nodes are, on average, connected to other nodes with low degree. A network is said to be disassortative when, on average, high degree nodes are connected to nodes with low(er) degree and, on average, low degree nodes are connected to nodes with high(er) degree"  - ([Noldus and Mieghem (2015)][noldusmieghem2015]).

[noldusmieghem2015]:[https://nas.ewi.tudelft.nl/people/Piet/papers/JCN2015AssortativitySurveyRogier.pdf]

### **cluster_efficiency**
To be continued... Need to find code example.


`Cluster Effiiency`<br>
“For the efficiency it holds that the greater the value, the greater the robustness, because the reciprocals of the path lengths are used. The advantage of this measure is that it can be used for unconnected networks, such as social networks or networks subject to failures. Otherwise, it has the same disadvantage as the average path length; alternative paths are not considered”  - ([Ellens and Kooij (2013)][ellenskooij2013])

[ellenskooij2013]:[https://arxiv.org/pdf/1311.5064]

## Node Metrics <a name= 'nodemetrics' />

### **eigencentrality**
Provides the eingenvector centrality of a cluster. 

```python
eigencentrality(sparkdf, src="src", dst="dst", cluster_id_colname="cluster_id")
```

`Eigencentrality`<br>
Eigenvector Centrality is an algorithm that measures the transitive influence or connectivity of nodes.
Relationships to high-scoring nodes contribute more to the score of a node than connections to low-scoring nodes.

<span style="color:green">**A high score means that a node is connected to other nodes that have high scores.**</span>

### **harmoniccentrality**
Provides the harmonic centrality of a cluster. 

```python
harmoniccentrality(sparkdf, src="src", dst="dst", cluster_id_colname="cluster_id")
```

`Harmonic Centrality`<br>
Harmonic centrality (also known as valued centrality) is a variant of closeness centrality, that was invented
to solve the problem the original formula had when dealing with unconnected graphs.
Rather than summing the distances of a node to all other nodes, the harmonic centrality algorithm sums the inverse of those distances.
This enables it deal with infinite values.

## Edge Metrics <a name= 'edgemetrics' />

### **edgebetweeness**
Provides the edge betweeness. 

```python
edgebetweeness(sparkdf, src="src", dst="dst", distance_colname = "distance", cluster_id_colname="cluster_id")
```

`Edge Betweeness`<br>
“The edge betweeness graph metric counts the number of shortest paths between any two nodes from the cluster that use this edge. If there is more than one shortest path between a pair of nodes, each path is assigned equal weight such that the total weight of all of the paths is equal to unity."  - ([Chatzoglou, Manassis, et al (2016)][chatzogloumanassisetal2016]).

[chatzogloumanassisetal2016]:[https://gss.civilservice.gov.uk/wp-content/uploads/2016/07/1.4.2-Christos-Chatzoglou-Use-of-graph-databases-to-improve-the-management-and-quality-of-linked-data.docx]

### **bridge_edges**
Returns any edges that are bridges.

```python
bridge_edges(sparkdf, src="src", dst="dst", distance_colname = "distance", cluster_id_colname="cluster_id")
```

`Bridge Edges`<br>
Bridge Edges are edges which join together two distinct clusters.

## Resources <a name= 'resources'/>

For more information regarding the **splink-graph package**, please see MoJ github: <https://github.com/moj-analytical-services/splink_graph>.

For a more in-depth look into **graph data and graph theory**, please see [Network Science][networksciencebook] by Albert-László Barabási. 

[networksciencebook]: http://networksciencebook.com/

## References <a name= 'references'/>

Chatzoglou, C., Manassis, T., Gammon, S. and Swier, N., 2016. Use of Graph Databases to Improve the Management and Quality of Linked Data.
<https://espace.curtin.edu.au/bitstream/handle/20.500.11937/3205/199679_199679.pdf?sequence=2&isAllowed=y>

Croset, S., Rupp, J. and Romacker, M., 2016. Flexible data integration and curation using a graph-based approach. Bioinformatics, 32(6), pp.918-925. <https://academic.oup.com/bioinformatics/article/32/6/918/1743746>


Ellens, W. and Kooij, R.E., 2013. Graph measures and network robustness. arXiv preprint arXiv:1311.5064.
<https://arxiv.org/pdf/1311.5064>


Noldus, R. and Van Mieghem, P., 2015. Assortativity in complex networks. Journal of Complex Networks, 3(4), pp.507-542.
<https://nas.ewi.tudelft.nl/people/Piet/papers/JCN2015AssortativitySurveyRogier.pdf>


Randall, S.M., Boyd, J.H., Ferrante, A.M., Bauer, J.K. and Semmens, J.B., 2014. Use of graph theory measures to identify errors in record linkage. Computer methods and programs in biomedicine, 115(2), pp.55-63.
<https://espace.curtin.edu.au/bitstream/handle/20.500.11937/3205/199679_199679.pdf?sequence=2&isAllowed=y>
