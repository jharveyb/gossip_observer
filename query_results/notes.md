# LN Graph Analysis Tools

## Dependencies

We used `uv` to generate the initial plots of collected data.
However, for LN graph analysis, the graph is large enough to justify using
libraries like `graph-tool`. The dependency on C++ libraries seems to prevent,
or at least complicate using this dependency AND managing the other dependencies with `uv`.

Debian testing packages:

- python3-graph-tool
- python3-pandas
- pyhton3-numpy

For the (old) plotting script:

- python3-matplotlib

## Approach

We have two goals:

- Identify _communities_ of LN nodes, based on basic information about the graph.
I.e. channels and channel capacity. Once we have a reasonable list of communities,
we can have individual collector instances connect to specific communities.
- _Classify_ LN nodes, given the information we have about each node
(node alias, clearnet/tor/both, AS number, edge degree, total capacity, age, first_seen, closed channel count, etc.) We could also mix in data from other
entities like Amboss or Terminal. Once we have nodes classified, we can see which
classes of nodes occupy specific communities, and make sure we connect to a variety
of node classes. We can also run queries over specific node types vs. all nodes.

### Community detection

The common approaches seem to be algorithms based on modularity (Leiden, Louvain)
and more complex techniques (statistical block modelling / SBM). Further reading:

<https://skewed.de/lab/posts/modularity-harmful/>
