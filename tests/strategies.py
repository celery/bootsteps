"""Hypothesis strategies."""

from hypothesis import strategies as st
from hypothesis_networkx import graph_builder
from networkx import DiGraph, is_directed_acyclic_graph, is_isomorphic


class DebuggableDiGraph(DiGraph):
    def __repr__(self):
        return f"""
Nodes: {self.nodes}
Edges: {self.edges}
        """

    def __eq__(self, other):
        return self.nodes == other.nodes and self.edges == other.edges


@st.composite
def non_isomorphic_graph_builder(
    draw,
    previous_graphs=None,
    node_data=st.fixed_dictionaries({}),
    edge_data=st.fixed_dictionaries({}),
    node_keys=None,
    min_nodes=0,
    max_nodes=25,
    min_edges=0,
    max_edges=None,
    graph_type=DebuggableDiGraph,
    self_loops=False,
    connected=True,
):
    if not isinstance(previous_graphs, list):
        raise ValueError("previous_graphs must be a list")

    strategy = graph_builder(
        node_data=node_data,
        edge_data=edge_data,
        node_keys=node_keys,
        min_nodes=min_nodes,
        max_nodes=max_nodes,
        min_edges=min_edges,
        max_edges=max_edges,
        graph_type=graph_type,
        self_loops=self_loops,
        connected=connected,
    ).filter(is_directed_acyclic_graph)
    graph = draw(strategy)
    retries = 0
    while any(is_isomorphic(graph, G) for G in previous_graphs):
        graph = draw(strategy)
        retries += 1
        if retries % 2 == 0:
            min_nodes = min(min_nodes + 2, max_nodes)
            min_edges = min_edges + 2
            strategy = graph_builder(
                node_data=node_data,
                edge_data=edge_data,
                node_keys=node_keys,
                min_nodes=min_nodes,
                max_nodes=max_nodes,
                min_edges=min_edges,
                max_edges=max_edges,
                graph_type=graph_type,
                self_loops=self_loops,
                connected=connected,
            ).filter(is_directed_acyclic_graph)

    previous_graphs.append(graph)

    return graph
