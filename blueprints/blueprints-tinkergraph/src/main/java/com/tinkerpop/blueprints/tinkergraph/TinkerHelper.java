package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.util.ElementHelper;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerHelper {

    protected static String getNextId(final TinkerGraph graph) {
        return Stream.generate(() -> ((Long) (++graph.currentId)).toString()).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findFirst().get();
    }

    protected static Edge addEdge(final TinkerGraph graph, final TinkerVertex outVertex, final TinkerVertex inVertex, final String label, final Object... keyValues) {
        if (label == null)
            throw Edge.Features.edgeLabelCanNotBeNull();
        ElementHelper.legalKeyValues(keyValues);

        Object idString = ElementHelper.getIdValue(keyValues);

        final Edge edge;
        if (null != idString) {
            if (graph.edges.containsKey(idString.toString()))
                throw Graph.Features.edgeWithIdAlreadyExist(idString);
        } else {
            idString = TinkerHelper.getNextId(graph);
        }

        edge = new TinkerEdge(idString.toString(), outVertex, label, inVertex, graph);
        ElementHelper.attachKeyValues(edge, keyValues);
        graph.edges.put(edge.getId().toString(), edge);
        TinkerHelper.addOutEdge(outVertex, label, edge);
        TinkerHelper.addInEdge(inVertex, label, edge);
        return edge;

    }

    protected static void addOutEdge(final TinkerVertex vertex, final String label, final Edge edge) {
        Set<Edge> edges = vertex.outEdges.get(label);
        if (null == edges) {
            edges = new HashSet<>();
            vertex.outEdges.put(label, edges);
        }
        edges.add(edge);
    }

    protected static void addInEdge(final TinkerVertex vertex, final String label, final Edge edge) {
        Set<Edge> edges = vertex.inEdges.get(label);
        if (null == edges) {
            edges = new HashSet<>();
            vertex.inEdges.put(label, edges);
        }
        edges.add(edge);
    }
}
