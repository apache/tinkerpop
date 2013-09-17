package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerHelper {

    protected static String getNextId(TinkerGraph graph) {
        return Stream.generate(() -> ((Long) (++graph.currentId)).toString()).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findFirst().get();
    }


    protected static Edge addEdge(final TinkerGraph graph, final TinkerVertex outVertex, final TinkerVertex inVertex, final String label, final Property... properties) {
        if (label == null)
            throw ExceptionFactory.edgeLabelCanNotBeNull();

        String idString = Stream.of(properties)
                .filter((Property p) -> p.getKey().equals(Property.Key.ID.toString()))
                .map(p -> p.getValue().toString())
                .findFirst()
                .orElseGet(() -> null);

        final Edge edge;
        if (null != idString) {
            if (graph.edges.containsKey(idString))
                throw ExceptionFactory.edgeWithIdAlreadyExist(idString);
        } else {
            idString = TinkerHelper.getNextId(graph);
        }

        edge = new TinkerEdge(idString, outVertex, inVertex, label, graph);
        Stream.of(properties)
                .filter(p -> !p.getKey().equals(Property.Key.ID.toString()) && !p.getKey().equals(Property.Key.LABEL.toString()))
                .forEach(p -> {
                    edge.setProperty(p.getKey(), p.getValue());
                });

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
