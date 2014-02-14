package com.tinkerpop.tinkergraph;

import com.tinkerpop.gremlin.process.steps.util.MultiIterator;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StreamFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerHelper {

    protected static String getNextId(final TinkerGraph graph) {
        return Stream.generate(() -> ((Long) (++graph.currentId)).toString()).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findFirst().get();
    }

    protected static Edge addEdge(final TinkerGraph graph, final TinkerVertex outVertex, final TinkerVertex inVertex, final String label, final Object... keyValues) {
        if (label == null)
            throw Edge.Exceptions.edgeLabelCanNotBeNull();
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Object idString = ElementHelper.getIdValue(keyValues).orElse(null);

        final Edge edge;
        if (null != idString) {
            if (graph.edges.containsKey(idString.toString()))
                throw Graph.Exceptions.edgeWithIdAlreadyExist(idString);
        } else {
            idString = TinkerHelper.getNextId(graph);
        }

        edge = new TinkerEdge(idString.toString(), outVertex, label, inVertex, graph);
        ElementHelper.attachProperties(edge, keyValues);
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

    public static Collection<Vertex> getVertices(final TinkerGraph graph) {
        return graph.vertices.values();
    }

    public static Collection<Edge> getEdges(final TinkerGraph graph) {
        return graph.edges.values();
    }

    public static List<TinkerVertex> queryVertexIndex(final TinkerGraph graph, final String key, final Object value) {
        return graph.vertexIndex.get(key, value);
    }

    public static List<TinkerEdge> queryEdgeIndex(final TinkerGraph graph, final String key, final Object value) {
        return graph.edgeIndex.get(key, value);
    }

    public static Iterator<TinkerEdge> getEdges(final TinkerVertex vertex, final Direction direction, final String... labels) {
        final MultiIterator<Edge> edges = new MultiIterator<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (labels.length > 0) {
                for (final String label : labels) {
                    edges.addIterator(vertex.outEdges.getOrDefault(label, Collections.emptySet()).iterator());
                }
            } else {
                for (final Set<Edge> set : vertex.outEdges.values()) {
                    edges.addIterator(set.iterator());
                }
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (labels.length > 0) {
                for (final String label : labels) {
                    edges.addIterator(vertex.inEdges.getOrDefault(label, Collections.emptySet()).iterator());
                }
            } else {
                for (final Set<Edge> set : vertex.inEdges.values()) {
                    edges.addIterator(set.iterator());
                }
            }
        }
        return (Iterator) edges;
    }

    public static Iterator<TinkerVertex> getVertices(final TinkerVertex vertex, final Direction direction, final String... labels) {
        if (direction != Direction.BOTH) {
            return (Iterator) StreamFactory.stream(TinkerHelper.getEdges(vertex, direction, labels)).map(e -> e.getVertex(direction.opposite())).iterator();
        } else {
            final MultiIterator<TinkerVertex> vertices = new MultiIterator<>();
            vertices.addIterator((Iterator) StreamFactory.stream(TinkerHelper.getEdges(vertex, Direction.OUT, labels)).map(e -> e.getVertex(Direction.IN)).iterator());
            vertices.addIterator((Iterator) StreamFactory.stream(TinkerHelper.getEdges(vertex, Direction.IN, labels)).map(e -> e.getVertex(Direction.OUT)).iterator());
            return vertices;
        }
    }
}
