package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.process.util.MultiIterator;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphView;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerHelper {

    protected final synchronized static long getNextId(final TinkerGraph graph) {
        return Stream.generate(() -> (++graph.currentId)).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findAny().get();
    }

    protected static Edge addEdge(final TinkerGraph graph, final TinkerVertex outVertex, final TinkerVertex inVertex, final String label, final Object... keyValues) {
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);

        final Edge edge;
        if (null != idValue) {
            if (graph.edges.containsKey(idValue))
                throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        } else {
            idValue = TinkerHelper.getNextId(graph);
        }

        edge = new TinkerEdge(idValue, outVertex, label, inVertex, graph);
        ElementHelper.attachProperties(edge, keyValues);
        graph.edges.put(edge.id(), edge);
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

    public static void dropView(final TinkerGraph graph) {
        graph.graphView = null;
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

    public static boolean inComputerMode(final TinkerGraph graph) {
        return null != graph.graphView;
    }

    public static TinkerGraphView createGraphView(final TinkerGraph graph, final GraphComputer.Isolation isolation, final Set<String> computeKeys) {
        return graph.graphView = new TinkerGraphView(isolation, computeKeys);
    }

    public static Map<String, List<Property>> getProperties(final TinkerElement element) {
        return element.properties;
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
            return new TinkerVertexIterator(TinkerHelper.getEdges(vertex, direction, labels), direction);
        } else {
            final MultiIterator<TinkerVertex> vertices = new MultiIterator<>();
            vertices.addIterator(new TinkerVertexIterator(TinkerHelper.getEdges(vertex, Direction.OUT, labels), Direction.OUT));
            vertices.addIterator(new TinkerVertexIterator(TinkerHelper.getEdges(vertex, Direction.IN, labels), Direction.IN));
            return vertices;
        }
    }

    public static Iterator<TinkerVertex> getVertices(final TinkerEdge edge, final Direction direction) {
        final List<TinkerVertex> vertices = new ArrayList<>(2);
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH))
            vertices.add((TinkerVertex) edge.outVertex);
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH))
            vertices.add((TinkerVertex) edge.inVertex);
        return vertices.iterator();
    }

    private static class TinkerVertexIterator implements Iterator<TinkerVertex> {

        private final Iterator<TinkerEdge> edges;
        private final Direction direction;

        public TinkerVertexIterator(final Iterator<TinkerEdge> edges, final Direction direction) {
            this.edges = edges;
            this.direction = direction;
        }

        @Override
        public boolean hasNext() {
            return this.edges.hasNext();
        }

        @Override
        public TinkerVertex next() {
            if (this.direction.equals(Direction.IN))
                return (TinkerVertex) this.edges.next().outVertex;
            else
                return (TinkerVertex) this.edges.next().inVertex;
        }
    }

    private static class TinkerEdgeIterator implements Iterator<TinkerEdge> {

        private final Iterator<TinkerEdge> edges;
        private final int branchFactor;
        private int currentCount = 0;

        public TinkerEdgeIterator(final Iterator<TinkerEdge> edges, final int branchFactor) {
            this.edges = edges;
            this.branchFactor = branchFactor;
        }

        @Override
        public boolean hasNext() {
            return this.currentCount < this.branchFactor && this.edges.hasNext();
        }

        @Override
        public TinkerEdge next() {
            if (this.currentCount++ < this.branchFactor) {
                return this.edges.next();
            } else {
                throw FastNoSuchElementException.instance();
            }
        }
    }
}
