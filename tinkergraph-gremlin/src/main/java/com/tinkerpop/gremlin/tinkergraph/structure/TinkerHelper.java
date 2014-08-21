package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.graph.strategy.CountCapStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.JumpComputerStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.SideEffectCapComputerStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.TraverserSourceStrategy;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.process.util.MultiIterator;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphView;
import com.tinkerpop.gremlin.tinkergraph.process.graph.step.sideEffect.TinkerGraphStep;
import com.tinkerpop.gremlin.tinkergraph.process.graph.strategy.TinkerGraphStepStrategy;

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

    protected static long getNextId(final TinkerGraph graph) {
        return Stream.generate(() -> (++graph.currentId)).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findFirst().get();
    }

    protected static Edge addEdge(final TinkerGraph graph, final TinkerVertex outVertex, final TinkerVertex inVertex, final String label, final Object... keyValues) {
        if (null == label)
            throw Edge.Exceptions.edgeLabelCanNotBeNull();
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

    public static TinkerGraphView getGraphView(final TinkerGraph graph) {
        return graph.graphView;
    }

    public static TinkerGraphView createGraphView(final TinkerGraph graph, final GraphComputer.Isolation isolation, final Map<String, VertexProgram.KeyType> computeKeys) {
        return graph.graphView = new TinkerGraphView(isolation, computeKeys);
    }

    public static Map<String, Property> getProperties(final TinkerElement element) {
        return element.properties;
    }

    public static Iterator<TinkerEdge> getEdges(final TinkerVertex vertex, final Direction direction, final int branchFactor, final String... labels) {
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
        return new TinkerEdgeIterator((Iterator) edges, branchFactor);
    }

    public static Iterator<TinkerVertex> getVertices(final TinkerVertex vertex, final Direction direction, final int branchFactor, final String... labels) {
        if (direction != Direction.BOTH) {
            return new TinkerVertexIterator(TinkerHelper.getEdges(vertex, direction, branchFactor, labels), direction);
        } else {
            final MultiIterator<TinkerVertex> vertices = new MultiIterator<>(branchFactor);
            vertices.addIterator(new TinkerVertexIterator(TinkerHelper.getEdges(vertex, Direction.OUT, branchFactor, labels), Direction.OUT));
            vertices.addIterator(new TinkerVertexIterator(TinkerHelper.getEdges(vertex, Direction.IN, branchFactor, labels), Direction.IN));
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

    public static void prepareTraversalForComputer(final Traversal traversal) {
        if (traversal.getSteps().get(0) instanceof TinkerGraphStep)
            ((TinkerGraphStep) traversal.getSteps().get(0)).graph = null;
        traversal.sideEffects().removeGraph();
        traversal.strategies().unregister(TinkerGraphStepStrategy.class);
        traversal.strategies().unregister(TraverserSourceStrategy.class);
        traversal.strategies().register(CountCapStrategy.instance());
        traversal.strategies().register(SideEffectCapComputerStrategy.instance());
        traversal.strategies().register(JumpComputerStrategy.instance());
    }

    private static class TinkerVertexIterator implements Iterator<TinkerVertex> {

        private final Iterator<TinkerEdge> edges;
        private final Direction direction;

        public TinkerVertexIterator(final Iterator<TinkerEdge> edges, final Direction direction) {
            this.edges = edges;
            this.direction = direction;
        }

        public boolean hasNext() {
            return this.edges.hasNext();
        }

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

        public boolean hasNext() {
            return this.currentCount < this.branchFactor && this.edges.hasNext();
        }

        public TinkerEdge next() {
            if (this.currentCount++ < this.branchFactor) {
                return this.edges.next();
            } else {
                throw FastNoSuchElementException.instance();
            }
        }
    }
}
