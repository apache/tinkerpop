package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphView;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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

    public static final Iterator<TinkerEdge> getEdges(final TinkerVertex vertex, final Direction direction, final String... edgeLabels) {
        Stream<Edge> edgeStream = null;
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH))
            edgeStream = (edgeLabels.length == 0 ? vertex.outEdges.keySet().stream() : Stream.of(edgeLabels)).filter(label -> !Graph.Hidden.isHidden(label)).map(vertex.outEdges::get).filter(edges -> null != edges).flatMap(list -> list.stream());
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH))
            edgeStream = Stream.concat(null == edgeStream ? Stream.empty() : edgeStream, (edgeLabels.length == 0 ? vertex.inEdges.keySet().stream() : Stream.of(edgeLabels)).filter(label -> !Graph.Hidden.isHidden(label)).map(vertex.inEdges::get).filter(edges -> null != edges).flatMap(list -> list.stream()));
        return (Iterator) edgeStream.collect(Collectors.toList()).iterator();


    }

    public static final Iterator<TinkerVertex> getVertices(final TinkerVertex vertex, final Direction direction, final String... edgeLabels) {
        if (direction != Direction.BOTH) {
            return IteratorUtils.map(TinkerHelper.getEdges(vertex, direction, edgeLabels), direction.equals(Direction.IN) ? edge -> (TinkerVertex) edge.outVertex : edge -> (TinkerVertex) edge.inVertex);
        } else {
            return IteratorUtils.<TinkerVertex>concat(
                    IteratorUtils.map(TinkerHelper.getEdges(vertex, Direction.OUT, edgeLabels), edge -> (TinkerVertex) edge.inVertex),
                    IteratorUtils.map(TinkerHelper.getEdges(vertex, Direction.IN, edgeLabels), edge -> (TinkerVertex) edge.outVertex));
        }
    }
}
