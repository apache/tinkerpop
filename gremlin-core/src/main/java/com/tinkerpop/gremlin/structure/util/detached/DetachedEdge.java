package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DetachedEdge extends DetachedElement implements Edge {

    DetachedVertex outVertex;
    DetachedVertex inVertex;

    private DetachedEdge() {

    }

    public DetachedEdge(final Object id, final String label,
                        final Map<String, Object> properties,
                        final Map<String, Object> hiddenProperties,
                        final Pair<Object, String> outV,
                        final Pair<Object, String> inV) {
        super(id, label);
        this.outVertex = new DetachedVertex(outV.getValue0(), outV.getValue1());
        this.inVertex = new DetachedVertex(inV.getValue0(), inV.getValue1());

        if (properties != null) {
            this.properties = properties.entrySet().stream()
                    .map(entry -> Pair.with(entry.getKey(), (Property) new DetachedProperty(entry.getKey(), entry.getValue(), this)))
                    .collect(Collectors.toMap(p -> p.getValue0(), p -> Arrays.asList(p.getValue1())));
        }

        if (hiddenProperties != null) {
            this.hiddens = hiddenProperties.entrySet().stream()
                    .map(entry -> Pair.with(entry.getKey(), (Property) new DetachedProperty(entry.getKey(), entry.getValue(), this)))
                    .collect(Collectors.toMap(p -> p.getValue0(), p -> Arrays.asList(p.getValue1())));
        }
    }

    private DetachedEdge(final Edge edge) {
        super(edge);
        this.outVertex = DetachedVertex.detach(edge.outV().next());
        this.inVertex = DetachedVertex.detach(edge.inV().next());
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    public Edge attach(final Vertex hostVertex) {
        return StreamFactory.stream(hostVertex.iterators().edges(Direction.OUT, Integer.MAX_VALUE, this.label))
                .filter(e -> e.id().equals(this.id))
                .findFirst().orElseThrow(() -> new IllegalStateException("The detached edge could not be be found incident to the provided vertex: " + this));
    }

    public Edge attach(final Graph graph) {
        return graph.e(this.id);
    }

    public static DetachedEdge detach(final Edge edge) {
        if (null == edge) throw Graph.Exceptions.argumentCanNotBeNull("edge");
        return new DetachedEdge(edge);
    }

    @Override
    public Edge.Iterators iterators() {
        return this.iterators;
    }

    @Override
    public GraphTraversal<Edge, Edge> start() {
        throw new UnsupportedOperationException("Detached edges cannot be traversed: " + this);
    }

    private final Edge.Iterators iterators = new Iterators();

    protected class Iterators extends DetachedElement.Iterators implements Edge.Iterators, Serializable {

        @Override
        public Iterator<Vertex> vertices(final Direction direction) {
            final List<Vertex> vertices = new ArrayList<>(2);
            if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH))
                vertices.add(outVertex);
            if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH))
                vertices.add(inVertex);
            return vertices.iterator();
        }

        @Override
        public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
            return (Iterator) super.properties(propertyKeys);
        }

        @Override
        public <V> Iterator<Property<V>> hiddens(final String... propertyKeys) {
            return (Iterator) super.hiddens(propertyKeys);
        }

    }
}
