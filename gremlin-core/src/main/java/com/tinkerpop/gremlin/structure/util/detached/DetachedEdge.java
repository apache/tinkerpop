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
 * Represents an {@link Edge} that is disconnected from a {@link Graph}.  "Disconnection" can mean detachment from
 * a {@link Graph} in the sense that the {@link Edge} was constructed from a {@link Graph} instance and this reference
 * was removed or it can mean that the {@code DetachedEdge} could have been constructed independently of a
 * {@link Graph} instance in the first place.
 * <br/>
 * A {@code DetachedEdge} only has reference to the properties and in/out vertices that are associated with it at the
 * time of detachment (or construction) and is not traversable or mutable.  Note that the references to the in/out
 * vertices are {@link DetachedVertex} instances that only have reference to the
 * {@link com.tinkerpop.gremlin.structure.Vertex#id()} and {@link com.tinkerpop.gremlin.structure.Vertex#label()}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DetachedEdge extends DetachedElement<Edge> implements Edge {

    DetachedVertex outVertex;
    DetachedVertex inVertex;

    private final transient Edge.Iterators iterators = new Iterators();

    public DetachedEdge(final Object id, final String label,
                        final Map<String, Object> properties,
                        final Map<String, Object> hiddenProperties,
                        final Pair<Object, String> outV,
                        final Pair<Object, String> inV) {
        super(id, label);
        this.outVertex = new DetachedVertex(outV.getValue0(), outV.getValue1());
        this.inVertex = new DetachedVertex(inV.getValue0(), inV.getValue1());

        if (properties != null) this.properties.putAll(convertToDetachedProperty(properties));
        if (hiddenProperties != null) this.properties.putAll(convertToDetachedProperty(hiddenProperties));
    }

    private DetachedEdge() { }

    private DetachedEdge(final Edge edge) {
        super(edge);
        this.outVertex = DetachedVertex.detach(edge.iterators().vertices(Direction.OUT).next());
        this.inVertex = DetachedVertex.detach(edge.iterators().vertices(Direction.IN).next());

        edge.iterators().properties().forEachRemaining(p -> this.properties.put(p.key(), new ArrayList(Arrays.asList(p instanceof DetachedProperty ? p : new DetachedProperty(p, this)))));
        edge.iterators().hiddens().forEachRemaining(p -> this.properties.put(Graph.Key.hide(p.key()), new ArrayList(Arrays.asList(p instanceof DetachedProperty ? p : new DetachedProperty(p, this)))));
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public Edge attach(final Vertex hostVertex) {
        return StreamFactory.stream(hostVertex.iterators().edges(Direction.OUT, Integer.MAX_VALUE, this.label))
                .filter(e -> e.id().equals(this.id))
                .findFirst().orElseThrow(() -> new IllegalStateException("The detached edge could not be be found incident to the provided vertex: " + this));
    }

    @Override
    public Edge attach(final Graph graph) {
        return graph.e(this.id);
    }

    public void clearVertex(final Direction direction) {
        if (direction == Direction.BOTH || direction == Direction.OUT)
            this.outVertex = null;

        if (direction == Direction.BOTH || direction == Direction.IN)
            this.inVertex = null;
    }

    public static DetachedEdge detach(final Edge edge) {
        if (null == edge) throw Graph.Exceptions.argumentCanNotBeNull("edge");
        if (edge instanceof DetachedEdge) throw new IllegalArgumentException("Edge is already detached");
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

    private Map<String, List<Property>> convertToDetachedProperty(final Map<String, Object> properties) {
        return properties.entrySet().stream()
                .map(entry -> Pair.with(entry.getKey(), (Property) new DetachedProperty(entry.getKey(), entry.getValue(), this)))
                .collect(Collectors.toMap(p -> p.getValue0(), p -> Arrays.asList(p.getValue1())));
    }

    protected class Iterators extends DetachedElement<Edge>.Iterators implements Edge.Iterators, Serializable {

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
