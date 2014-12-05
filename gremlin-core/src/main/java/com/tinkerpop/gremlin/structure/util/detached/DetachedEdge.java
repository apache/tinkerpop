package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.util.DoubleIterator;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.javatuples.Pair;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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
public class DetachedEdge extends DetachedElement<Edge> implements Edge, Edge.Iterators {

    private DetachedVertex outVertex;
    private DetachedVertex inVertex;

    private DetachedEdge() {

    }

    public DetachedEdge(final Edge edge, final boolean asReference) {
        super(edge);
        this.outVertex = DetachedFactory.detach(edge.iterators().vertexIterator(Direction.OUT).next(), true);
        this.inVertex = DetachedFactory.detach(edge.iterators().vertexIterator(Direction.IN).next(), true);
        if (!asReference) {
            this.properties = new HashMap<>();
            edge.iterators().propertyIterator().forEachRemaining(property -> this.properties.put(property.key(), DetachedElement.makeSinglePropertyList(DetachedFactory.detach(property))));
        }
    }

    public DetachedEdge(final Object id, final String label,
                        final Map<String, Object> properties,
                        final Pair<Object, String> outV,
                        final Pair<Object, String> inV) {
        super(id, label);
        this.outVertex = new DetachedVertex(outV.getValue0(), outV.getValue1(), Collections.emptyMap());
        this.inVertex = new DetachedVertex(inV.getValue0(), inV.getValue1(), Collections.emptyMap());
        if (!properties.isEmpty()) {
            this.properties = new HashMap<>();
            this.properties.putAll(convertToDetachedProperty(properties));
        }
    }


    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public Edge attach(final Vertex hostVertex) {
        return StreamFactory.stream(hostVertex.iterators().edgeIterator(Direction.OUT, this.label))
                .filter(edge -> edge.equals(this))
                .findAny().orElseThrow(() -> new IllegalStateException("The detached edge could not be be found incident to the provided vertex: " + this));
    }

    @Override
    public Edge attach(final Graph hostGraph) {
        return hostGraph.e(this.id);
    }

    public static Edge addTo(final Graph graph, final DetachedEdge detachedEdge) {
        Vertex outV;
        try {
            outV = graph.v(detachedEdge.outVertex.id());
        } catch (final NoSuchElementException e) {
            outV = null;
        }
        if (null == outV) {
            outV = graph.addVertex(T.id, detachedEdge.outVertex.id());
        }

        Vertex inV;
        try {
            inV = graph.v(detachedEdge.inVertex.id());
        } catch (final NoSuchElementException e) {
            inV = null;
        }
        if (null == inV) {
            inV = graph.addVertex(T.id, detachedEdge.inVertex.id());
        }

        if (ElementHelper.areEqual(outV, inV)) {
            final Iterator<Edge> itty = outV.iterators().edgeIterator(Direction.OUT, detachedEdge.label());
            while (itty.hasNext()) {
                final Edge e = itty.next();
                if (ElementHelper.areEqual(detachedEdge, e))
                    return e;
            }
        }

        final Edge e = outV.addEdge(detachedEdge.label(), inV, T.id, detachedEdge.id());
        detachedEdge.properties.entrySet().forEach(kv ->
                        kv.getValue().forEach(p -> e.<Object>property(kv.getKey(), p.value()))
        );
        return e;
    }

    @Override
    public Edge.Iterators iterators() {
        return this;
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

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction) {
        switch (direction) {
            case OUT:
                return new SingleIterator<>(this.outVertex);
            case IN:
                return new SingleIterator<>(this.inVertex);
            default:
                return new DoubleIterator<>(this.outVertex, this.inVertex);
        }
    }

    @Override
    public <V> Iterator<Property<V>> propertyIterator(final String... propertyKeys) {
        return (Iterator) super.propertyIterator(propertyKeys);
    }
}
