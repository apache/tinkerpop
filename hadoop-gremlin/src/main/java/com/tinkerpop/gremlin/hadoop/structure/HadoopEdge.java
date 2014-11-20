package com.tinkerpop.gremlin.hadoop.structure;

import com.tinkerpop.gremlin.hadoop.process.graph.HadoopElementTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.util.DoubleIterator;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerProperty;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopEdge extends HadoopElement implements Edge, Edge.Iterators, WrappedEdge<TinkerEdge> {

    protected HadoopEdge() {
    }

    public HadoopEdge(final TinkerEdge edge, final HadoopGraph graph) {
        super(edge, graph);
    }

    @Override
    public GraphTraversal<Edge, Edge> start() {
        return new HadoopElementTraversal<>(this, this.graph);
    }

    @Override
    public TinkerEdge getBaseEdge() {
        return (TinkerEdge) this.tinkerElement;
    }

    @Override
    public Edge.Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction) {
        switch (direction) {
            case OUT:
                return new SingleIterator<>(this.graph.v(getBaseEdge().iterators().vertexIterator(Direction.OUT).next().id()));
            case IN:
                return new SingleIterator<>(this.graph.v(getBaseEdge().iterators().vertexIterator(Direction.IN).next().id()));
            default:
                return new DoubleIterator<>(this.graph.v(getBaseEdge().iterators().vertexIterator(Direction.OUT).next().id()), this.graph.v(getBaseEdge().iterators().vertexIterator(Direction.IN).next().id()));
        }
    }

    @Override
    public <V> Iterator<Property<V>> propertyIterator(final String... propertyKeys) {
        return (Iterator) StreamFactory.stream(getBaseEdge().iterators().propertyIterator(propertyKeys))
                .map(property -> new HadoopProperty<>((TinkerProperty<V>) property, this)).iterator();
    }
}