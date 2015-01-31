package com.tinkerpop.gremlin.hadoop.structure;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopVertex extends HadoopElement implements Vertex, Vertex.Iterators, WrappedVertex<Vertex> {

    protected HadoopVertex() {
    }

    public HadoopVertex(final Vertex vertex, final HadoopGraph graph) {
        super(vertex, graph);
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        final VertexProperty<V> vertexProperty = getBaseVertex().<V>property(key);
        return vertexProperty.isPresent() ?
                new HadoopVertexProperty<>((VertexProperty<V>) ((Vertex) this.baseElement).property(key), this) :
                VertexProperty.<V>empty();
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw Vertex.Exceptions.edgeAdditionsNotSupported();
    }

    @Override
    public Vertex getBaseVertex() {
        return (Vertex) this.baseElement;
    }

    @Override
    public Vertex.Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction, final String... edgeLabels) {
        return IteratorUtils.map(this.getBaseVertex().iterators().vertexIterator(direction, edgeLabels), vertex -> HadoopVertex.this.graph.iterators().vertexIterator(vertex.id()).next());
    }

    @Override
    public Iterator<Edge> edgeIterator(final Direction direction, final String... edgeLabels) {
        return IteratorUtils.map(this.getBaseVertex().iterators().edgeIterator(direction, edgeLabels), edge -> HadoopVertex.this.graph.iterators().edgeIterator(edge.id()).next());
    }

    @Override
    public <V> Iterator<VertexProperty<V>> propertyIterator(final String... propertyKeys) {
        return IteratorUtils.<VertexProperty<V>, VertexProperty<V>>map(this.getBaseVertex().iterators().propertyIterator(propertyKeys), property -> new HadoopVertexProperty<>(property, HadoopVertex.this));
    }
}
