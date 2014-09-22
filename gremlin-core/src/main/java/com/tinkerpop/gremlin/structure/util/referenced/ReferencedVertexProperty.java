package com.tinkerpop.gremlin.structure.util.referenced;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.detached.Attachable;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferencedVertexProperty<V> extends ReferencedElement implements VertexProperty<V>, Attachable<VertexProperty> {

    protected String key;
    protected V value;
    protected boolean hidden;
    protected ReferencedVertex vertex;

    public ReferencedVertexProperty() {

    }

    public ReferencedVertexProperty(final VertexProperty<V> vertexProperty) {
        super(vertexProperty);
        this.key = vertexProperty.key();
        this.value = vertexProperty.value();
        this.hidden = vertexProperty.isHidden();
        this.vertex = ReferencedFactory.detach(vertexProperty.getElement());
    }

    @Override
    public Vertex getElement() {
        return this.vertex;
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return this.value != null;
    }

    @Override
    public boolean isHidden() {
        return this.hidden;
    }

    @Override
    public VertexProperty.Iterators iterators() {
        return Iterators.ITERATORS;
    }

    @Override
    public VertexProperty attach(final Graph hostGraph) {
        return this.attach(this.vertex);
    }

    @Override
    public VertexProperty attach(final Vertex hostVertex) {
        return StreamFactory.stream(hostVertex.iterators().properties(this.key))
                .filter(vertexProperty -> vertexProperty.equals(this))
                .findFirst().orElseThrow(() -> new IllegalStateException("The referenced meta-property does not reference a meta-property on the host vertex"));
    }

    private static final class Iterators implements VertexProperty.Iterators {

        protected static final Iterators ITERATORS = new Iterators();

        @Override
        public <V> Iterator<Property<V>> properties(String... propertyKeys) {
            return Collections.emptyIterator();
        }

        @Override
        public <V> Iterator<Property<V>> hiddens(String... propertyKeys) {
            return Collections.emptyIterator();
        }
    }
}
