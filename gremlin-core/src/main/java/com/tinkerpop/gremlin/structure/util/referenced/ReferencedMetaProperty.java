package com.tinkerpop.gremlin.structure.util.referenced;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.MetaProperty;
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
public class ReferencedMetaProperty<V> extends ReferencedElement implements MetaProperty<V>, Attachable<MetaProperty> {

    protected String key;
    protected V value;
    protected boolean hidden;
    protected ReferencedVertex vertex;

    public ReferencedMetaProperty() {

    }

    public ReferencedMetaProperty(final MetaProperty<V> metaProperty) {
        super(metaProperty);
        this.key = metaProperty.key();
        this.value = metaProperty.value();
        this.hidden = metaProperty.isHidden();
        this.vertex = ReferencedFactory.detach(metaProperty.getElement());
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
    public MetaProperty.Iterators iterators() {
        return Iterators.ITERATORS;
    }

    @Override
    public MetaProperty attach(final Graph hostGraph) {
        return this.attach(this.vertex);
    }

    @Override
    public MetaProperty attach(final Vertex hostVertex) {
        return StreamFactory.stream(hostVertex.iterators().properties(this.key))
                .filter(metaProperty -> metaProperty.id().toString().equals(this.id()))
                .findFirst().orElseThrow(() -> new IllegalStateException("The referenced meta-property does not reference a meta-property on the host vertex"));
    }

    private static final class Iterators implements MetaProperty.Iterators {

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
