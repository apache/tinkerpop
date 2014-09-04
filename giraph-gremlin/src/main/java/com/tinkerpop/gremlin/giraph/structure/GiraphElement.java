package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerElement;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GiraphElement implements Element, Serializable {

    protected TinkerElement element;
    protected GiraphGraph graph;

    protected GiraphElement() {
    }

    protected GiraphElement(final TinkerElement element, final GiraphGraph graph) {
        this.element = element;
        this.graph = graph;
    }

    @Override
    public Object id() {
        return this.element.id();
    }

    @Override
    public String label() {
        return this.element.label();
    }

    @Override
    public void remove() {
        if (this.element instanceof Vertex)
            throw Vertex.Exceptions.vertexRemovalNotSupported();
        else
            throw Edge.Exceptions.edgeRemovalNotSupported();
    }


    @Override
    public <V> Property<V> property(final String key) {
        return this.element.property(key);
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return this.element.hashCode();
    }

    @Override
    public String toString() {
        return this.element.toString();
    }

    protected class Iterators implements Element.Iterators {

        protected final Element element;

        public Iterators(final Element element) {
            this.element = element;
        }

        @Override
        public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys) {
            return this.element.iterators().properties(propertyKeys);
        }

        @Override
        public <V> Iterator<? extends Property<V>> hiddens(final String... propertyKeys) {
            return this.element.iterators().hiddens(propertyKeys);
        }
    }
}
