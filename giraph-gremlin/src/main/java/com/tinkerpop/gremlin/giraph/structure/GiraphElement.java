package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GiraphElement implements Element, Serializable {

    protected Element element;
    protected GiraphGraph graph;

    protected GiraphElement() {
    }

    protected GiraphElement(final Element element, final GiraphGraph graph) {
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
    public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys) {
        return this.element.properties(propertyKeys);
    }

    @Override
    public <V> Iterator<? extends Property<V>> hiddens(final String... propertyKeys) {
        return this.element.hiddens(propertyKeys);
    }

    @Override
    public <V> Property<V> property(final String key) {
        return this.element.property(key);
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public int hashCode() {
        return this.element.hashCode();
    }

    public String toString() {
        return this.element.toString();
    }
}
