package com.tinkerpop.gremlin.structure.util.referenced;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.detached.Attachable;

import java.io.Serializable;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferencedProperty<V> implements Property<V>, Attachable<Property>, Serializable {

    protected String key;
    protected V value;
    protected boolean hidden;
    protected ReferencedElement element;

    public ReferencedProperty() {

    }

    public ReferencedProperty(final Property<V> property) {
        this.key = property.key();
        this.value = property.value();
        this.hidden = property.isHidden();
        this.element = ReferencedFactory.detach((Element)property.element());
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
        return null != this.value;
    }

    @Override
    public boolean isHidden() {
        return this.hidden;
    }

    @Override
    public void remove() {
        throw new IllegalStateException("Referenced properties can not be removed:" + this);
    }

    @Override
    public <E extends Element> E element() {
        return (E) this.element;
    }

    @Override
    public int hashCode() {
        return this.key.hashCode() + this.value.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public Property attach(final Graph hostGraph) {
        if (this.element instanceof VertexProperty) {
            return ((ReferencedVertexProperty) this.element).attach(hostGraph).property(this.key());
        } else if (this.element instanceof Edge) {
            return ((ReferencedEdge) this.element).attach(hostGraph).property(this.key());
        } else {
            throw new IllegalStateException("The property is not attached to a legal element: " + this);
        }
    }

    @Override
    public Property attach(final Vertex hostVertex) {
        if (this.element instanceof VertexProperty) {
            return ((ReferencedVertexProperty) this.element).attach(hostVertex).property(this.key());
        } else if (this.element instanceof Edge) {
            return ((ReferencedEdge) this.element).attach(hostVertex).property(this.key());
        } else {
            throw new IllegalStateException("The property is not attached to a legal element: " + this);
        }
    }
}
